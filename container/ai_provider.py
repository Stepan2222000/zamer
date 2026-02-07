"""
Абстракция AI провайдеров для валидации объявлений.

Базовый класс AIValidationProvider определяет интерфейс для всех провайдеров.
FireworksProvider — провайдер для валидации через Fireworks AI API.
"""

import logging
import base64
import json
import re
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

logger = logging.getLogger(__name__)


@dataclass
class ListingForValidation:
    """Объявление для AI валидации."""
    avito_item_id: str
    title: str
    price: Optional[float]
    snippet_text: Optional[str]
    seller_name: Optional[str]
    images_bytes: List[bytes] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Конвертация в словарь для JSON сериализации (без изображений)."""
        return {
            'id': self.avito_item_id,
            'title': self.title,
            'price': self.price,
            'snippet': self.snippet_text,
            'seller': self.seller_name,
        }

    def get_images_base64(self, max_images: int = 2) -> List[str]:
        """
        Возвращает изображения в формате base64.

        Args:
            max_images: Максимальное количество изображений для возврата.

        Returns:
            Список строк base64-encoded изображений.
        """
        result = []
        for img_bytes in self.images_bytes[:max_images]:
            if img_bytes:
                result.append(base64.b64encode(img_bytes).decode('utf-8'))
        return result


@dataclass
class RejectedListing:
    """Отклонённое объявление с причиной."""
    avito_item_id: str
    reason: str


@dataclass
class ValidationResult:
    """Результат валидации группы объявлений."""
    passed_ids: List[str]
    rejected: List[RejectedListing]

    @property
    def passed_count(self) -> int:
        return len(self.passed_ids)

    @property
    def rejected_count(self) -> int:
        return len(self.rejected)


class AIValidationProvider(ABC):
    """
    Базовый класс для AI провайдеров валидации.

    Все провайдеры должны реализовать:
    - validate() — основной метод валидации
    - close() — освобождение ресурсов
    """

    @abstractmethod
    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True
    ) -> ValidationResult:
        """
        Валидация списка объявлений для артикула.

        Args:
            articulum: Артикул для проверки.
            listings: Список объявлений для валидации.
            use_images: Использовать ли изображения в валидации.

        Returns:
            ValidationResult с passed_ids и rejected списками.

        Raises:
            AIProviderError: При ошибках API.
        """
        pass

    @abstractmethod
    async def close(self):
        """Освобождение ресурсов (HTTP клиенты, сессии и т.д.)"""
        pass

    def __str__(self) -> str:
        return self.__class__.__name__


class AIProviderError(Exception):
    """Ошибка AI провайдера — артикул нужно вернуть в очередь."""
    pass


class FireworksProvider(AIValidationProvider):
    """
    Провайдер AI валидации через Fireworks AI API.
    Поддерживает мультимодальную валидацию (текст + изображения).
    """

    API_URL = "https://api.fireworks.ai/inference/v1/chat/completions"

    def __init__(
        self,
        api_key: str,
        model: str,
        timeout: int = 120,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
        max_images_per_listing: int = 2,
    ):
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.max_images_per_listing = max_images_per_listing
        self.session: Optional[aiohttp.ClientSession] = None
        logger.info(f"FireworksProvider: model={model}, timeout={timeout}s")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self.session

    def _build_prompt(self, articulum: str, listings: List[ListingForValidation], use_images: bool) -> str:
        items = [l.to_dict() for l in listings]
        real_ids = [i['id'] for i in items[:4]]

        image_criteria = ""
        if use_images:
            image_criteria = """
4. КРИТЕРИИ ПО ИЗОБРАЖЕНИЯМ (если предоставлены фото):
   - Фото НЕ соответствует описанию товара (сток-фото, случайное изображение)
   - На фото видны признаки использования (царапины, потёртости, грязь)
   - Состояние товара НЕ НОВОЕ (следы эксплуатации)
   - На фото видна неоригинальная упаковка или отсутствие маркировки
   - Фото низкого качества, не позволяющее оценить товар

КРИТЕРИИ ПРИНЯТИЯ ПО ФОТО:
✓ Фото соответствует описанию товара
✓ Видна оригинальная упаковка или маркировка производителя
✓ Состояние товара — НОВОЕ (нет следов использования)
✓ На фото действительно автозапчасть, соответствующая артикулу
"""

        return f"""Ты эксперт по валидации автозапчастей с Авито. Твоя задача - отсеивать неоригинальные запчасти и подделки.

АРТИКУЛ ДЛЯ ПРОВЕРКИ: "{articulum}"
(Примечание: у запчасти может быть несколько артикулов, главное - чтобы "{articulum}" входил в их число)

ОБЪЯВЛЕНИЯ:
{json.dumps(items, ensure_ascii=False)}

СТРОГИЕ КРИТЕРИИ ОТКЛОНЕНИЯ (REJECT):

1. НЕОРИГИНАЛЬНЫЕ ЗАПЧАСТИ:
   - Явное указание на аналог, копию, реплику, имитацию
   - Фразы: "неоригинальный", "аналог оригинала", "китайская копия", "aftermarket", "заменитель"
   - Указание на сторонние бренды-производители (не OEM)
   - Фразы: "качество как оригинал", "не уступает оригиналу" (это признак подделки)

2. ПОДДЕЛКИ И ПАЛЬ:
   - Подозрительно низкая цена (значительно ниже рыночной для оригинала)
   - Признаки подделки в описании
   - Отсутствие оригинальной упаковки/маркировки (если об этом упоминается)

3. НЕСООТВЕТСТВИЕ АРТИКУЛУ:
   - Запчасть явно НЕ соответствует артикулу "{articulum}"
   - Артикул "{articulum}" отсутствует в списке подходящих артикулов
{image_criteria}
КРИТЕРИИ ПРИНЯТИЯ (PASS):

✓ Явное указание на оригинальность (OEM, оригинальный артикул)
✓ Бренд известного оригинального производителя
✓ Цена соответствует оригинальной запчасти
✓ Артикул "{articulum}" присутствует (может быть одним из нескольких)
✓ Отсутствие признаков подделки в описании

ВАЖНО: При малейших сомнениях в оригинальности - ОТКЛОНЯЙ объявление.

ФОРМАТ ОТВЕТА - СТРОГО JSON:
- Верни ОДИН JSON объект (не повторяй его!)
- КАЖДОЕ объявление из входных данных ОБЯЗАТЕЛЬНО должно быть либо в passed_ids, либо в rejected
- Используй РЕАЛЬНЫЕ ID объявлений (например: "{real_ids[0] if real_ids else ''}", "{real_ids[1] if len(real_ids) > 1 else ''}")
- НЕ используй шаблонные id1, id2 - только настоящие числовые ID!
- Для каждого отклонённого объявления ОБЯЗАТЕЛЬНО укажи причину в поле reason

{{
  "passed_ids": ["ID принятых объявлений"],
  "rejected": [
    {{"id": "ID отклонённого", "reason": "Краткая причина отклонения"}}
  ]
}}

ПРИМЕР для {len(items)} объявлений - все ID должны быть распределены:
{{
  "passed_ids": ["{real_ids[0] if real_ids else ''}"],
  "rejected": [
    {{"id": "{real_ids[1] if len(real_ids) > 1 else ''}", "reason": "Аналог, не оригинал"}},
    {{"id": "{real_ids[2] if len(real_ids) > 2 else ''}", "reason": "Подозрительно низкая цена"}}
  ]
}}"""

    def _build_messages(self, prompt: str, listings: List[ListingForValidation], use_images: bool) -> List[Dict]:
        if not use_images:
            return [
                {"role": "system", "content": "Отвечай только в JSON формате."},
                {"role": "user", "content": prompt}
            ]

        # Мультимодальный режим
        content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
        for listing in listings:
            for img_b64 in listing.get_images_base64(self.max_images_per_listing):
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
                })

        return [
            {"role": "system", "content": "Отвечай только в JSON формате."},
            {"role": "user", "content": content}
        ]

    def _extract_json(self, raw: str) -> str:
        """Извлечь JSON из ответа, убирая <think> теги и прочий мусор."""
        # Убираем <think>...</think> блоки (thinking-модели)
        cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()

        # Если после очистки это валидный JSON — возвращаем
        try:
            json.loads(cleaned)
            return cleaned
        except json.JSONDecodeError:
            pass

        # Ищем JSON-объект с passed_ids в тексте
        match = re.search(r'\{[^{}]*"passed_ids"[^{}]*\{.*?\}.*?\}', cleaned, re.DOTALL)
        if match:
            return match.group(0)

        # Последний fallback — ищем любой {...} блок
        match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if match:
            return match.group(0)

        return cleaned

    def _parse_response(self, raw: str, listings: List[ListingForValidation]) -> ValidationResult:
        all_ids = {l.avito_item_id for l in listings}

        extracted = self._extract_json(raw)

        try:
            data = json.loads(extracted)
            passed_ids = set(str(pid) for pid in data.get('passed_ids', []))
            rejected_dict = {str(r['id']): r.get('reason', 'Причина не указана') for r in data.get('rejected', [])}
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning(f"JSON parse error, trying regex. Response: {raw[:500]}")
            # Fallback regex
            match = re.search(r'"passed_ids"\s*:\s*\[(.*?)\]', raw, re.DOTALL)
            passed_ids = set(re.findall(r'"(\d+)"', match.group(1))) if match else set()
            rejected_dict = dict(re.findall(r'\{"id"\s*:\s*"(\d+)"\s*,\s*"reason"\s*:\s*"([^"]*)"', raw))

        # Проверяем, распознал ли AI хоть что-то
        recognized = passed_ids | set(rejected_dict.keys())
        if not recognized:
            logger.error(f"AI не вернул ни одного ID. Raw response: {raw[:500]}")
            raise AIProviderError("AI вернул невалидный ответ: ни одного объявления не распознано")

        rejected = [RejectedListing(id, reason) for id, reason in rejected_dict.items()]

        # Не учтённые — в rejected с предупреждением
        missing = all_ids - passed_ids - set(rejected_dict.keys())
        if missing:
            logger.warning(f"AI не упомянул {len(missing)} из {len(all_ids)} объявлений: {missing}")
            for id in missing:
                rejected.append(RejectedListing(id, "Не учтено в ответе AI"))

        return ValidationResult(list(passed_ids), rejected)

    async def _request_with_retry(self, messages: List[Dict]) -> str:
        session = await self._get_session()
        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},  # Structured output
        }

        last_error = None
        for attempt in range(self.max_retries):
            try:
                async with session.post(self.API_URL, json=payload) as resp:
                    if resp.status == 200:
                        return (await resp.json())['choices'][0]['message']['content']

                    if resp.status in (429, 503, 504):
                        delay = self.retry_base_delay * (2 ** attempt)
                        logger.warning(f"Error {resp.status}, retry {attempt+1}/{self.max_retries} in {delay}s")
                        await asyncio.sleep(delay)
                        continue

                    text = await resp.text()
                    raise AIProviderError(f"Fireworks API {resp.status}: {text[:300]}")

            except aiohttp.ClientError as e:
                last_error = e
                delay = self.retry_base_delay * (2 ** attempt)
                logger.warning(f"Network error: {e}, retry {attempt+1}/{self.max_retries}")
                await asyncio.sleep(delay)

        raise AIProviderError(f"Fireworks: {self.max_retries} retries failed. Last: {last_error}")

    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True
    ) -> ValidationResult:
        if not listings:
            return ValidationResult([], [])

        total_img = sum(len(l.images_bytes) for l in listings) if use_images else 0
        logger.info(f"Fireworks: {len(listings)} listings, articulum='{articulum}', images={total_img}")

        prompt = self._build_prompt(articulum, listings, use_images)
        messages = self._build_messages(prompt, listings, use_images)
        raw = await self._request_with_retry(messages)

        logger.debug(f"Fireworks response: {raw[:500]}")
        result = self._parse_response(raw, listings)
        logger.info(f"Fireworks: passed={result.passed_count}, rejected={result.rejected_count}")

        return result

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("FireworksProvider: session closed")


def create_provider(provider_type: str = "fireworks") -> AIValidationProvider:
    """
    Фабричный метод для создания AI провайдера.

    Args:
        provider_type: Тип провайдера (поддерживается только "fireworks").

    Returns:
        Экземпляр AIValidationProvider.

    Raises:
        ValueError: Если тип провайдера не поддерживается.
    """
    if provider_type == "fireworks":
        from config import (
            FIREWORKS_API_KEY,
            FIREWORKS_MODEL,
            AI_REQUEST_TIMEOUT,
            AI_MAX_RETRIES,
            AI_RETRY_BASE_DELAY,
            AI_MAX_IMAGES_PER_LISTING,
        )
        return FireworksProvider(
            api_key=FIREWORKS_API_KEY,
            model=FIREWORKS_MODEL,
            timeout=AI_REQUEST_TIMEOUT,
            max_retries=AI_MAX_RETRIES,
            retry_base_delay=AI_RETRY_BASE_DELAY,
            max_images_per_listing=AI_MAX_IMAGES_PER_LISTING,
        )

    raise ValueError(f"Неизвестный тип провайдера: '{provider_type}'. Поддерживается только: fireworks")


def convert_listing_dict_to_validation(
    listing: Dict,
    max_images: int = 5
) -> ListingForValidation:
    """
    Конвертирует словарь объявления в ListingForValidation.

    Args:
        listing: Словарь с данными объявления из БД.
        max_images: Максимальное количество изображений.

    Returns:
        ListingForValidation для передачи в провайдер.
    """
    # Получаем изображения
    images_bytes_raw = listing.get('images_bytes') or []
    images_bytes = images_bytes_raw[:max_images] if images_bytes_raw else []

    # Конвертируем price из Decimal в float
    price = listing.get('price')
    if price is not None:
        price = float(price)

    return ListingForValidation(
        avito_item_id=listing['avito_item_id'],
        title=listing.get('title', ''),
        price=price,
        snippet_text=listing.get('snippet_text'),
        seller_name=listing.get('seller_name'),
        images_bytes=images_bytes,
    )
