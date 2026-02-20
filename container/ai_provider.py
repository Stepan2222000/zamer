"""
Абстракция AI провайдеров для валидации объявлений.

Базовый класс AIValidationProvider определяет интерфейс для всех провайдеров.
FireworksProvider — провайдер для валидации через Fireworks AI API.
CodexProvider — провайдер через OpenAI Codex CLI (GPT-5.2 по подписке ChatGPT).
KimiProvider — провайдер через Kimi K2.5 по подписке (AIClient2API).
FallbackProvider — обёртка с автопереключением на резервный провайдер.
"""

import logging
import base64
import json
import re
import asyncio
import os
import tempfile
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp
import cv2
import numpy as np

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

    def get_images_base64(self, max_images: int = 2, max_size: int = 0) -> List[str]:
        """
        Возвращает изображения в формате base64, с опциональным ресайзом.

        Args:
            max_images: Максимальное количество изображений для возврата.
            max_size: Максимальный размер по длинной стороне (px). 0 = без ресайза.

        Returns:
            Список строк base64-encoded изображений.
        """
        result = []
        for img_bytes in self.images_bytes[:max_images]:
            if not img_bytes:
                continue

            if max_size > 0:
                # Ресайз через cv2
                raw = img_bytes
                if isinstance(raw, memoryview):
                    raw = bytes(raw)
                nparr = np.frombuffer(raw, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if img is not None:
                    h, w = img.shape[:2]
                    if max(h, w) > max_size:
                        scale = max_size / max(h, w)
                        new_w, new_h = int(w * scale), int(h * scale)
                        img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
                    _, encoded = cv2.imencode('.jpg', img, [cv2.IMWRITE_JPEG_QUALITY, 85])
                    result.append(base64.b64encode(encoded.tobytes()).decode('utf-8'))
                    continue

            # Fallback: оригинальные байты
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
        pass

    @abstractmethod
    async def close(self):
        pass

    def __str__(self) -> str:
        return self.__class__.__name__


class AIProviderError(Exception):
    """Ошибка AI провайдера — артикул нужно вернуть в очередь."""
    pass


# ──────────────────────────────────────────────────────────────
# Общие функции валидации (используются всеми провайдерами)
# ──────────────────────────────────────────────────────────────

def build_validation_prompt(
    articulum: str,
    listings: List[ListingForValidation],
    use_images: bool,
) -> str:
    """Построить промпт для AI-валидации (общий для всех провайдеров)."""
    items = [l.to_dict() for l in listings]
    real_ids = [i['id'] for i in items[:4]]

    image_criteria = ""
    if use_images:
        image_criteria = """
ИЗОБРАЖЕНИЯ: К каждому объявлению прикреплено фото. Фото идут в порядке объявлений: Фото 1 → первое объявление, Фото 2 → второе и т.д.

САМЫЙ ВАЖНЫЙ КРИТЕРИЙ — ФОТО:
На фото ОБЯЗАТЕЛЬНО должна быть видна сама запчасть или её упаковка.
Если на фото что угодно кроме запчасти (автомобиль, рекламный баннер, логотип магазина, заглушка, каталожная картинка) — ОТКЛОНЯЙ.

Дополнительные критерии отклонения по фото:
   - На фото видны признаки использования (царапины, потёртости, грязь)
   - Состояние товара НЕ НОВОЕ (следы эксплуатации)
   - На фото видна неоригинальная упаковка

Критерии принятия по фото:
✓ На фото видна реальная запчасть или коробка от запчасти
✓ Если видна оригинальная упаковка/маркировка производителя — это сильный плюс, даже при низкой цене
✓ Состояние товара — НОВОЕ

4. ДУБЛИРОВАНИЕ ОБЪЯВЛЕНИЙ ПО ФОТО:
   - Сравни фото всех объявлений между собой
   - Если у нескольких объявлений визуально ОДИНАКОВОЕ или ПОЧТИ ОДИНАКОВОЕ фото (одна и та же фотография, тот же ракурс, тот же фон, та же запчасть) — это дубли
   - Из группы дублей оставь только ОДНО объявление с лучшей (наименьшей) ценой
   - Остальные отклони с причиной "Дубль фото с объявлением [ID оставленного]"
   - ВАЖНО: похожие запчасти на РАЗНЫХ фото — НЕ дубли. Дубль — когда использовано одно и то же физическое фото
"""

    return f"""Ты эксперт по валидации автозапчастей с Авито. Твоя задача - отсеивать неоригинальные запчасти и подделки.

АРТИКУЛ ДЛЯ ПРОВЕРКИ: "{articulum}"
Этот артикул соответствует определённому типу запчасти для определённого автомобиля.
Ты должен определить по названию, описанию и фото — продаётся ли в объявлении запчасть того же типа для того же автомобиля.
НЕ проверяй наличие точного номера артикула в тексте — у запчасти может быть много совместимых артикулов, и в описании может быть указан другой номер.

ОБЪЯВЛЕНИЯ:
{json.dumps(items, ensure_ascii=False)}

СТРОГИЕ КРИТЕРИИ ОТКЛОНЕНИЯ (REJECT):

1. НЕОРИГИНАЛЬНЫЕ ЗАПЧАСТИ:
   - Явное указание на аналог, копию, реплику, имитацию
   - Фразы: "неоригинальный", "аналог оригинала", "китайская копия", "aftermarket", "заменитель"
   - Указание на сторонние бренды-производители (не OEM)
   - Фразы: "качество как оригинал", "не уступает оригиналу" (это признак подделки)

2. ПОДДЕЛКИ И ПАЛЬ:
   - Признаки подделки в описании
   - Отсутствие оригинальной упаковки/маркировки (если об этом упоминается)
   - Подозрительно низкая цена — НО ТОЛЬКО если на фото НЕТ подтверждения оригинальности. Если на фото видна оригинальная упаковка, маркировка производителя или QR-код — цена НЕ является основанием для отклонения.

3. НЕСООТВЕТСТВИЕ ТИПУ ЗАПЧАСТИ:
   - Запчасть явно ДРУГОГО типа (например, ищем тормозной диск, а продаётся колодка)
   - Запчасть для ДРУГОГО автомобиля или модели
   - Запчасть не имеет отношения к артикулу "{articulum}" по типу и назначению

4. КОМПЛЕКТЫ И НАБОРЫ:
   - Продаётся набор/комплект/кит из разных деталей, а не сама запчасть отдельно — ОТКЛОНЯЙ
   - Несколько одинаковых деталей (например "4 поршня") — это ОК
{image_criteria}
КРИТЕРИИ ПРИНЯТИЯ (PASS):

✓ Запчасть того же типа и для того же автомобиля, что ожидается для артикула "{articulum}"
✓ Явное указание на оригинальность (OEM, оригинальный артикул)
✓ Бренд известного оригинального производителя
✓ Цена соответствует оригинальной запчасти
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


def extract_json_from_text(raw: str) -> str:
    """Извлечь JSON из ответа AI, убирая <think> теги и прочий мусор."""
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


def parse_ai_response(raw: str, listings: List[ListingForValidation]) -> ValidationResult:
    """Распарсить текстовый ответ AI в структурированный ValidationResult."""
    all_ids = {l.avito_item_id for l in listings}

    extracted = extract_json_from_text(raw)

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


def build_openai_messages(
    prompt: str,
    listings: List[ListingForValidation],
    use_images: bool,
    max_images_per_listing: int = 2,
    image_max_size: int = 0,
) -> List[Dict]:
    """Построить массив messages для OpenAI-совместимого API (общий для всех HTTP-провайдеров)."""
    system_msg = {
        "role": "system",
        "content": (
            "Ты валидатор автозапчастей. Отвечай ТОЛЬКО одним JSON объектом "
            "с полями passed_ids (массив строк) и rejected (массив объектов с id и reason). "
            "НЕ копируй входные данные объявлений в ответ. Верни только своё решение."
        ),
    }

    if not use_images:
        return [system_msg, {"role": "user", "content": prompt}]

    # Мультимодальный режим
    content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
    for listing in listings:
        for img_b64 in listing.get_images_base64(max_images_per_listing, image_max_size):
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
            })

    return [system_msg, {"role": "user", "content": content}]


# ──────────────────────────────────────────────────────────────
# FireworksProvider
# ──────────────────────────────────────────────────────────────

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
        image_max_size: int = 0,
    ):
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.max_images_per_listing = max_images_per_listing
        self.image_max_size = image_max_size
        self.session: Optional[aiohttp.ClientSession] = None
        resize_info = f", resize={image_max_size}px" if image_max_size > 0 else ""
        logger.info(f"FireworksProvider: model={model}, timeout={timeout}s{resize_info}")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            # ThreadedResolver использует нативный DNS macOS (socket.getaddrinfo),
            # а не c-ares (aiodns), который игнорирует scoped-резолверы macOS
            resolver = aiohttp.resolver.ThreadedResolver()
            connector = aiohttp.TCPConnector(resolver=resolver)
            self.session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self.session

    def _build_messages(self, prompt: str, listings: List[ListingForValidation], use_images: bool) -> List[Dict]:
        return build_openai_messages(prompt, listings, use_images, self.max_images_per_listing, self.image_max_size)

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

        prompt = build_validation_prompt(articulum, listings, use_images)
        messages = self._build_messages(prompt, listings, use_images)
        raw = await self._request_with_retry(messages)

        logger.debug(f"Fireworks response: {raw[:500]}")
        result = parse_ai_response(raw, listings)
        logger.info(f"Fireworks: passed={result.passed_count}, rejected={result.rejected_count}")

        return result

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("FireworksProvider: session closed")


# ──────────────────────────────────────────────────────────────
# CodexProvider (GPT-5.2 через Codex CLI)
# ──────────────────────────────────────────────────────────────

class CodexProvider(AIValidationProvider):
    """
    Провайдер AI валидации через OpenAI Codex CLI.

    Использует GPT-5.2 по подписке ChatGPT (Pro/Plus) через команду `codex exec`.
    Авторизация — через auth.json (OAuth токены ChatGPT).

    Codex CLI вызывается в неинтерактивном режиме:
      codex exec --json --ephemeral --full-auto --skip-git-repo-check "промпт"

    Ответ приходит как JSONL-поток событий. Модуль извлекает текст
    из события item.completed (type=agent_message) и парсит JSON.
    """

    def __init__(
        self,
        codex_home: str = "",
        model: str = "",
        reasoning_effort: str = "medium",
        timeout: int = 180,
        max_retries: int = 2,
        max_images_per_listing: int = 1,
        image_max_size: int = 512,
        max_concurrent: int = 1,
    ):
        self.codex_home = codex_home or os.path.expanduser("~/.codex")
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout = timeout
        self.max_retries = max_retries
        self.max_images_per_listing = max_images_per_listing
        self.image_max_size = image_max_size
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # Проверяем наличие codex в PATH
        codex_bin = shutil.which("codex")
        if codex_bin:
            logger.info(f"CodexProvider: codex найден в {codex_bin}")
        else:
            logger.warning("CodexProvider: команда 'codex' не найдена в PATH!")

        # Проверяем auth.json
        auth_path = os.path.join(self.codex_home, "auth.json")
        if os.path.isfile(auth_path):
            try:
                with open(auth_path) as f:
                    auth = json.load(f)
                tokens = auth.get("tokens", {})
                has_token = bool(tokens.get("access_token"))
                logger.info(
                    f"CodexProvider: codex_home={self.codex_home}, "
                    f"model={model or 'default'}, reasoning={reasoning_effort}, "
                    f"auth={'OK' if has_token else 'EMPTY TOKEN'}"
                )
            except Exception as e:
                logger.warning(f"CodexProvider: ошибка чтения auth.json: {e}")
        else:
            logger.warning(
                f"CodexProvider: auth.json не найден в {self.codex_home}. "
                f"Codex CLI не сможет авторизоваться."
            )

    async def _run_codex_exec(self, prompt: str, image_paths: Optional[List[str]] = None) -> str:
        """Запустить codex exec и вернуть текст ответа агента."""
        cmd = [
            "codex", "exec",
            "--json",
            "--ephemeral",
            "--full-auto",
            "--skip-git-repo-check",
        ]

        if self.model:
            cmd.extend(["-m", self.model])

        if self.reasoning_effort:
            cmd.extend(["-c", f"model_reasoning_effort={self.reasoning_effort}"])

        if image_paths:
            cmd.extend(["-i", ",".join(image_paths)])

        # Промпт передаём через stdin (не как аргумент CLI),
        # чтобы не упираться в лимит длины аргументов ОС (~2 МБ).
        # Codex CLI автоматически читает промпт из stdin, если не передан аргументом.

        env = os.environ.copy()
        env["CODEX_HOME"] = self.codex_home

        async with self._semaphore:
            try:
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )
            except FileNotFoundError:
                raise AIProviderError(
                    "Codex CLI не установлен (команда 'codex' не найдена в PATH). "
                    "Установите: npm install -g @openai/codex"
                )

            try:
                stdout_data, stderr_data = await asyncio.wait_for(
                    process.communicate(input=prompt.encode("utf-8")),
                    timeout=self.timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.communicate()
                raise AIProviderError(f"Codex CLI: таймаут ({self.timeout}с)")

        exit_code = process.returncode
        stdout_text = stdout_data.decode("utf-8", errors="replace")
        stderr_text = stderr_data.decode("utf-8", errors="replace")

        if exit_code != 0:
            # Логируем stderr для диагностики
            logger.error(f"Codex CLI exit={exit_code}, stderr: {stderr_text[:500]}")
            raise AIProviderError(
                f"Codex CLI код выхода {exit_code}: {stderr_text[:300]}"
            )

        # Парсим JSONL — ищем последний agent_message
        return self._parse_jsonl_response(stdout_text)

    def _parse_jsonl_response(self, raw: str) -> str:
        """Извлечь текст agent_message из JSONL-потока событий Codex CLI.

        Формат JSONL (по одному JSON-объекту на строку):
          {"type":"thread.started","thread_id":"..."}
          {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
          {"type":"turn.completed","usage":{"input_tokens":...,"output_tokens":...}}

        Извлекаем текст из последнего item.completed с type=agent_message.
        """
        last_message = ""
        usage_info = None

        for line in raw.split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                event_type = event.get("type", "")

                if event_type == "item.completed":
                    item = event.get("item", {})
                    if item.get("type") == "agent_message":
                        text = item.get("text", "")
                        if text:
                            last_message = text

                elif event_type == "turn.completed":
                    usage_info = event.get("usage")

                elif event_type == "turn.failed":
                    error = event.get("error", "unknown error")
                    raise AIProviderError(f"Codex CLI turn.failed: {error}")

                elif event_type == "error":
                    error_msg = event.get("message", str(event))
                    raise AIProviderError(f"Codex CLI error event: {error_msg}")

            except json.JSONDecodeError:
                continue

        if usage_info:
            logger.info(
                f"Codex usage: input={usage_info.get('input_tokens', '?')}, "
                f"output={usage_info.get('output_tokens', '?')}, "
                f"cached={usage_info.get('cached_input_tokens', '?')}"
            )

        if not last_message:
            # Fallback: если JSONL не дал результат, пробуем raw stdout как текст
            stripped = raw.strip()
            if stripped:
                logger.warning("Codex CLI: agent_message не найден в JSONL, используем raw stdout")
                return stripped
            raise AIProviderError("Codex CLI: пустой ответ (нет agent_message в JSONL)")

        return last_message

    async def _run_with_retry(self, prompt: str, image_paths: Optional[List[str]] = None) -> str:
        """Запуск codex exec с retry при ошибках."""
        last_error = None
        for attempt in range(self.max_retries):
            try:
                return await self._run_codex_exec(prompt, image_paths)
            except AIProviderError as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = 2.0 * (2 ** attempt)
                    logger.warning(
                        f"Codex CLI ошибка (попытка {attempt + 1}/{self.max_retries}), "
                        f"retry через {delay}с: {e}"
                    )
                    await asyncio.sleep(delay)

        raise AIProviderError(
            f"Codex CLI: {self.max_retries} попыток неудачны. Последняя ошибка: {last_error}"
        )

    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True,
    ) -> ValidationResult:
        if not listings:
            return ValidationResult([], [])

        total_img = sum(len(l.images_bytes) for l in listings) if use_images else 0
        logger.info(
            f"Codex: {len(listings)} listings, articulum='{articulum}', images={total_img}"
        )

        prompt = build_validation_prompt(articulum, listings, use_images)

        # Сохраняем изображения во временные файлы для флага -i
        image_paths: List[str] = []
        tmpdir = None

        try:
            if use_images and total_img > 0:
                tmpdir = tempfile.mkdtemp(prefix="codex_img_")
                img_idx = 0
                for listing in listings:
                    for img_b64 in listing.get_images_base64(
                        self.max_images_per_listing, self.image_max_size
                    ):
                        img_path = os.path.join(tmpdir, f"img_{img_idx}.jpg")
                        with open(img_path, "wb") as f:
                            f.write(base64.b64decode(img_b64))
                        image_paths.append(img_path)
                        img_idx += 1

            raw = await self._run_with_retry(prompt, image_paths or None)

            logger.debug(f"Codex response: {raw[:500]}")
            result = parse_ai_response(raw, listings)
            logger.info(
                f"Codex: passed={result.passed_count}, rejected={result.rejected_count}"
            )
            return result

        finally:
            if tmpdir:
                shutil.rmtree(tmpdir, ignore_errors=True)

    async def close(self):
        logger.info("CodexProvider: закрыт")


# ──────────────────────────────────────────────────────────────
# KimiProvider (Kimi K2.5 через AIClient2API по подписке)
# ──────────────────────────────────────────────────────────────

class KimiProvider(AIValidationProvider):
    """
    Провайдер AI валидации через Kimi K2.5 по подписке.

    Использует AIClient2API — OpenAI-совместимый прокси на сервере.
    Стоимость: 0 (по подписке, без оплаты за токены).
    """

    def __init__(
        self,
        api_url: str,
        api_key: str,
        model: str = "kimi-for-coding",
        timeout: int = 120,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
        max_images_per_listing: int = 2,
        image_max_size: int = 0,
        max_tokens: int = 4096,
        max_concurrent: int = 1,
    ):
        self.api_url = api_url
        self.api_key = api_key
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay
        self.max_images_per_listing = max_images_per_listing
        self.image_max_size = image_max_size
        self.max_tokens = max_tokens
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[aiohttp.ClientSession] = None
        resize_info = f", resize={image_max_size}px" if image_max_size > 0 else ""
        logger.info(
            f"KimiProvider: model={model}, timeout={timeout}s, "
            f"max_tokens={max_tokens}, max_concurrent={max_concurrent}{resize_info}"
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            resolver = aiohttp.resolver.ThreadedResolver()
            connector = aiohttp.TCPConnector(resolver=resolver)
            self.session = aiohttp.ClientSession(
                connector=connector,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=self.timeout)
            )
        return self.session

    async def _request_with_retry(self, messages: List[Dict]) -> str:
        session = await self._get_session()
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": self.max_tokens,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
            "thinking": {"type": "disabled"},
        }

        last_error = None
        for attempt in range(self.max_retries):
            try:
                async with self._semaphore:
                    async with session.post(self.api_url, json=payload) as resp:
                        if resp.status == 200:
                            return (await resp.json())['choices'][0]['message']['content']

                        if resp.status in (429, 503, 504):
                            delay = self.retry_base_delay * (2 ** attempt)
                            logger.warning(f"Kimi API {resp.status}, retry {attempt+1}/{self.max_retries} in {delay}s")
                            await asyncio.sleep(delay)
                            continue

                        text = await resp.text()
                        raise AIProviderError(f"Kimi API {resp.status}: {text[:300]}")

            except aiohttp.ClientError as e:
                last_error = e
                delay = self.retry_base_delay * (2 ** attempt)
                logger.warning(f"Kimi network error: {e}, retry {attempt+1}/{self.max_retries}")
                await asyncio.sleep(delay)

        raise AIProviderError(f"Kimi: {self.max_retries} retries failed. Last: {last_error}")

    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True
    ) -> ValidationResult:
        if not listings:
            return ValidationResult([], [])

        total_img = sum(len(l.images_bytes) for l in listings) if use_images else 0
        logger.info(f"Kimi: {len(listings)} listings, articulum='{articulum}', images={total_img}")

        prompt = build_validation_prompt(articulum, listings, use_images)
        messages = build_openai_messages(prompt, listings, use_images, self.max_images_per_listing, self.image_max_size)
        raw = await self._request_with_retry(messages)

        logger.debug(f"Kimi response: {raw[:500]}")
        result = parse_ai_response(raw, listings)
        logger.info(f"Kimi: passed={result.passed_count}, rejected={result.rejected_count}")

        return result

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("KimiProvider: session closed")


# ──────────────────────────────────────────────────────────────
# FallbackProvider (основной + резервный)
# ──────────────────────────────────────────────────────────────

class FallbackProvider(AIValidationProvider):
    """
    Обёртка: пробует основной провайдер, при ошибке переключается на резервный.

    Пример: FallbackProvider(CodexProvider, FireworksProvider)
    — сначала Codex (бесплатно по подписке), при ошибке — Fireworks (платный API).
    """

    def __init__(self, primary: AIValidationProvider, fallback: AIValidationProvider):
        self.primary = primary
        self.fallback = fallback
        logger.info(f"FallbackProvider: {self.primary} → {self.fallback}")

    async def validate(
        self,
        articulum: str,
        listings: List[ListingForValidation],
        use_images: bool = True,
    ) -> ValidationResult:
        try:
            return await self.primary.validate(articulum, listings, use_images)
        except AIProviderError as e:
            logger.warning(
                f"Основной провайдер ({self.primary}) не сработал: {e}"
            )
            logger.info(f"Переключение на резервный провайдер ({self.fallback})")
            return await self.fallback.validate(articulum, listings, use_images)

    async def close(self):
        await self.primary.close()
        await self.fallback.close()

    def __str__(self) -> str:
        return f"Fallback({self.primary} → {self.fallback})"


# ──────────────────────────────────────────────────────────────
# Фабрика провайдеров
# ──────────────────────────────────────────────────────────────

def create_provider(provider_type: str = "fireworks") -> AIValidationProvider:
    """
    Фабричный метод для создания AI провайдера.

    Args:
        provider_type: Тип провайдера:
            - "fireworks" — Fireworks AI API (платный, быстрый)
            - "codex" — OpenAI Codex CLI / GPT-5.2 (по подписке ChatGPT)
            - "kimi" — Kimi K2.5 через AIClient2API (по подписке, бесплатно)
            - "codex+fireworks" — Codex как основной, Fireworks как fallback
            - "kimi+fireworks" — Kimi как основной, Fireworks как fallback

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
            AI_IMAGE_MAX_SIZE,
        )
        return FireworksProvider(
            api_key=FIREWORKS_API_KEY,
            model=FIREWORKS_MODEL,
            timeout=AI_REQUEST_TIMEOUT,
            max_retries=AI_MAX_RETRIES,
            retry_base_delay=AI_RETRY_BASE_DELAY,
            max_images_per_listing=AI_MAX_IMAGES_PER_LISTING,
            image_max_size=AI_IMAGE_MAX_SIZE,
        )

    if provider_type == "codex":
        from config import (
            CODEX_HOME,
            CODEX_MODEL,
            CODEX_REASONING_EFFORT,
            CODEX_TIMEOUT,
            CODEX_MAX_RETRIES,
            CODEX_MAX_CONCURRENT,
            AI_MAX_IMAGES_PER_LISTING,
            AI_IMAGE_MAX_SIZE,
        )
        return CodexProvider(
            codex_home=CODEX_HOME,
            model=CODEX_MODEL,
            reasoning_effort=CODEX_REASONING_EFFORT,
            timeout=CODEX_TIMEOUT,
            max_retries=CODEX_MAX_RETRIES,
            max_images_per_listing=AI_MAX_IMAGES_PER_LISTING,
            image_max_size=AI_IMAGE_MAX_SIZE,
            max_concurrent=CODEX_MAX_CONCURRENT,
        )

    if provider_type == "codex+fireworks":
        primary = create_provider("codex")
        fallback = create_provider("fireworks")
        return FallbackProvider(primary, fallback)

    if provider_type == "kimi":
        from config import (
            KIMI_API_URL,
            KIMI_API_KEY,
            KIMI_MODEL,
            KIMI_TIMEOUT,
            KIMI_MAX_RETRIES,
            KIMI_MAX_TOKENS,
            KIMI_MAX_CONCURRENT,
            AI_RETRY_BASE_DELAY,
            AI_MAX_IMAGES_PER_LISTING,
            AI_IMAGE_MAX_SIZE,
        )
        return KimiProvider(
            api_url=KIMI_API_URL,
            api_key=KIMI_API_KEY,
            model=KIMI_MODEL,
            timeout=KIMI_TIMEOUT,
            max_retries=KIMI_MAX_RETRIES,
            retry_base_delay=AI_RETRY_BASE_DELAY,
            max_images_per_listing=AI_MAX_IMAGES_PER_LISTING,
            image_max_size=AI_IMAGE_MAX_SIZE,
            max_tokens=KIMI_MAX_TOKENS,
            max_concurrent=KIMI_MAX_CONCURRENT,
        )

    if provider_type == "kimi+fireworks":
        primary = create_provider("kimi")
        fallback = create_provider("fireworks")
        return FallbackProvider(primary, fallback)

    raise ValueError(
        f"Неизвестный тип провайдера: '{provider_type}'. "
        f"Поддерживаются: fireworks, codex, kimi, codex+fireworks, kimi+fireworks"
    )


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
