"""Validation Worker - валидация объявлений без браузера"""

import asyncio
import logging
import sys
import json
import statistics
import aiohttp
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from google.auth import default
import google.auth.transport.requests
from openai import AsyncOpenAI

from database import create_pool
from config import (
    MIN_PRICE,
    MIN_VALIDATED_ITEMS,
    MIN_SELLER_REVIEWS,
    ENABLE_PRICE_VALIDATION,
    ENABLE_AI_VALIDATION,
    REQUIRE_ARTICULUM_IN_TEXT,
    VALIDATION_STOPWORDS,
    VERTEX_AI_PROJECT_ID,
    VERTEX_AI_LOCATION,
    VERTEX_AI_MODEL,
    GOOGLE_APPLICATION_CREDENTIALS,
    SKIP_OBJECT_PARSING,
    ArticulumState,
    AI_PROVIDER,
    HF_API_TOKEN,
    HF_ENDPOINT_URL,
)
from state_machine import (
    transition_to_validating,
    transition_to_validated,
    reject_articulum,
    rollback_to_catalog_parsed,
)
from object_task_manager import create_object_tasks_for_articulum


class AIAPIError(Exception):
    """Ошибка AI API - артикул нужно вернуть в очередь"""
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Установить уровень WARNING для HTTP логов сторонних библиотек
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('google.auth').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


def normalize_text_for_articulum_search(text: str) -> str:
    """
    Нормализует текст для поиска артикула:
    - Приводит к нижнему регистру
    - Заменяет визуально схожие русские буквы на английские
    - Удаляет все спецсимволы (оставляет только буквы и цифры)
    """
    if not text:
        return ""

    # Нижний регистр
    text = text.lower()

    # Замена русских букв на английские
    replacements = {
        'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k',
        'м': 'm', 'н': 'h', 'о': 'o', 'р': 'p',
        'с': 'c', 'т': 't', 'у': 'y', 'х': 'x'
    }
    for ru, en in replacements.items():
        text = text.replace(ru, en)

    # Удаление спецсимволов (оставляем только буквы и цифры)
    text = ''.join(char for char in text if char.isalnum())

    return text


class VertexAIClient:
    """Wrapper для Vertex AI через OpenAI SDK с автообновлением OAuth токенов"""

    def __init__(self, project_id: str, location: str, model: str, logger=None):
        self.project_id = project_id
        self.location = location
        self.model = model
        self.credentials = None
        self.token_expiry = None
        self._client = None
        self.logger = logger or logging.getLogger(__name__)

        # Инициализация credentials при создании
        self.credentials, _ = default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

    def _refresh_token(self):
        """Обновить OAuth токен если истек"""
        # Токены живут 1 час, обновляем за 5 минут до истечения
        if self.token_expiry is None or datetime.now() >= self.token_expiry:
            self.credentials.refresh(google.auth.transport.requests.Request())
            self.token_expiry = datetime.now() + timedelta(minutes=55)

            # Пересоздать клиент с новым токеном
            base_url = f"https://aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/endpoints/openapi"
            self._client = AsyncOpenAI(
                base_url=base_url,
                api_key=self.credentials.token,
            )
            self.logger.info(f"Vertex AI клиент обновлен с endpoint: {base_url}")

    async def get_client(self) -> AsyncOpenAI:
        """Получить AsyncOpenAI клиент с валидным токеном"""
        self._refresh_token()
        return self._client


class HuggingFaceClient:
    """Клиент для HuggingFace Inference Endpoint с retry логикой"""

    # Коды ошибок для retry (server errors)
    RETRY_STATUS_CODES = {502, 503, 504, 429}
    MAX_RETRIES = 3
    RETRY_DELAY = 4.0  # секунды

    def __init__(self, endpoint_url: str, api_token: str, logger=None):
        self.endpoint_url = endpoint_url
        self.api_token = api_token
        self.logger = logger or logging.getLogger(__name__)
        self._session = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать aiohttp сессию"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.api_token}"},
                timeout=aiohttp.ClientTimeout(total=120)  # 2 минуты таймаут
            )
        return self._session

    async def generate(self, prompt: str) -> str:
        """Отправить запрос к HuggingFace Inference Endpoint с retry"""
        session = await self._get_session()

        # Формат для text-generation-inference (TGI)
        payload = {
            "inputs": prompt,
            "parameters": {
                "max_new_tokens": 2048,
                "temperature": 0.1,
                "return_full_text": False,
            }
        }

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                async with session.post(self.endpoint_url, json=payload) as response:
                    # Retry на server errors
                    if response.status in self.RETRY_STATUS_CODES:
                        error_text = await response.text()
                        last_error = f"HF Endpoint error {response.status}: {error_text}"
                        if attempt < self.MAX_RETRIES:
                            self.logger.warning(
                                f"HF API retry {attempt}/{self.MAX_RETRIES}: status {response.status}, "
                                f"ждем {self.RETRY_DELAY}с..."
                            )
                            await asyncio.sleep(self.RETRY_DELAY)
                            continue
                        else:
                            raise Exception(last_error)

                    # Другие ошибки — не retry
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"HF Endpoint error {response.status}: {error_text}")

                    result = await response.json()

                    # TGI возвращает список с generated_text
                    if isinstance(result, list) and len(result) > 0:
                        return result[0].get("generated_text", "")
                    elif isinstance(result, dict):
                        return result.get("generated_text", "")
                    else:
                        raise Exception(f"Unexpected response format: {result}")

            except aiohttp.ClientError as e:
                # Сетевые ошибки — retry
                last_error = f"HF network error: {e}"
                if attempt < self.MAX_RETRIES:
                    self.logger.warning(
                        f"HF API retry {attempt}/{self.MAX_RETRIES}: {type(e).__name__}, "
                        f"ждем {self.RETRY_DELAY}с..."
                    )
                    await asyncio.sleep(self.RETRY_DELAY)
                    continue
                else:
                    raise Exception(last_error)

        # Если дошли сюда — все попытки исчерпаны
        raise Exception(f"HF API все {self.MAX_RETRIES} попытки неудачны: {last_error}")

    async def close(self):
        """Закрыть сессию"""
        if self._session and not self._session.closed:
            await self._session.close()


class ValidationWorker:
    """Воркер для валидации объявлений (БЕЗ браузера)"""

    def __init__(self, worker_id: str):
        self.worker_id = worker_id

        # Создать кастомный formatter с worker_id для логов воркера
        logger = logging.getLogger(__name__)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            f'%(asctime)s [VALIDATION-{worker_id}] %(levelname)s: %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False  # Не передавать в root logger

        self.logger = logger
        self.pool = None
        self.vertex_client = None
        self.hf_client = None
        self.ai_provider = AI_PROVIDER
        self.ai_error_count = 0  # Счетчик последовательных ошибок API
        self.should_shutdown = False  # Флаг для graceful shutdown
        self.exit_code = 0  # Код выхода (2 = проблема с API)

        # Инициализация AI клиента в зависимости от провайдера
        if ENABLE_AI_VALIDATION:
            if self.ai_provider == 'huggingface':
                # HuggingFace Inference Endpoint
                if not HF_ENDPOINT_URL:
                    self.logger.error("HF_ENDPOINT_URL не задан — воркер не может работать")
                    self.logger.error("Убедитесь что endpoint запущен и URL передан через env")
                    raise RuntimeError("HF_ENDPOINT_URL обязателен при AI_PROVIDER=huggingface")
                if not HF_API_TOKEN:
                    self.logger.error("HF_API_TOKEN не задан — воркер не может работать")
                    raise RuntimeError("HF_API_TOKEN обязателен при AI_PROVIDER=huggingface")

                # Валидация URL
                if not HF_ENDPOINT_URL.startswith('https://'):
                    self.logger.error(f"HF_ENDPOINT_URL должен начинаться с https://")
                    raise RuntimeError(f"Некорректный HF_ENDPOINT_URL: {HF_ENDPOINT_URL}")

                self.hf_client = HuggingFaceClient(
                    endpoint_url=HF_ENDPOINT_URL,
                    api_token=HF_API_TOKEN,
                    logger=self.logger
                )
                self.logger.info(f"HuggingFace клиент инициализирован")
                self.logger.info(f"Endpoint: {HF_ENDPOINT_URL[:50]}...")
            else:
                # Vertex AI (Gemini) - по умолчанию
                try:
                    self.vertex_client = VertexAIClient(
                        project_id=VERTEX_AI_PROJECT_ID,
                        location=VERTEX_AI_LOCATION,
                        model=VERTEX_AI_MODEL,
                        logger=self.logger
                    )
                    self.logger.info(f"Vertex AI инициализирован: {VERTEX_AI_MODEL}")
                except Exception as e:
                    self.logger.error(f"Не удалось инициализировать Vertex AI: {e}")
                    raise RuntimeError(f"Vertex AI обязателен при ENABLE_AI_VALIDATION=true: {e}")
        else:
            self.logger.warning("ИИ-валидация отключена (ENABLE_AI_VALIDATION=false)")

    async def init(self):
        """Инициализация подключения к БД"""
        self.pool = await create_pool()
        self.logger.info("Validation Worker инициализирован")

    async def get_next_articulum(self) -> Optional[Dict]:
        """
        Атомарно захватывает следующий артикул для валидации.
        Сразу переводит его в статус VALIDATING.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Атомарный захват через UPDATE ... RETURNING
                articulum = await conn.fetchrow("""
                    UPDATE articulums
                    SET state = $1,
                        state_updated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = (
                        SELECT id
                        FROM articulums
                        WHERE state = $2
                        ORDER BY state_updated_at ASC
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, articulum, state
                """, ArticulumState.VALIDATING, ArticulumState.CATALOG_PARSED)

                if articulum:
                    return dict(articulum)
                return None

    async def get_listings_for_articulum(self, articulum_id: int) -> List[Dict]:
        """
        Получить все объявления для артикула из catalog_listings.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT
                    avito_item_id,
                    title,
                    price,
                    snippet_text,
                    seller_name,
                    seller_id,
                    seller_rating,
                    seller_reviews
                FROM catalog_listings
                WHERE articulum_id = $1
            """, articulum_id)

            return [dict(row) for row in rows]

    async def save_validation_result(
        self,
        articulum_id: int,
        avito_item_id: str,
        validation_type: str,
        passed: bool,
        rejection_reason: Optional[str] = None
    ):
        """Сохранить результат валидации в БД"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO validation_results (
                    articulum_id, avito_item_id, validation_type, passed, rejection_reason
                )
                VALUES ($1, $2, $3, $4, $5)
            """, articulum_id, avito_item_id, validation_type, passed, rejection_reason)

    async def price_filter_validation(
        self,
        articulum_id: int,
        listings: List[Dict]
    ) -> List[Dict]:
        """
        ПРОВЕРКА #1: Фильтрация по MIN_PRICE.
        Отсеивает объявления с ценой ниже минимального порога интереса.
        """
        passed_listings = []

        for listing in listings:
            price = listing.get('price')
            avito_item_id = listing['avito_item_id']

            # Фильтр MIN_PRICE - глобальный порог интереса
            if price is None or price < MIN_PRICE:
                await self.save_validation_result(
                    articulum_id,
                    avito_item_id,
                    'price_filter',
                    False,
                    f'Цена {price} < MIN_PRICE {MIN_PRICE}'
                )
            else:
                await self.save_validation_result(
                    articulum_id,
                    avito_item_id,
                    'price_filter',
                    True,
                    None
                )
                passed_listings.append(listing)

        self.logger.info(
            f"Price filter: {len(passed_listings)}/{len(listings)} прошли фильтр MIN_PRICE={MIN_PRICE}"
        )
        return passed_listings

    async def mechanical_validation(
        self,
        articulum_id: int,
        articulum: str,
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 2: Механическая валидация (проверка артикула + стоп-слова + ценовая проверка)"""
        passed_listings = []
        # Конвертируем Decimal в float для математических операций
        prices = [float(l['price']) for l in listings if l.get('price') is not None]

        # Вычисление статистики для ценовой валидации
        if len(prices) >= 4:  # Минимум 4 цены для квартилей
            prices_sorted = sorted(prices)

            # IQR метод для определения выбросов (коэффициент 1.0 для более строгой фильтрации)
            q1, q3 = statistics.quantiles(prices_sorted, n=4)[0], statistics.quantiles(prices_sorted, n=4)[2]
            iqr = q3 - q1
            lower_bound = q1 - 1.0 * iqr
            upper_bound = q3 + 1.0 * iqr

            # Фильтруем выбросы для расчета "чистой" медианы
            prices_clean = [p for p in prices_sorted if lower_bound <= p <= upper_bound]

            if len(prices_clean) > 0:
                median_clean = statistics.median(prices_clean)

                # Дополнительная защита от экстремальных выбросов (цена > 2.5× медианы)
                extreme_outlier_threshold = median_clean * 2.5
                prices_clean_final = [p for p in prices_clean if p <= extreme_outlier_threshold]

                # Если дополнительная фильтрация удалила все цены, используем prices_clean
                if len(prices_clean_final) == 0:
                    prices_clean_final = prices_clean
                    extreme_outliers_removed = 0
                else:
                    extreme_outliers_removed = len(prices_clean) - len(prices_clean_final)

                # Топ-40% для проверки подозрительно дешевых
                prices_sorted_desc = sorted(prices_clean_final, reverse=True)
                top40_count = max(1, len(prices_sorted_desc) * 2 // 5)
                top40_prices = prices_sorted_desc[:top40_count]
                median_top40 = statistics.median(top40_prices)

                outlier_upper_bound = upper_bound

                # Логирование статистики с двухэтапной фильтрацией
                iqr_outliers_removed = len(prices) - len(prices_clean)
                self.logger.info(
                    f"Фильтрация выбросов: IQR метод исключил {iqr_outliers_removed} шт, "
                    f"дополнительная защита (>{extreme_outlier_threshold:.2f}) исключила {extreme_outliers_removed} шт. "
                    f"Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}, "
                    f"границы=[{lower_bound:.2f}, {upper_bound:.2f}], median_clean={median_clean:.2f}"
                )
            else:
                # Если все цены - выбросы, используем простую логику
                median_clean = statistics.median(prices_sorted)
                median_top40 = median_clean
                outlier_upper_bound = median_clean * 3
        elif len(prices) >= 1:
            # Мало данных для IQR - используем простую логику
            prices_sorted = sorted(prices, reverse=True)
            median_clean = statistics.median(prices_sorted)
            median_top40 = median_clean
            outlier_upper_bound = median_clean * 3
        else:
            median_top40 = None
            median_clean = None
            outlier_upper_bound = None

        for listing in listings:
            avito_item_id = listing['avito_item_id']
            title = (listing.get('title') or '').lower()
            snippet = (listing.get('snippet_text') or '').lower()
            seller = (listing.get('seller_name') or '').lower()
            # Конвертируем Decimal в float для математических операций
            price = float(listing['price']) if listing.get('price') is not None else None

            rejection_reason = None

            # Проверка наличия артикула в тексте (если включено)
            if REQUIRE_ARTICULUM_IN_TEXT:
                articulum_normalized = normalize_text_for_articulum_search(articulum)
                title_original = listing.get('title', '') or ''
                snippet_original = listing.get('snippet_text', '') or ''

                # Нормализуем тексты
                title_normalized = normalize_text_for_articulum_search(title_original)
                snippet_normalized = normalize_text_for_articulum_search(snippet_original)

                # Проверяем наличие артикула
                if articulum_normalized not in title_normalized and articulum_normalized not in snippet_normalized:
                    rejection_reason = f'Артикул "{articulum}" не найден в названии или описании'

            # Проверка стоп-слов
            if not rejection_reason:
                text_combined = f"{title} {snippet} {seller}"
                for stopword in VALIDATION_STOPWORDS:
                    if stopword.lower() in text_combined:
                        rejection_reason = f'Найдено стоп-слово: "{stopword}"'
                        break

            # Проверка количества отзывов продавца (только если MIN_SELLER_REVIEWS > 0)
            if not rejection_reason and MIN_SELLER_REVIEWS > 0:
                seller_reviews = listing.get('seller_reviews')
                if seller_reviews is None or seller_reviews < MIN_SELLER_REVIEWS:
                    rejection_reason = f'Недостаточно отзывов продавца: {seller_reviews if seller_reviews is not None else "N/A"} < {MIN_SELLER_REVIEWS}'

            # Ценовая валидация (если включена и достаточно данных)
            if ENABLE_PRICE_VALIDATION and not rejection_reason and median_top40 is not None and price is not None:
                # Проверка на подозрительно дешевые (< 50% медианы топ-40%)
                if price < median_top40 * 0.5:
                    rejection_reason = f'Подозрительно низкая цена: {price} < {median_top40 * 0.5:.2f} (50% медианы топ-40%)'

                # Исключение выбросов по IQR методу
                elif outlier_upper_bound is not None and price > outlier_upper_bound:
                    rejection_reason = f'Выброс по цене (IQR): {price} > {outlier_upper_bound:.2f} (Q3 + 1.5×IQR)'

            # Сохранение результата
            if rejection_reason:
                await self.save_validation_result(
                    articulum_id,
                    avito_item_id,
                    'mechanical',
                    False,
                    rejection_reason
                )
            else:
                await self.save_validation_result(
                    articulum_id,
                    avito_item_id,
                    'mechanical',
                    True,
                    None
                )
                passed_listings.append(listing)

        self.logger.info(
            f"Mechanical validation: {len(passed_listings)}/{len(listings)} прошли проверку"
        )
        return passed_listings

    async def ai_validation(
        self,
        articulum_id: int,
        articulum: str,
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 3: ИИ-валидация через Vertex AI Gemini или HuggingFace"""
        # Проверка доступности AI клиента
        ai_available = (
            ENABLE_AI_VALIDATION and
            (self.vertex_client is not None or self.hf_client is not None)
        )
        if not ai_available:
            self.logger.info("ИИ-валидация пропущена (отключена или клиент не инициализирован)")
            return listings

        try:
            # Подготовка данных для промпта
            items_for_ai = []
            for listing in listings:
                # Конвертируем Decimal в float для JSON сериализации
                price = float(listing['price']) if listing.get('price') is not None else None
                items_for_ai.append({
                    'id': listing['avito_item_id'],
                    'title': listing.get('title', ''),
                    'price': price,
                    'snippet': listing.get('snippet_text', ''),
                    'seller': listing.get('seller_name', ''),
                })

            # Промпт для Gemini
            prompt = f"""
Ты эксперт по валидации автозапчастей с Авито. Твоя задача - отсеивать неоригинальные запчасти и подделки.

АРТИКУЛ ДЛЯ ПРОВЕРКИ: "{articulum}"
(Примечание: у запчасти может быть несколько артикулов, главное - чтобы "{articulum}" входил в их число)

ОБЪЯВЛЕНИЯ:
{json.dumps(items_for_ai, ensure_ascii=False)}

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

КРИТЕРИИ ПРИНЯТИЯ (PASS):

✓ Явное указание на оригинальность (OEM, оригинальный артикул)
✓ Бренд известного оригинального производителя
✓ Цена соответствует оригинальной запчасти
✓ Артикул "{articulum}" присутствует (может быть одним из нескольких)
✓ Отсутствие признаков подделки в описании

ВАЖНО: При малейших сомнениях в оригинальности - ОТКЛОНЯЙ объявление.

ВЕРНИ JSON объект:
{{
  "passed_ids": ["id1", "id2"],
  "rejected": [
    {{"id": "id3", "reason": "Неоригинальная запчасть - указан аналог"}},
    {{"id": "id4", "reason": "Подозрение на подделку - низкая цена"}}
  ]
}}
"""

            # Запрос к AI провайдеру
            if self.hf_client is not None:
                # HuggingFace Inference Endpoint
                raw_response = await self.hf_client.generate(prompt)
                self.logger.info(f"Сырой ответ от HuggingFace (первые 500 символов): {raw_response[:500]}")
            else:
                # Vertex AI (Gemini)
                client = await self.vertex_client.get_client()
                response = await client.chat.completions.create(
                    model=self.vertex_client.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "Ты эксперт по валидации объявлений. Всегда отвечай в JSON формате."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                raw_response = response.choices[0].message.content
                self.logger.info(f"Сырой ответ от Gemini (первые 500 символов): {raw_response[:500]}")

            try:
                ai_result = json.loads(raw_response)
                passed_ids = set(ai_result.get('passed_ids', []))
                rejected = {r['id']: r['reason'] for r in ai_result.get('rejected', [])}
            except json.JSONDecodeError as e:
                self.logger.error(f"Ошибка парсинга JSON от Gemini: {e}")
                self.logger.error(f"Полный ответ (первые 1000 символов): {raw_response[:1000]}")

                # Попытка partial parsing через regex
                import re
                try:
                    # Извлечь passed_ids массив через regex
                    match = re.search(r'"passed_ids"\s*:\s*\[(.*?)\]', raw_response, re.DOTALL)
                    if match:
                        ids_str = match.group(1)
                        passed_ids = set(re.findall(r'"(\d+)"', ids_str))
                        self.logger.warning(f"Partial parsing: извлечено {len(passed_ids)} passed IDs из обрезанного JSON")
                    else:
                        # Полный fallback - все прошли
                        self.logger.warning("Partial parsing не удался, используем полный fallback")
                        passed_ids = set([l['avito_item_id'] for l in listings])

                    # Попытка извлечь rejected (если есть)
                    rejected = {}
                    rejected_matches = re.findall(r'\{"id"\s*:\s*"(\d+)"\s*,\s*"reason"\s*:\s*"([^"]*)"', raw_response)
                    if rejected_matches:
                        rejected = {item_id: reason for item_id, reason in rejected_matches}
                        self.logger.info(f"Partial parsing: извлечено {len(rejected)} rejected записей")

                except Exception as parse_error:
                    self.logger.error(f"Ошибка partial parsing: {parse_error}")
                    # Финальный fallback - все прошли
                    passed_ids = set([l['avito_item_id'] for l in listings])
                    rejected = {}

                self.logger.warning(f"Итого после обработки ошибки: {len(passed_ids)} passed, {len(rejected)} rejected")

            # Сохранение результатов
            passed_listings = []
            for listing in listings:
                avito_item_id = listing['avito_item_id']

                if avito_item_id in passed_ids:
                    await self.save_validation_result(
                        articulum_id,
                        avito_item_id,
                        'ai',
                        True,
                        None
                    )
                    passed_listings.append(listing)
                else:
                    reason = rejected.get(avito_item_id, 'ИИ не посчитал релевантным')
                    await self.save_validation_result(
                        articulum_id,
                        avito_item_id,
                        'ai',
                        False,
                        reason
                    )

            self.logger.info(
                f"AI validation: {len(passed_listings)}/{len(listings)} прошли ИИ-проверку"
            )
            # Сбросить счетчик ошибок при успешной валидации
            self.ai_error_count = 0
            return passed_listings

        except Exception as e:
            # Увеличить счетчик последовательных ошибок
            self.ai_error_count += 1

            # ГРОМКОЕ ЛОГИРОВАНИЕ ОШИБКИ API
            self.logger.error("=" * 80)
            self.logger.error(f"!!! ОШИБКА ПРИ ИИ-ВАЛИДАЦИИ (#{self.ai_error_count} подряд) !!!")
            self.logger.error(f"Тип ошибки: {type(e).__name__}")
            self.logger.error(f"Сообщение: {e}")
            self.logger.error("=" * 80)
            self.logger.error("Полная трассировка:", exc_info=True)
            self.logger.error("=" * 80)

            # При 3+ ошибках подряд — воркер должен выключиться
            if self.ai_error_count >= 3:
                self.logger.critical("*" * 80)
                self.logger.critical(f"!!! {self.ai_error_count} ОШИБОК API ПОДРЯД - ВОРКЕР ВЫКЛЮЧАЕТСЯ !!!")
                self.logger.critical("!!! Код выхода: 2 (проблема с AI API) !!!")
                self.logger.critical("*" * 80)
                self.should_shutdown = True
                self.exit_code = 2

            # Бросаем исключение - артикул будет возвращен в очередь
            raise AIAPIError(f"Ошибка AI API (#{self.ai_error_count}): {e}")

    async def validate_articulum(self, articulum: Dict):
        """Главный метод валидации артикула (3 этапа)"""
        articulum_id = articulum['id']
        articulum_name = articulum['articulum']

        self.logger.info(f"Начало валидации артикула: {articulum_name} (id={articulum_id})")

        try:
            # Артикул уже в статусе VALIDATING (переведен в get_next_articulum)

            # Получение всех объявлений артикула
            listings = await self.get_listings_for_articulum(articulum_id)
            self.logger.info(f"Найдено {len(listings)} объявлений после парсинга каталога")

            # ПРОВЕРКА #0: Минимальное количество после парсинга каталога (до фильтров)
            if len(listings) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после парсинга каталога: {len(listings)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после парсинга каталога"
                    )
                return

            # ПРОВЕРКА #1: Фильтрация по MIN_PRICE
            listings_after_price = await self.price_filter_validation(articulum_id, listings)

            if len(listings_after_price) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после price filter: {len(listings_after_price)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после price filter"
                    )
                return

            # ПРОВЕРКА #2: Механическая валидация (проверка артикула + стоп-слова + ценовая проверка оригинальности)
            listings_after_mechanical = await self.mechanical_validation(articulum_id, articulum_name, listings_after_price)

            if len(listings_after_mechanical) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после mechanical validation: {len(listings_after_mechanical)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после mechanical validation"
                    )
                return

            # ПРОВЕРКА #3: ИИ-валидация (Gemini)
            listings_after_ai = await self.ai_validation(
                articulum_id,
                articulum_name,
                listings_after_mechanical
            )

            if ENABLE_AI_VALIDATION and len(listings_after_ai) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после AI validation: {len(listings_after_ai)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после AI validation"
                    )
                return

            # ВСЕ ЭТАПЫ ПРОЙДЕНЫ → VALIDATED
            self.logger.info(
                f"Валидация успешна: {len(listings_after_ai)} объявлений прошли все проверки"
            )

            # Переводим в VALIDATED и создаем object_tasks (если парсинг объявлений включен)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await transition_to_validated(conn, articulum_id)

                    if not SKIP_OBJECT_PARSING:
                        # Создаем object_tasks для объявлений, прошедших валидацию
                        tasks_created = await create_object_tasks_for_articulum(conn, articulum_id)
                        self.logger.info(f"Создано {tasks_created} object_tasks для артикула {articulum_id}")
                    else:
                        self.logger.info("Парсинг объявлений отключен, object_tasks не создаются")

        except AIAPIError as e:
            # Ошибка AI API - возвращаем артикул в очередь
            self.logger.warning(f"AI API ошибка для артикула {articulum_id}: {e}")
            self.logger.warning(f"Возвращаем артикул {articulum_id} в CATALOG_PARSED")
            async with self.pool.acquire() as conn:
                await rollback_to_catalog_parsed(conn, articulum_id, "AI API error")

        except Exception as e:
            self.logger.error(f"Ошибка при валидации артикула {articulum_id}: {e}", exc_info=True)

    async def run(self) -> int:
        """Главный цикл воркера. Возвращает код выхода."""
        try:
            await self.init()

            self.logger.info("Validation Worker запущен, ожидание артикулов для валидации...")

            while not self.should_shutdown:
                try:
                    # Получить следующий артикул
                    articulum = await self.get_next_articulum()

                    if articulum:
                        await self.validate_articulum(articulum)

                        # Проверить флаг после валидации
                        if self.should_shutdown:
                            self.logger.warning("Воркер завершается из-за проблем с AI API")
                            break
                    else:
                        # Нет артикулов для валидации
                        await asyncio.sleep(10)

                except KeyboardInterrupt:
                    self.logger.info("Получен сигнал остановки")
                    break
                except Exception as e:
                    self.logger.error(f"Ошибка в главном цикле: {e}", exc_info=True)
                    await asyncio.sleep(5)

        except KeyboardInterrupt:
            self.logger.info("Получен сигнал остановки (KeyboardInterrupt)")
        except asyncio.CancelledError:
            self.logger.info("Воркер отменен (CancelledError)")
        except Exception as e:
            self.logger.error(f"Ошибка в воркере: {e}", exc_info=True)
            self.exit_code = 1
        finally:
            # Закрытие HuggingFace клиента
            if self.hf_client:
                try:
                    await self.hf_client.close()
                    self.logger.info("HuggingFace клиент закрыт")
                except Exception as e:
                    self.logger.warning(f"Ошибка при закрытии HF клиента: {e}")

            # Закрытие пула БД
            if self.pool:
                try:
                    await self.pool.close()
                    self.logger.info("Пул БД закрыт")
                except Exception as e:
                    self.logger.warning(f"Ошибка при закрытии пула БД: {e}")

            self.logger.info(f"Validation Worker завершен (код выхода: {self.exit_code})")

        return self.exit_code


async def main() -> int:
    """Точка входа для Validation Worker. Возвращает код выхода."""
    # Worker ID из аргументов командной строки
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "0"

    worker = ValidationWorker(worker_id)
    return await worker.run()


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        # Graceful shutdown - не показываем traceback
        logging.info("Воркер остановлен пользователем")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logging.error(f"Критическая ошибка воркера: {e}", exc_info=True)
        sys.exit(1)
