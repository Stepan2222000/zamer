"""Validation Worker - валидация объявлений без браузера"""

import asyncio
import logging
import sys
import json
import statistics
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from google.auth import default
import google.auth.transport.requests
from openai import AsyncOpenAI

from database import create_pool
from config import (
    MIN_PRICE,
    MIN_VALIDATED_ITEMS,
    ENABLE_AI_VALIDATION,
    VALIDATION_STOPWORDS,
    VERTEX_AI_PROJECT_ID,
    VERTEX_AI_LOCATION,
    VERTEX_AI_MODEL,
    GOOGLE_APPLICATION_CREDENTIALS,
    SKIP_OBJECT_PARSING,
    ArticulumState,
)
from state_machine import (
    transition_to_validating,
    transition_to_validated,
    reject_articulum,
)
from object_task_manager import create_object_tasks_for_articulum

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [VALIDATION-%(worker_id)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


class VertexAIClient:
    """Wrapper для Vertex AI через OpenAI SDK с автообновлением OAuth токенов"""

    def __init__(self, project_id: str, location: str, model: str):
        self.project_id = project_id
        self.location = location
        self.model = model
        self.credentials = None
        self.token_expiry = None
        self._client = None

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
            base_url = f"https://{self.location}-aiplatform.googleapis.com/v1/projects/{self.project_id}/locations/{self.location}/endpoints/openapi"
            self._client = AsyncOpenAI(
                base_url=base_url,
                api_key=self.credentials.token,
            )

    async def get_client(self) -> AsyncOpenAI:
        """Получить AsyncOpenAI клиент с валидным токеном"""
        self._refresh_token()
        return self._client


class ValidationWorker:
    """Воркер для валидации объявлений (БЕЗ браузера)"""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.logger = logging.LoggerAdapter(
            logging.getLogger(__name__),
            {'worker_id': worker_id}
        )
        self.pool = None
        self.vertex_client = None

        # Инициализация Vertex AI клиента если включена ИИ-валидация
        if ENABLE_AI_VALIDATION:
            try:
                self.vertex_client = VertexAIClient(
                    project_id=VERTEX_AI_PROJECT_ID,
                    location=VERTEX_AI_LOCATION,
                    model=VERTEX_AI_MODEL
                )
                self.logger.info(f"Vertex AI инициализирован: {VERTEX_AI_MODEL}")
            except Exception as e:
                self.logger.warning(f"Не удалось инициализировать Vertex AI: {e}")
                self.logger.info("Продолжаем без ИИ-валидации")
                self.vertex_client = None
        else:
            self.logger.info("ИИ-валидация отключена (нет Service Account credentials)")

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
        Применяет фильтр MIN_PRICE при выборке.
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
                    seller_rating
                FROM catalog_listings
                WHERE articulum_id = $1
                  AND (price IS NULL OR price >= $2)
            """, articulum_id, MIN_PRICE)

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
        """Этап 1: Фильтрация по MIN_PRICE"""
        passed_listings = []

        for listing in listings:
            price = listing.get('price')
            avito_item_id = listing['avito_item_id']

            # Проверка цены
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
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 2: Механическая валидация (стоп-слова + ценовая проверка)"""
        passed_listings = []
        # Конвертируем Decimal в float для математических операций
        prices = [float(l['price']) for l in listings if l.get('price') is not None]

        # Вычисление статистики для ценовой валидации
        if len(prices) >= 1:
            # Топ-20% по цене
            prices_sorted = sorted(prices, reverse=True)
            top20_count = max(1, len(prices_sorted) // 5)
            top20_prices = prices_sorted[:top20_count]
            median_top20 = statistics.median(top20_prices)

            # Общая медиана для исключения выбросов
            median_all = statistics.median(prices)
            outlier_threshold = median_all * 3
        else:
            median_top20 = None
            outlier_threshold = None

        for listing in listings:
            avito_item_id = listing['avito_item_id']
            title = (listing.get('title') or '').lower()
            snippet = (listing.get('snippet_text') or '').lower()
            seller = (listing.get('seller_name') or '').lower()
            # Конвертируем Decimal в float для математических операций
            price = float(listing['price']) if listing.get('price') is not None else None

            rejection_reason = None

            # Проверка стоп-слов
            text_combined = f"{title} {snippet} {seller}"
            for stopword in VALIDATION_STOPWORDS:
                if stopword.lower() in text_combined:
                    rejection_reason = f'Найдено стоп-слово: "{stopword}"'
                    break

            # Ценовая валидация (если достаточно данных)
            if not rejection_reason and median_top20 is not None and price is not None:
                # Проверка на подозрительно дешевые (< 50% медианы топ-20%)
                if price < median_top20 * 0.5:
                    rejection_reason = f'Подозрительно низкая цена: {price} < {median_top20 * 0.5:.2f} (50% медианы топ-20%)'

                # Исключение выбросов (> 3× общей медианы)
                elif outlier_threshold is not None and price > outlier_threshold:
                    rejection_reason = f'Выброс по цене: {price} > {outlier_threshold:.2f} (3× медианы)'

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
        """Этап 3: ИИ-валидация через Vertex AI Gemini"""
        if not ENABLE_AI_VALIDATION or not self.vertex_client:
            self.logger.info("ИИ-валидация пропущена (отключена)")
            return listings

        try:
            client = await self.vertex_client.get_client()

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
Ты помощник для валидации объявлений с Авито.

ЗАДАЧА: Определи какие объявления релевантны для артикула "{articulum}".

ОБЪЯВЛЕНИЯ:
{json.dumps(items_for_ai, ensure_ascii=False, indent=2)}

КРИТЕРИИ РЕЛЕВАНТНОСТИ:
1. Объявление описывает ОРИГИНАЛЬНЫЙ товар с артикулом "{articulum}"
2. Это не копия, реплика, имитация или похожий товар
3. Продавец предлагает именно то, что соответствует артикулу
4. Цена адекватна для оригинального товара (не подозрительно низкая)

ВЕРНИ JSON объект с ключами:
- "passed_ids": список id релевантных объявлений
- "rejected": список объектов с ключами "id" и "reason" для нерелевантных

Пример ответа:
{{
  "passed_ids": ["123", "456"],
  "rejected": [
    {{"id": "789", "reason": "Подозрение на копию - низкая цена"}},
    {{"id": "101", "reason": "Неоригинальный товар по описанию"}}
  ]
}}
"""

            # Запрос к Gemini
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
                max_tokens=2000,
                response_format={"type": "json_object"}
            )

            # Парсинг ответа
            raw_response = response.choices[0].message.content
            self.logger.info(f"Сырой ответ от Gemini (первые 500 символов): {raw_response[:500]}")

            try:
                ai_result = json.loads(raw_response)
                passed_ids = set(ai_result.get('passed_ids', []))
                rejected = {r['id']: r['reason'] for r in ai_result.get('rejected', [])}
            except json.JSONDecodeError as e:
                self.logger.error(f"Ошибка парсинга JSON от Gemini: {e}")
                self.logger.error(f"Полный ответ: {raw_response}")

                # Fallback: считаем все объявления прошедшими валидацию
                self.logger.warning("Используем fallback: все объявления помечены как прошедшие ИИ-валидацию")
                passed_ids = set([l['avito_item_id'] for l in listings])
                rejected = {}

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
            return passed_listings

        except Exception as e:
            self.logger.error(f"Ошибка при ИИ-валидации: {e}", exc_info=True)
            # При ошибке ИИ - пропускаем этап, все листинги проходят
            self.logger.warning("ИИ-валидация пропущена из-за ошибки, все объявления прошли")
            return listings

    async def validate_articulum(self, articulum: Dict):
        """Главный метод валидации артикула (3 этапа)"""
        articulum_id = articulum['id']
        articulum_name = articulum['articulum']

        self.logger.info(f"Начало валидации артикула: {articulum_name} (id={articulum_id})")

        try:
            # Артикул уже в статусе VALIDATING (переведен в get_next_articulum)

            # Получение всех объявлений (с фильтром MIN_PRICE при выборке)
            listings = await self.get_listings_for_articulum(articulum_id)
            self.logger.info(f"Найдено {len(listings)} объявлений для валидации (уже отфильтровано по MIN_PRICE={MIN_PRICE})")

            if len(listings) == 0:
                self.logger.warning(f"Нет объявлений для валидации, отклоняем артикул")
                async with self.pool.acquire() as conn:
                    await reject_articulum(conn, articulum_id, "Нет объявлений")
                return

            # ЭТАП 1: Фильтрация по цене
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

            # ЭТАП 2: Механическая валидация
            listings_after_mechanical = await self.mechanical_validation(articulum_id, listings_after_price)

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

            # ЭТАП 3: ИИ-валидация (опционально)
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

        except Exception as e:
            self.logger.error(f"Ошибка при валидации артикула {articulum_id}: {e}", exc_info=True)

    async def run(self):
        """Главный цикл воркера"""
        try:
            await self.init()

            self.logger.info("Validation Worker запущен, ожидание артикулов для валидации...")

            while True:
                try:
                    # Получить следующий артикул
                    articulum = await self.get_next_articulum()

                    if articulum:
                        await self.validate_articulum(articulum)
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
        finally:
            # Закрытие пула БД
            if self.pool:
                try:
                    await self.pool.close()
                    self.logger.info("Пул БД закрыт")
                except Exception as e:
                    self.logger.warning(f"Ошибка при закрытии пула БД: {e}")

            self.logger.info("Validation Worker завершен")


async def main():
    """Точка входа для Validation Worker"""
    # Worker ID из аргументов командной строки
    worker_id = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    worker = ValidationWorker(worker_id)
    await worker.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful shutdown - не показываем traceback
        logging.info("Воркер остановлен пользователем")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logging.error(f"Критическая ошибка воркера: {e}", exc_info=True)
        sys.exit(1)
