"""Validation Worker - валидация объявлений без браузера"""

import asyncio
import hashlib
import logging
import re
import sys
import statistics
from typing import Dict, List, Optional

from database import create_pool
from config import (
    MIN_PRICE,
    MIN_VALIDATED_ITEMS,
    MIN_SELLER_REVIEWS,
    ENABLE_PRICE_VALIDATION,
    ENABLE_AI_VALIDATION,
    VALIDATION_STOPWORDS,
    SKIP_OBJECT_PARSING,
    ArticulumState,
    # Параметры изображений
    COLLECT_IMAGES,
    REQUIRE_IMAGES,
    AI_USE_IMAGES,
    AI_MAX_IMAGES_PER_LISTING,
    # AI провайдер
    AI_PROVIDER,
)
from state_machine import (
    transition_to_validated,
    reject_articulum,
    rollback_to_catalog_parsed,
)
from object_task_manager import create_object_tasks_for_articulum
from ai_provider import (
    create_provider,
    convert_listing_dict_to_validation,
    AIProviderError,
)


# JSON Schema для structured output (grammar parameter TGI)
# Гарантирует что модель вернёт валидный JSON с нужной структурой
VALIDATION_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "passed_ids": {
            "type": "array",
            "items": {"type": "string"}
        },
        "rejected": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "reason": {"type": "string"}
                },
                "required": ["id", "reason"]
            }
        }
    },
    "required": ["passed_ids", "rejected"]
}


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
logging.getLogger('urllib3').setLevel(logging.WARNING)


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
        self.hf_client = None
        self.ai_provider = None
        self.ai_error_count = 0  # Счетчик последовательных ошибок API
        self.should_shutdown = False  # Флаг для graceful shutdown
        self.exit_code = 0  # Код выхода (2 = проблема с API)
        if ENABLE_AI_VALIDATION:
            self.ai_provider = create_provider(AI_PROVIDER)
            self.logger.info(f"AI провайдер: {self.ai_provider} (тип: {AI_PROVIDER})")
            if AI_USE_IMAGES:
                self.logger.info(f"Мультимодальная валидация включена (до {AI_MAX_IMAGES_PER_LISTING} изображений)")
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
        Включает images_bytes и images_count если нужны для валидации.
        """
        async with self.pool.acquire() as conn:
            # Базовые поля
            base_fields = """
                avito_item_id,
                title,
                price,
                snippet_text,
                seller_name,
                seller_id,
                seller_rating,
                seller_reviews,
                images_count
            """

            # Добавляем images_bytes если нужны для AI валидации
            if AI_USE_IMAGES and ENABLE_AI_VALIDATION:
                fields = base_fields + ", images_bytes"
            else:
                fields = base_fields

            rows = await conn.fetch(f"""
                SELECT {fields}
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
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 2: Механическая валидация (проверка изображений + стоп-слова + ценовая проверка)"""

        # ПРОВЕРКА ИЗОБРАЖЕНИЙ (если включено)
        # Выполняется ПЕРЕД остальными проверками для быстрого отсева
        if COLLECT_IMAGES and REQUIRE_IMAGES:
            listings_with_images = []
            rejected_no_images = 0

            for listing in listings:
                avito_item_id = listing['avito_item_id']
                images_count = listing.get('images_count')

                # Если images_count=0 или None (не запрашивалось) — отклоняем
                if images_count is None or images_count == 0:
                    await self.save_validation_result(
                        articulum_id,
                        avito_item_id,
                        'mechanical',
                        False,
                        'Объявление без изображений'
                    )
                    rejected_no_images += 1
                else:
                    listings_with_images.append(listing)

            if rejected_no_images > 0:
                self.logger.info(
                    f"Проверка изображений: отклонено {rejected_no_images}/{len(listings)} объявлений без фото"
                )

            # Продолжаем с объявлениями, у которых есть изображения
            listings = listings_with_images

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

            # Проверка стоп-слов (поиск по границам слов, не подстрокам)
            if not rejection_reason:
                text_combined = f"{title} {snippet} {seller}"
                for stopword in VALIDATION_STOPWORDS:
                    pattern = r'\b' + re.escape(stopword.lower()) + r'\b'
                    if re.search(pattern, text_combined):
                        rejection_reason = f'Найдено стоп-слово: "{stopword}"'
                        break

            # Проверка количества отзывов продавца (только если MIN_SELLER_REVIEWS > 0)
            if not rejection_reason and MIN_SELLER_REVIEWS > 0:
                seller_reviews = listing.get('seller_reviews')
                if seller_reviews is None or seller_reviews < MIN_SELLER_REVIEWS:
                    rejection_reason = f'Недостаточно отзывов продавца: {seller_reviews if seller_reviews is not None else "N/A"} < {MIN_SELLER_REVIEWS}'

            # Ценовая валидация (если включена и достаточно данных)
            if ENABLE_PRICE_VALIDATION and not rejection_reason and median_top40 is not None and price is not None:
                # Проверка на подозрительно дешевые (< 20% медианы топ-40%)
                if price < median_top40 * 0.2:
                    rejection_reason = f'Подозрительно низкая цена: {price} < {median_top40 * 0.2:.2f} (20% медианы топ-40%)'

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

    async def seller_dedup(
        self,
        articulum_id: int,
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 2.5: Дедупликация по продавцу — оставляем одно объявление на продавца.

        Ключ: seller_id (если есть), иначе seller_name.
        Приоритет: больше изображений → ниже цена.
        """
        # Группируем по продавцу
        seller_groups: Dict[str, List[Dict]] = {}
        for listing in listings:
            key = listing.get('seller_id') or listing.get('seller_name') or ''
            seller_groups.setdefault(key, []).append(listing)

        kept = []
        rejected_count = 0

        for seller_key, group in seller_groups.items():
            if len(group) == 1:
                kept.append(group[0])
                continue

            # Сортируем: больше изображений → ниже цена
            group.sort(key=lambda l: (
                -(l.get('images_count') or 0),
                float(l.get('price') or 0),
            ))

            best = group[0]
            kept.append(best)

            # Остальные — отклоняем
            for dup in group[1:]:
                await self.save_validation_result(
                    articulum_id,
                    dup['avito_item_id'],
                    'seller_dedup',
                    False,
                    f'Дубль продавца "{seller_key}", оставлено {best["avito_item_id"]}'
                )
                rejected_count += 1

        if rejected_count > 0:
            self.logger.info(
                f"Seller dedup: {len(kept)}/{len(listings)} уникальных продавцов "
                f"(отсеяно {rejected_count} дублей)"
            )
        return kept

    async def image_hash_dedup(
        self,
        articulum_id: int,
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 2.7: Дедупликация по MD5-хэшу первого изображения.

        Ключ: MD5 хэш байтов первого изображения.
        Приоритет: больше изображений → ниже цена.
        Объявления без изображений пропускаются (не дедуплицируются).
        """
        hash_groups: Dict[str, List[Dict]] = {}
        no_image = []

        for listing in listings:
            images_bytes = listing.get('images_bytes') or []
            if not images_bytes or len(images_bytes) == 0:
                no_image.append(listing)
                continue

            img = images_bytes[0]
            if isinstance(img, memoryview):
                img = bytes(img)
            if not img:
                no_image.append(listing)
                continue

            img_hash = hashlib.md5(img).hexdigest()
            hash_groups.setdefault(img_hash, []).append(listing)

        kept = list(no_image)
        rejected_count = 0

        for img_hash, group in hash_groups.items():
            if len(group) == 1:
                kept.append(group[0])
                continue

            # Сортируем: больше изображений → ниже цена
            group.sort(key=lambda l: (
                -(l.get('images_count') or 0),
                float(l.get('price') or 0),
            ))

            best = group[0]
            kept.append(best)

            for dup in group[1:]:
                await self.save_validation_result(
                    articulum_id,
                    dup['avito_item_id'],
                    'image_dedup',
                    False,
                    f'Дубль изображения (MD5), оставлено {best["avito_item_id"]}'
                )
                rejected_count += 1

        if rejected_count > 0:
            self.logger.info(
                f"Image hash dedup: {len(kept)}/{len(listings)} уникальных изображений "
                f"(отсеяно {rejected_count} дублей)"
            )
        return kept

    async def ai_validation(
        self,
        articulum_id: int,
        articulum: str,
        listings: List[Dict]
    ) -> List[Dict]:
        """Этап 3: ИИ-валидация через AI провайдер (Fireworks, Dummy и т.д.)"""
        # Проверка доступности AI провайдера
        if not ENABLE_AI_VALIDATION or self.ai_provider is None:
            self.logger.info("ИИ-валидация пропущена (отключена или провайдер не инициализирован)")
            return listings

        try:
            # Определяем, используем ли изображения
            use_images = AI_USE_IMAGES and COLLECT_IMAGES

            # Ограничиваем до 30 объявлений (лимит Fireworks: 30 изображений на запрос)
            # TODO: убрать после реализации батчинга
            MAX_LISTINGS_FOR_AI = 30
            ai_listings = listings[:MAX_LISTINGS_FOR_AI]
            if len(listings) > MAX_LISTINGS_FOR_AI:
                self.logger.warning(f"AI: обрезано {len(listings)} → {MAX_LISTINGS_FOR_AI} объявлений (лимит Fireworks)")

            # Конвертируем listings в ListingForValidation
            listings_for_ai = [
                convert_listing_dict_to_validation(listing, AI_MAX_IMAGES_PER_LISTING)
                for listing in ai_listings
            ]

            # Вызов AI провайдера
            result = await self.ai_provider.validate(articulum, listings_for_ai, use_images)

            # Сохранение результатов в БД
            passed_ids = set(result.passed_ids)
            rejected_dict = {r.avito_item_id: r.reason for r in result.rejected}

            passed_listings = []
            for listing in ai_listings:
                avito_item_id = listing['avito_item_id']

                if avito_item_id in passed_ids:
                    await self.save_validation_result(
                        articulum_id, avito_item_id, 'ai', True, None
                    )
                    passed_listings.append(listing)
                else:
                    reason = rejected_dict.get(avito_item_id, 'ИИ не посчитал релевантным')
                    await self.save_validation_result(
                        articulum_id, avito_item_id, 'ai', False, reason
                    )

            # Объявления за пределами лимита — не отправлялись в AI, пропускаем
            if len(listings) > MAX_LISTINGS_FOR_AI:
                skipped = len(listings) - MAX_LISTINGS_FOR_AI
                self.logger.info(f"AI validation: {skipped} объявлений пропущено (не отправлялись в AI)")

            self.logger.info(
                f"AI validation: {len(passed_listings)}/{len(ai_listings)} прошли ИИ-проверку"
            )
            # Сбросить счетчик ошибок при успешной валидации
            self.ai_error_count = 0
            return passed_listings

        except AIProviderError as e:
            # Ошибка AI провайдера — увеличить счетчик
            self.ai_error_count += 1

            self.logger.error("=" * 80)
            self.logger.error(f"!!! ОШИБКА AI ПРОВАЙДЕРА (#{self.ai_error_count} подряд) !!!")
            self.logger.error(f"Сообщение: {e}")
            self.logger.error("=" * 80)

            # При 3+ ошибках подряд — воркер должен выключиться
            if self.ai_error_count >= 3:
                self.logger.critical("*" * 80)
                self.logger.critical(f"!!! {self.ai_error_count} ОШИБОК API ПОДРЯД - ВОРКЕР ВЫКЛЮЧАЕТСЯ !!!")
                self.logger.critical("*" * 80)
                self.should_shutdown = True
                self.exit_code = 2

            # Бросаем исключение — артикул вернётся в очередь
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

            # ПРОВЕРКА #2: Механическая валидация (стоп-слова + изображения + ценовая проверка)
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

            # ПРОВЕРКА #2.5: Дедупликация по продавцу (одно объявление на продавца)
            listings_after_dedup = await self.seller_dedup(articulum_id, listings_after_mechanical)

            if len(listings_after_dedup) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после seller dedup: {len(listings_after_dedup)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после seller dedup"
                    )
                return

            # ПРОВЕРКА #2.7: Дедупликация по хэшу изображений (MD5)
            listings_after_img_dedup = await self.image_hash_dedup(articulum_id, listings_after_dedup)

            if len(listings_after_img_dedup) < MIN_VALIDATED_ITEMS:
                self.logger.warning(
                    f"Недостаточно объявлений после image hash dedup: {len(listings_after_img_dedup)} < {MIN_VALIDATED_ITEMS}"
                )
                async with self.pool.acquire() as conn:
                    await reject_articulum(
                        conn,
                        articulum_id,
                        f"Менее {MIN_VALIDATED_ITEMS} объявлений после image hash dedup"
                    )
                return

            # ПРОВЕРКА #3: ИИ-валидация (Fireworks AI)
            listings_after_ai = await self.ai_validation(
                articulum_id,
                articulum_name,
                listings_after_img_dedup
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
            # Закрытие AI провайдера
            if self.ai_provider:
                try:
                    await self.ai_provider.close()
                    self.logger.info("AI провайдер закрыт")
                except Exception as e:
                    self.logger.warning(f"Ошибка при закрытии AI провайдера: {e}")

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
