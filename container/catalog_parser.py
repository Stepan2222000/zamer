"""Парсинг каталогов через avito-library"""

import asyncpg
import logging
from playwright.async_api import Page
from typing import List, Tuple, Optional
import json

from avito_library import (
    parse_catalog,
    CatalogListing,
    CatalogParseResult,
)
from config import (
    CATALOG_MAX_PAGES,
    CATALOG_INCLUDE_HTML,
    CATALOG_FIELDS,
    COLLECT_IMAGES,
    SAVE_IMAGES_BYTES,
    MAX_IMAGES_PER_LISTING,
    MIN_PRICE,
)

logger = logging.getLogger(__name__)


def build_catalog_url(articulum: str) -> str:
    """
    Построение URL каталога для поиска по артикулу.

    Формат: https://www.avito.ru/rossiya/zapchasti?q={articulum}
    Категория "zapchasti" добавлена для точности поиска.
    Сортировка по дате добавляется библиотекой через параметр sort="date"
    """
    base_url = "https://www.avito.ru/rossiya/zapchasti"
    # URL-кодирование артикула для безопасности
    from urllib.parse import quote
    encoded_articulum = quote(articulum)

    return f"{base_url}?q={encoded_articulum}"


def deduplicate_listings(listings: List[CatalogListing]) -> Tuple[List[CatalogListing], int]:
    """
    Удаляет дубликаты по комбинации title + snippet_text.

    Возвращает (уникальные объявления, количество удаленных дубликатов).
    """
    seen = set()
    unique_listings = []

    for listing in listings:
        # Ключ дедупликации: title + snippet_text
        key = (listing.title, listing.snippet_text)

        if key not in seen:
            seen.add(key)
            unique_listings.append(listing)

    removed_count = len(listings) - len(unique_listings)
    return unique_listings, removed_count


def extract_images_data(listing: CatalogListing) -> Tuple[Optional[str], Optional[List[bytes]], Optional[int]]:
    """
    Извлекает данные изображений из объявления.

    Возвращает:
    - images_urls_json: JSON-строка с URLs (или None)
    - images_bytes: список байтов изображений (или None)
    - images_count: количество изображений (или None если не запрашивалось)
    """
    if not COLLECT_IMAGES:
        return None, None, None

    # Получаем данные изображений из listing
    # avito-library возвращает: images (list[bytes]), images_urls (list[str])
    images_bytes_raw = getattr(listing, 'images', None) or []
    images_urls_raw = getattr(listing, 'images_urls', None) or []

    # Ограничиваем количество изображений
    images_bytes_limited = images_bytes_raw[:MAX_IMAGES_PER_LISTING]
    images_urls_limited = images_urls_raw[:MAX_IMAGES_PER_LISTING]

    # Количество изображений (используем максимум из обоих источников)
    images_count = max(len(images_bytes_limited), len(images_urls_limited))

    # URLs в JSON
    images_urls_json = json.dumps(images_urls_limited) if images_urls_limited else None

    # Байты сохраняем только если включено
    images_bytes_result = images_bytes_limited if SAVE_IMAGES_BYTES and images_bytes_limited else None

    return images_urls_json, images_bytes_result, images_count


async def save_listings_to_db(
    conn: asyncpg.Connection,
    articulum_id: int,
    listings: List[CatalogListing]
) -> int:
    """
    Сохраняет объявления из каталога в БД.

    Удаляет дубликаты по title + snippet_text перед сохранением.
    Обрабатывает дубликаты по avito_item_id (ON CONFLICT DO NOTHING).
    Сохраняет изображения если COLLECT_IMAGES=true.
    Возвращает количество сохраненных объявлений.
    """
    # Удаляем дубликаты по title + snippet_text
    unique_listings, removed_count = deduplicate_listings(listings)

    if removed_count > 0:
        logger.info(f"Удалено {removed_count} дубликатов (одинаковые title + snippet_text)")

    # ВАЖНО: Не используем try/except внутри транзакции!
    # SQL ошибка переводит транзакцию в "aborted" состояние,
    # после чего все команды выдают InFailedSQLTransactionError

    saved_count = 0
    images_saved_count = 0

    for listing in unique_listings:
        # Извлекаем данные изображений
        images_urls_json, images_bytes_data, images_count = extract_images_data(listing)

        result = await conn.execute("""
            INSERT INTO catalog_listings (
                articulum_id,
                avito_item_id,
                title,
                price,
                snippet_text,
                seller_name,
                seller_id,
                seller_rating,
                seller_reviews,
                images_urls,
                images_bytes,
                images_count
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (avito_item_id) DO NOTHING
        """,
            articulum_id,
            listing.item_id,
            listing.title,
            listing.price,
            listing.snippet_text,
            listing.seller_name,
            listing.seller_id,
            listing.seller_rating,
            listing.seller_reviews,
            images_urls_json,
            images_bytes_data,
            images_count,
        )

        # ON CONFLICT DO NOTHING возвращает "INSERT 0" если был конфликт
        # и "INSERT 0 1" если была вставка
        if "INSERT 0 1" in result:
            saved_count += 1
            if images_count and images_count > 0:
                images_saved_count += 1

    if COLLECT_IMAGES:
        logger.info(f"Сохранено {images_saved_count} объявлений с изображениями")

    return saved_count


async def parse_catalog_for_articulum(
    page: Page,
    articulum: str,
    start_page: int = 1
) -> CatalogParseResult:
    """
    Парсит каталог по артикулу через parse_catalog.

    Использует настройки из config.py:
    - CATALOG_MAX_PAGES: максимум страниц
    - CATALOG_INCLUDE_HTML: сохранять ли HTML
    - CATALOG_FIELDS: поля для извлечения
    - MIN_PRICE: минимальная цена (фильтр на уровне Avito)

    Фильтры на уровне Avito:
    - price_min: отсекает дешёвые объявления ещё до парсинга
    - condition: только новые товары

    Возвращает CatalogParseResult.
    """
    catalog_url = build_catalog_url(articulum)

    result = await parse_catalog(
        page,
        catalog_url,
        fields=CATALOG_FIELDS,
        max_pages=CATALOG_MAX_PAGES,
        sort="date",  # Сортировка по дате
        include_html=CATALOG_INCLUDE_HTML,
        start_page=start_page,
        # Фильтры на уровне Avito (двойная защита с validation_worker)
        price_min=int(MIN_PRICE),  # Минимальная цена из config.py
        condition="Новый",  # Только новые товары
    )

    return result
