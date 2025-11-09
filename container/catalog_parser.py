"""Парсинг каталогов через avito-library"""

import asyncpg
import logging
from playwright.async_api import Page
from typing import List, Tuple

from avito_library.parsers.catalog_parser import (
    parse_catalog_until_complete,
    CatalogListing,
    CatalogParseMeta,
)
from config import CATALOG_MAX_PAGES, CATALOG_INCLUDE_HTML, CATALOG_FIELDS

logger = logging.getLogger(__name__)


def build_catalog_url(articulum: str) -> str:
    """
    Построение URL каталога для поиска по артикулу.

    Формат: https://www.avito.ru/rossiya?q={articulum}
    Сортировка по дате добавляется библиотекой через параметр sort_by_date=True
    """
    base_url = "https://www.avito.ru/rossiya"
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


async def save_listings_to_db(
    conn: asyncpg.Connection,
    articulum_id: int,
    listings: List[CatalogListing]
) -> int:
    """
    Сохраняет объявления из каталога в БД.

    Удаляет дубликаты по title + snippet_text перед сохранением.
    Обрабатывает дубликаты по avito_item_id (ON CONFLICT DO NOTHING).
    Возвращает количество сохраненных объявлений.
    """
    # Удаляем дубликаты по title + snippet_text
    unique_listings, removed_count = deduplicate_listings(listings)

    if removed_count > 0:
        logger.info(f"Удалено {removed_count} дубликатов (одинаковые title + snippet_text)")

    saved_count = 0

    for listing in unique_listings:
        try:
            await conn.execute("""
                INSERT INTO catalog_listings (
                    articulum_id,
                    avito_item_id,
                    title,
                    price,
                    snippet_text,
                    seller_name,
                    seller_id,
                    seller_rating,
                    seller_reviews
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
            )
            saved_count += 1
        except Exception as e:
            # Логируем ошибку, но продолжаем сохранять остальные
            logger.error(f"Ошибка при сохранении объявления {listing.item_id}: {e}")
            continue

    return saved_count


async def parse_catalog_for_articulum(
    page: Page,
    articulum: str,
    start_page: int = 1
) -> Tuple[List[CatalogListing], CatalogParseMeta]:
    """
    Парсит каталог по артикулу через parse_catalog_until_complete.

    Использует настройки из config.py:
    - CATALOG_MAX_PAGES: максимум страниц
    - CATALOG_INCLUDE_HTML: сохранять ли HTML
    - CATALOG_FIELDS: поля для извлечения

    Возвращает (список объявлений, метаданные парсинга).
    """
    catalog_url = build_catalog_url(articulum)

    listings, meta = await parse_catalog_until_complete(
        page,
        catalog_url,
        fields=CATALOG_FIELDS,
        max_pages=CATALOG_MAX_PAGES,
        sort_by_date=True,  # Сортировка по дате
        include_html=CATALOG_INCLUDE_HTML,
        start_page=start_page,
    )

    return listings, meta
