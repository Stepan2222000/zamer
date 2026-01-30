"""Парсинг карточек объявлений через avito-library"""

import asyncpg
import logging
import json
from datetime import datetime
from avito_library import CardData
from config import OBJECT_INCLUDE_HTML

logger = logging.getLogger(__name__)


async def save_object_data_to_db(
    conn: asyncpg.Connection,
    articulum_id: int,
    avito_item_id: str,
    card_data: CardData,
    raw_html: str = None
) -> int:
    """
    Сохраняет детальные данные объявления в БД.
    Создает НОВУЮ запись при каждом парсинге (для анализа динамики).
    """
    # Парсинг seller
    seller_name = card_data.seller.get('name') if card_data.seller else None
    # Поля seller_id и seller_rating удалены в новой версии библиотеки
    seller_id = None
    seller_rating = None

    # Парсинг location
    location_name = card_data.location.get('address') if card_data.location else None
    # Поле coords удалено в новой версии библиотеки
    location_coords = None

    # Characteristics как JSONB
    characteristics_json = json.dumps(card_data.characteristics, ensure_ascii=False) if card_data.characteristics else None

    # Парсинг published_at
    published_at = None
    if card_data.published_at:
        try:
            published_at = datetime.fromisoformat(card_data.published_at)
        except (ValueError, AttributeError):
            logger.warning(f"Не удалось распарсить published_at: {card_data.published_at}")

    # INSERT в БД
    record_id = await conn.fetchval("""
        INSERT INTO object_data (
            articulum_id, avito_item_id, title, price,
            seller_name, seller_id, seller_rating,
            published_at, description,
            location_name, location_coords,
            characteristics, views_total, raw_html
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        RETURNING id
    """,
        articulum_id, avito_item_id, card_data.title, card_data.price,
        seller_name, seller_id, seller_rating,
        published_at, card_data.description,
        location_name, location_coords,
        characteristics_json, card_data.views_total,
        raw_html if OBJECT_INCLUDE_HTML else None
    )

    logger.info(f"Сохранены данные объявления {avito_item_id} (id={record_id})")
    return record_id
