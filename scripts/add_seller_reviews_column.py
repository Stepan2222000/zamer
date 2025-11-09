#!/usr/bin/env python3
"""Миграция: Добавление столбца seller_reviews в таблицу catalog_listings"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# Добавляем путь к модулям container
sys.path.insert(0, str(Path(__file__).parent.parent / 'container'))

from config import DB_CONFIG


async def migrate():
    """Добавляет столбец seller_reviews в таблицу catalog_listings"""
    print("Подключение к базе данных...")
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # Проверяем существование столбца
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'catalog_listings'
                  AND column_name = 'seller_reviews'
            )
        """)

        if column_exists:
            print("✓ Столбец seller_reviews уже существует в catalog_listings")
        else:
            print("Добавление столбца seller_reviews в catalog_listings...")
            await conn.execute("""
                ALTER TABLE catalog_listings
                ADD COLUMN seller_reviews INTEGER
            """)
            print("✓ Столбец seller_reviews успешно добавлен")

        print("\nМиграция завершена успешно!")

    except Exception as e:
        print(f"✗ Ошибка при выполнении миграции: {e}", file=sys.stderr)
        raise
    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(migrate())
