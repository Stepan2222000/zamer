#!/usr/bin/env python3
"""
Миграция БД: добавление колонок для хранения изображений в catalog_listings.

Добавляет:
- images_urls (JSONB) - массив URL изображений
- images_bytes (BYTEA[]) - массив байтов изображений
- images_count (SMALLINT) - количество изображений

Скрипт идемпотентен - можно запускать повторно без ошибок.
"""

import asyncio
import asyncpg
import sys
import os

# Добавляем путь к container для импорта config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'container'))

from config import DB_CONFIG


async def migrate():
    """Выполняет миграцию базы данных."""
    print("Подключение к базе данных...")

    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        print("Проверка существующих колонок...")

        # Проверяем какие колонки уже существуют
        existing_columns = await conn.fetch("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'catalog_listings'
            AND column_name IN ('images_urls', 'images_bytes', 'images_count')
        """)

        existing = {row['column_name'] for row in existing_columns}
        print(f"Существующие колонки: {existing or 'нет'}")

        # Добавляем images_urls если не существует
        if 'images_urls' not in existing:
            print("Добавление колонки images_urls (JSONB)...")
            await conn.execute("""
                ALTER TABLE catalog_listings
                ADD COLUMN images_urls JSONB
            """)
            print("  ✓ images_urls добавлена")
        else:
            print("  - images_urls уже существует")

        # Добавляем images_bytes если не существует
        if 'images_bytes' not in existing:
            print("Добавление колонки images_bytes (BYTEA[])...")
            await conn.execute("""
                ALTER TABLE catalog_listings
                ADD COLUMN images_bytes BYTEA[]
            """)
            print("  ✓ images_bytes добавлена")
        else:
            print("  - images_bytes уже существует")

        # Добавляем images_count если не существует
        if 'images_count' not in existing:
            print("Добавление колонки images_count (SMALLINT)...")
            await conn.execute("""
                ALTER TABLE catalog_listings
                ADD COLUMN images_count SMALLINT
            """)
            print("  ✓ images_count добавлена")
        else:
            print("  - images_count уже существует")

        # Создаем индекс по images_count для быстрой фильтрации
        print("Создание индекса по images_count...")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_catalog_listings_images_count
            ON catalog_listings(images_count)
        """)
        print("  ✓ Индекс idx_catalog_listings_images_count создан")

        # Добавляем комментарии к колонкам
        print("Добавление комментариев к колонкам...")
        await conn.execute("""
            COMMENT ON COLUMN catalog_listings.images_urls IS
            'JSON-массив URL изображений объявления'
        """)
        await conn.execute("""
            COMMENT ON COLUMN catalog_listings.images_bytes IS
            'Массив байтов изображений (до 5 шт, 636w качество)'
        """)
        await conn.execute("""
            COMMENT ON COLUMN catalog_listings.images_count IS
            'Количество изображений (0-5), NULL если не запрашивалось'
        """)
        print("  ✓ Комментарии добавлены")

        print("\n✅ Миграция завершена успешно!")

        # Показываем структуру таблицы
        print("\nТекущая структура catalog_listings:")
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'catalog_listings'
            ORDER BY ordinal_position
        """)

        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  {col['column_name']}: {col['data_type']} {nullable}")

    finally:
        await conn.close()
        print("\nСоединение закрыто.")


if __name__ == '__main__':
    asyncio.run(migrate())
