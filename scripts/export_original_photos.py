#!/usr/bin/env python3
"""
Выгрузка первых фотографий оригинальных объявлений из БД.
Сохраняет JPEG файлы в папку scripts/data/original_photos/
"""

import asyncio
from pathlib import Path

import asyncpg

# Настройки
LIMIT = 3000
OUTPUT_DIR = Path(__file__).parent.resolve() / 'data' / 'original_photos'

DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Папка: {OUTPUT_DIR}")

    print(f"Подключение к БД ({DB_CONFIG['host']}:{DB_CONFIG['port']})...")
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # Шаг 1: получить avito_item_id оригинальных объявлений (без BYTEA — легковесно)
        print(f"Шаг 1: Запрос ID оригинальных объявлений...")
        id_rows = await conn.fetch("""
            SELECT cl.avito_item_id
            FROM catalog_listings cl
            JOIN articulums a ON a.id = cl.articulum_id
            WHERE a.state = 'VALIDATED'
              AND cl.images_count > 0
              AND EXISTS (
                  SELECT 1 FROM validation_results vr
                  WHERE vr.articulum_id = cl.articulum_id
                    AND vr.avito_item_id = cl.avito_item_id
                    AND vr.validation_type = 'ai'
                    AND vr.passed = true
              )
              AND NOT EXISTS (
                  SELECT 1 FROM validation_results vr
                  WHERE vr.articulum_id = cl.articulum_id
                    AND vr.avito_item_id = cl.avito_item_id
                    AND vr.passed = false
              )
            ORDER BY RANDOM()
            LIMIT $1
        """, LIMIT)

        avito_ids = [r['avito_item_id'] for r in id_rows]
        print(f"  Найдено ID: {len(avito_ids)}")

        # Шаг 2: загружаем фото батчами по 100
        BATCH_SIZE = 100
        saved = 0
        skipped = 0

        print(f"Шаг 2: Загрузка фото батчами по {BATCH_SIZE}...")
        for i in range(0, len(avito_ids), BATCH_SIZE):
            batch_ids = avito_ids[i:i + BATCH_SIZE]
            rows = await conn.fetch("""
                SELECT avito_item_id, images_bytes[1] as first_img
                FROM catalog_listings
                WHERE avito_item_id = ANY($1)
            """, batch_ids)

            for row in rows:
                img_data = row['first_img']
                if not img_data:
                    skipped += 1
                    continue

                if isinstance(img_data, memoryview):
                    img_data = bytes(img_data)

                filepath = OUTPUT_DIR / f"{row['avito_item_id']}.jpg"
                filepath.write_bytes(img_data)
                saved += 1

            print(f"  Батч {i // BATCH_SIZE + 1}/{(len(avito_ids) + BATCH_SIZE - 1) // BATCH_SIZE}: сохранено {saved}...")

        print(f"\nГотово! Сохранено: {saved}, пропущено: {skipped}")
        total_mb = sum(f.stat().st_size for f in OUTPUT_DIR.glob('*.jpg')) / 1024 / 1024
        print(f"Общий объём: {total_mb:.1f} MB")
        print(f"Папка: {OUTPUT_DIR}")

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
