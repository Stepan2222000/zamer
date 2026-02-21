#!/usr/bin/env python3
"""
Выгрузка первых фотографий оригинальных объявлений из БД.
Сохраняет JPEG файлы в папку scripts/data/original_photos/
"""

import asyncio
import sys
from pathlib import Path

import asyncpg

# Добавляем container в sys.path для импорта s3_client
sys.path.insert(0, str(Path(__file__).parent.parent / 'container'))

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
        # Шаг 1: получить avito_item_id оригинальных объявлений (легковесный запрос)
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

        from s3_client import get_s3_async_client
        s3 = get_s3_async_client()

        print(f"Шаг 2: Загрузка фото батчами по {BATCH_SIZE}...")
        for i in range(0, len(avito_ids), BATCH_SIZE):
            batch_ids = avito_ids[i:i + BATCH_SIZE]
            rows = await conn.fetch("""
                SELECT avito_item_id, s3_keys[1] as first_s3_key
                FROM catalog_listings
                WHERE avito_item_id = ANY($1)
            """, batch_ids)

            # Собираем ключи для батчевого скачивания
            keys_map = {}  # s3_key -> avito_item_id
            for row in rows:
                s3_key = row['first_s3_key']
                if s3_key:
                    keys_map[s3_key] = row['avito_item_id']

            downloaded = await s3.download_many(list(keys_map.keys())) if keys_map else {}

            for s3_key, avito_id in keys_map.items():
                if s3_key not in downloaded:
                    skipped += 1
                    continue

                filepath = OUTPUT_DIR / f"{avito_id}.jpg"
                filepath.write_bytes(downloaded[s3_key])
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
