#!/usr/bin/env python3
"""Проверка размеров изображений в S3"""
import asyncio
import sys
from collections import Counter
from pathlib import Path
from typing import Optional, Tuple

# Добавляем container в path для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / "container"))

import asyncpg
import boto3
import cv2
import numpy as np

DB_CONFIG = {
    "host": "81.30.105.134",
    "port": 5419,
    "database": "zamer_sys",
    "user": "admin",
    "password": "Password123",
}

S3_CONFIG = {
    "endpoint_url": "http://94.156.112.211:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "hPvCxU064y1nPAuHRPtHCow",
    "bucket": "photos",
}

SAMPLE_SIZE = 300  # Сколько изображений проверить


def get_image_size(img_bytes: bytes) -> Optional[Tuple[int, int]]:
    """Возвращает (width, height) или None при ошибке"""
    nparr = np.frombuffer(img_bytes, np.uint8)
    img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    h, w = img.shape[:2]
    return (w, h)


async def main():
    print("Подключение к БД...")
    conn = await asyncio.wait_for(asyncpg.connect(**DB_CONFIG), timeout=30)

    print("Получение s3_keys из catalog_listings...")
    rows = await conn.fetch("""
        SELECT s3_keys
        FROM catalog_listings
        WHERE s3_keys IS NOT NULL AND array_length(s3_keys, 1) > 0
        ORDER BY RANDOM()
        LIMIT $1
    """, SAMPLE_SIZE * 2)  # Берём с запасом, т.к. у одного объявления может быть несколько ключей

    await conn.close()

    # Собираем все ключи
    keys = []
    for row in rows:
        keys.extend(row["s3_keys"] or [])
    keys = keys[:SAMPLE_SIZE]

    print(f"Скачивание {len(keys)} изображений из S3...")

    from botocore.config import Config
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_CONFIG["endpoint_url"],
        aws_access_key_id=S3_CONFIG["aws_access_key_id"],
        aws_secret_access_key=S3_CONFIG["aws_secret_access_key"],
        config=Config(connect_timeout=10, read_timeout=30),
    )

    sizes: list = []
    errors = 0

    for i, key in enumerate(keys):
        try:
            resp = s3.get_object(Bucket=S3_CONFIG["bucket"], Key=key)
            data = resp["Body"].read()
            sz = get_image_size(data)
            if sz:
                sizes.append(sz)
            else:
                errors += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Ошибка {key}: {e}")

        if (i + 1) % 100 == 0:
            print(f"  Обработано {i + 1}/{len(keys)}...")

    if not sizes:
        print("Не удалось получить размеры ни одного изображения")
        return

    # Статистика
    counter = Counter(sizes)
    unique_sizes = sorted(counter.items(), key=lambda x: -x[1])

    print()
    print("=" * 50)
    print("РАЗМЕРЫ ИЗОБРАЖЕНИЙ В S3 (width x height)")
    print("=" * 50)
    print(f"Проверено: {len(sizes)} изображений")
    print(f"Ошибок: {errors}")
    print(f"Уникальных размеров: {len(unique_sizes)}")
    print()
    print("Топ размеров (сколько изображений каждого):")
    print("-" * 40)
    for (w, h), count in unique_sizes[:15]:
        pct = 100 * count / len(sizes)
        print(f"  {w}x{h}: {count} ({pct:.1f}%)")
    print()
    # Сводка по длинной стороне
    long_side = [max(w, h) for w, h in sizes]
    long_counter = Counter(long_side)
    print("По длинной стороне (топ):")
    for side, count in long_counter.most_common(10):
        pct = 100 * count / len(sizes)
        print(f"  {side}px: {count} ({pct:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())
