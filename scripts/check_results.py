#!/usr/bin/env python3
"""Проверка результатов и S3"""
import asyncio
import asyncpg

DB = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def main():
    conn = await asyncpg.connect(**DB)
    try:
        r1 = await conn.fetchrow('SELECT COUNT(*) as c FROM catalog_listings')
        r2 = await conn.fetchrow(
            "SELECT COUNT(*) as c FROM articulums WHERE state = 'VALIDATED'"
        )
        r3 = await conn.fetchrow(
            "SELECT COUNT(*) as c FROM articulums WHERE state = 'OBJECT_PARSING'"
        )
        r4 = await conn.fetchrow("""
            SELECT COUNT(*) as total,
                   COUNT(*) FILTER (WHERE s3_keys IS NOT NULL AND array_length(s3_keys, 1) > 0) as with_s3
            FROM catalog_listings
        """)
        r5 = await conn.fetch("""
            SELECT avito_item_id, s3_keys, images_count
            FROM catalog_listings
            WHERE s3_keys IS NOT NULL AND array_length(s3_keys, 1) > 0
            LIMIT 3
        """)
        print('=== РЕЗУЛЬТАТЫ ===')
        print('catalog_listings:', r1['c'])
        print('articulums VALIDATED:', r2['c'])
        print('articulums OBJECT_PARSING:', r3['c'])
        print()
        print('=== S3 (s3_keys) ===')
        print('Всего объявлений:', r4['total'])
        print('С s3_keys в БД:', r4['with_s3'])
        if r5:
            print()
            print('Примеры:')
            for row in r5:
                keys = row['s3_keys'][:2] if row['s3_keys'] else []
                print(' ', row['avito_item_id'], '->', keys, 'count:', row['images_count'])
        else:
            print('Нет записей с s3_keys')
    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
