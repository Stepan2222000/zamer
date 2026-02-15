#!/usr/bin/env python3
"""
Экспорт результатов валидации артикулов в JSON.
Читает артикулы из scripts/data/art.txt, получает данные из БД
и сохраняет детальный отчёт в scripts/data/validation_export.json.
"""

import asyncio
import json
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import asyncpg

# Пути
SCRIPT_DIR = Path(__file__).parent.resolve()
ART_FILE = SCRIPT_DIR / 'data' / 'art.txt'
OUTPUT_FILE = SCRIPT_DIR / 'data' / 'validation_export.json'

# БД
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder с поддержкой Decimal и datetime"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def load_articulums() -> list[str]:
    """Прочитать артикулы из файла"""
    if not ART_FILE.exists():
        print(f"Файл не найден: {ART_FILE}")
        sys.exit(1)

    articulums = []
    with open(ART_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                articulums.append(line)

    return articulums


async def main():
    articulums = load_articulums()
    print(f"Загружено {len(articulums)} артикулов из {ART_FILE.name}")
    for art in articulums:
        print(f"  - {art}")

    print(f"\nПодключение к БД ({DB_CONFIG['host']}:{DB_CONFIG['port']})...")
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # Получить состояния артикулов
        art_rows = await conn.fetch("""
            SELECT id, articulum, state, state_updated_at, created_at, updated_at
            FROM articulums
            WHERE articulum = ANY($1)
        """, articulums)

        art_map = {row['articulum']: dict(row) for row in art_rows}

        found = [a for a in articulums if a in art_map]
        not_found = [a for a in articulums if a not in art_map]

        if not_found:
            print(f"\nНе найдены в БД: {', '.join(not_found)}")
        print(f"Найдено в БД: {len(found)}/{len(articulums)}")

        if not found:
            print("Нет артикулов для экспорта.")
            await conn.close()
            return

        art_ids = [art_map[a]['id'] for a in found]

        # Получить catalog_listings для всех артикулов (без images_bytes — бинарные данные не нужны в JSON)
        listings_rows = await conn.fetch("""
            SELECT
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
                images_count,
                created_at
            FROM catalog_listings
            WHERE articulum_id = ANY($1)
            ORDER BY articulum_id, avito_item_id
        """, art_ids)

        # Получить validation_results с pivot
        vr_rows = await conn.fetch("""
            SELECT
                articulum_id,
                avito_item_id,
                validation_type,
                passed,
                rejection_reason,
                created_at
            FROM validation_results
            WHERE articulum_id = ANY($1)
            ORDER BY articulum_id, avito_item_id, validation_type
        """, art_ids)

        # Группировка validation_results по (articulum_id, avito_item_id)
        vr_map = {}
        for row in vr_rows:
            key = (row['articulum_id'], row['avito_item_id'])
            if key not in vr_map:
                vr_map[key] = {}
            vr_map[key][row['validation_type']] = {
                'passed': row['passed'],
                'rejection_reason': row['rejection_reason'],
                'validated_at': row['created_at'],
            }

        # Группировка listings по articulum_id
        listings_by_art = {}
        for row in listings_rows:
            art_id = row['articulum_id']
            if art_id not in listings_by_art:
                listings_by_art[art_id] = []
            listings_by_art[art_id].append(dict(row))

        # Сборка результата
        result = {
            'generated_at': datetime.now().isoformat(),
            'source': 'validation_results (БД)',
            'articulums_file': str(ART_FILE.name),
            'articulums': {}
        }

        for art_name in articulums:
            if art_name not in art_map:
                result['articulums'][art_name] = {
                    'status': 'NOT_FOUND_IN_DB',
                    'listings': [],
                    'summary': None,
                }
                continue

            art_info = art_map[art_name]
            art_id = art_info['id']
            listings = listings_by_art.get(art_id, [])

            # Счётчики для summary
            summary = {
                'total_listings': len(listings),
                'state': art_info['state'],
                'state_updated_at': art_info['state_updated_at'],
                'price_filter': {'passed': 0, 'rejected': 0, 'no_data': 0},
                'mechanical': {'passed': 0, 'rejected': 0, 'no_data': 0},
                'ai': {'passed': 0, 'rejected': 0, 'no_data': 0},
                'final_passed': 0,
                'final_rejected': 0,
            }

            listings_json = []
            for listing in listings:
                avito_id = listing['avito_item_id']
                vr_key = (art_id, avito_id)
                vr_data = vr_map.get(vr_key, {})

                # Результаты валидации по этапам
                validation = {}
                has_ai = 'ai' in vr_data

                for vtype in ['price_filter', 'mechanical', 'ai']:
                    if vtype in vr_data:
                        validation[vtype] = {
                            'passed': vr_data[vtype]['passed'],
                            'rejection_reason': vr_data[vtype]['rejection_reason'],
                        }
                        if vr_data[vtype]['passed']:
                            summary[vtype]['passed'] += 1
                        else:
                            summary[vtype]['rejected'] += 1
                    else:
                        validation[vtype] = None
                        summary[vtype]['no_data'] += 1

                # Определить final_passed
                pf = validation.get('price_filter')
                mech = validation.get('mechanical')
                ai = validation.get('ai')

                pf_passed = pf['passed'] if pf else None
                mech_passed = mech['passed'] if mech else None
                ai_passed = ai['passed'] if ai else None

                if has_ai:
                    final_passed = bool(pf_passed and mech_passed and ai_passed)
                elif pf is not None and mech is not None:
                    final_passed = bool(pf_passed and mech_passed)
                else:
                    final_passed = None  # нет данных валидации

                # Определить rejection_stage
                rejection_stage = None
                if final_passed is False:
                    if pf and not pf['passed']:
                        rejection_stage = 'price_filter'
                    elif mech and not mech['passed']:
                        rejection_stage = 'mechanical'
                    elif ai and not ai['passed']:
                        rejection_stage = 'ai'

                if final_passed is True:
                    summary['final_passed'] += 1
                elif final_passed is False:
                    summary['final_rejected'] += 1

                listing_entry = {
                    'avito_item_id': avito_id,
                    'title': listing['title'],
                    'price': listing['price'],
                    'snippet_text': listing['snippet_text'],
                    'seller_name': listing['seller_name'],
                    'seller_id': listing['seller_id'],
                    'seller_rating': listing['seller_rating'],
                    'seller_reviews': listing['seller_reviews'],
                    'images_count': listing['images_count'],
                    'images_urls': listing['images_urls'],
                    'listing_created_at': listing['created_at'],
                    'validation': validation,
                    'final_passed': final_passed,
                    'rejection_stage': rejection_stage,
                }
                listings_json.append(listing_entry)

            result['articulums'][art_name] = {
                'db_id': art_id,
                'state': art_info['state'],
                'summary': summary,
                'listings': listings_json,
            }

            # Вывод прогресса
            print(f"\n  {art_name}: state={art_info['state']}, "
                  f"listings={len(listings)}, "
                  f"passed={summary['final_passed']}, "
                  f"rejected={summary['final_rejected']}")

        # Сохранение
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(result, f, cls=DecimalEncoder, ensure_ascii=False, indent=2)

        print(f"\nРезультат сохранён: {OUTPUT_FILE}")
        print(f"Размер: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
