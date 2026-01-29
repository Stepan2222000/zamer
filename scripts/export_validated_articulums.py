#!/usr/bin/env python3
"""
Скрипт для валидации артикулов (без AI) и выгрузки в файл.
Оптимизированная версия - один запрос для всех данных.
"""

import asyncio
import asyncpg
import statistics
from collections import defaultdict
from datetime import datetime, timedelta

# Конфигурация
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123'
}

MIN_PRICE = 8000.0
MIN_VALIDATED_ITEMS = 7

VALIDATION_STOPWORDS = [
    'копия', 'реплика', 'подделка', 'фейк', 'fake',
    'replica', 'copy', 'имитация', 'аналог',
    'не оригинал', 'неоригинал', 'китай', 'china',
    'подобие', 'как оригинал', 'копи', 'копию', 'дубликат', 'дубль',
    'б/у', 'бу', 'б у', 'использованный', 'использованная',
    'ношенный', 'ношеный', 'поношенный',
    'second hand', 'second-hand', 'secondhand', 'used',
    'worn', 'pre-owned', 'preowned', 'pre owned',
    'после носки', 'поноска', 'с дефектами', 'дефект',
    'потертости', 'потёртости', 'царапины', 'следы носки',
    'требует ремонта', 'на запчасти', 'не новый', 'не новая',
]


def validate_articulum_listings(listings):
    """Валидация объявлений одного артикула"""
    if not listings:
        return 0
    
    # Этап 1: Price filter
    after_price = [l for l in listings if l['price'] is not None and float(l['price']) >= MIN_PRICE]
    if len(after_price) < MIN_VALIDATED_ITEMS:
        return 0
    
    # Подготовка IQR статистики
    prices = [float(l['price']) for l in after_price]
    
    if len(prices) >= 4:
        prices_sorted = sorted(prices)
        q1, q3 = statistics.quantiles(prices_sorted, n=4)[0], statistics.quantiles(prices_sorted, n=4)[2]
        iqr = q3 - q1
        lower_bound = q1 - 1.0 * iqr
        upper_bound = q3 + 1.0 * iqr
        prices_clean = [p for p in prices_sorted if lower_bound <= p <= upper_bound]
        
        if prices_clean:
            median_clean = statistics.median(prices_clean)
            prices_clean_final = [p for p in prices_clean if p <= median_clean * 2.5] or prices_clean
            prices_sorted_desc = sorted(prices_clean_final, reverse=True)
            top40_count = max(1, len(prices_sorted_desc) * 2 // 5)
            median_top40 = statistics.median(prices_sorted_desc[:top40_count])
            outlier_upper = upper_bound
        else:
            median_top40 = statistics.median(prices_sorted)
            outlier_upper = median_top40 * 3
    elif prices:
        median_top40 = statistics.median(prices)
        outlier_upper = median_top40 * 3
    else:
        median_top40 = outlier_upper = None
    
    # Этап 2: Mechanical validation
    passed = 0
    for l in after_price:
        text = f"{(l.get('title') or '').lower()} {(l.get('snippet_text') or '').lower()} {(l.get('seller_name') or '').lower()}"
        price = float(l['price']) if l.get('price') else None
        
        # Стоп-слова
        if any(sw in text for sw in VALIDATION_STOPWORDS):
            continue
        
        # Ценовая валидация
        if median_top40 and price:
            if price < median_top40 * 0.5 or (outlier_upper and price > outlier_upper):
                continue
        
        passed += 1
    
    return passed


async def main():
    print("Подключение к БД...")
    conn = await asyncpg.connect(**DB_CONFIG)
    
    cutoff_date = datetime.now() - timedelta(days=15)
    
    # Один большой запрос - все объявления для артикулов за 15 дней
    print("Загрузка данных (это займёт пару минут)...")
    
    rows = await conn.fetch('''
        SELECT 
            a.articulum,
            cl.price,
            cl.title,
            cl.snippet_text,
            cl.seller_name
        FROM catalog_listings cl
        JOIN articulums a ON a.id = cl.articulum_id
        WHERE a.updated_at >= $1
          AND a.state IN ('CATALOG_PARSED', 'VALIDATED', 'REJECTED_BY_MIN_COUNT')
    ''', cutoff_date)
    
    await conn.close()
    print(f"Загружено {len(rows):,} объявлений")
    
    # Группируем по артикулам
    print("Группировка по артикулам...")
    by_articulum = defaultdict(list)
    for row in rows:
        by_articulum[row['articulum']].append(dict(row))
    
    print(f"Уникальных артикулов: {len(by_articulum):,}")
    
    # Валидация
    print("Валидация...")
    results = []
    for i, (articulum, listings) in enumerate(by_articulum.items()):
        if (i + 1) % 10000 == 0:
            print(f"  {i + 1}/{len(by_articulum)}...")
        
        passed_count = validate_articulum_listings(listings)
        if passed_count >= MIN_VALIDATED_ITEMS:
            results.append((articulum, passed_count))
    
    # Сортировка по убыванию
    results.sort(key=lambda x: x[1], reverse=True)
    
    # Сохранение
    output_file = 'validated_articulums_15days.txt'
    with open(output_file, 'w') as f:
        for articulum, _ in results:
            f.write(f"{articulum}\n")
    
    print(f"\n=== РЕЗУЛЬТАТ ===")
    print(f"Артикулов прошло валидацию (>={MIN_VALIDATED_ITEMS} объявлений): {len(results):,}")
    print(f"Файл: {output_file}")
    
    if results:
        counts = [c for _, c in results]
        print(f"\nСтатистика:")
        print(f"  Макс: {max(counts)}, Мин: {min(counts)}, Среднее: {sum(counts)/len(counts):.1f}")
        print(f"\nТоп-10:")
        for art, cnt in results[:10]:
            print(f"  {art}: {cnt}")


if __name__ == '__main__':
    asyncio.run(main())
