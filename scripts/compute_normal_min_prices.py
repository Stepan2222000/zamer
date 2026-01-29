#!/usr/bin/env python3
"""Расчёт «нормальной» минимальной цены для артикулов.

Алгоритм устойчив к выбросам благодаря сравнению медианы, нижнего квартиля
и доли очень дешёвых объявлений. Если аномально низкая цена встречается
реже порогового значения, она отбрасывается и в качестве минимальной
используется ближайшая адекватная цена.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

import asyncpg
import pandas as pd

# Параметры фильтрации
MIN_LISTINGS = 5  # нужно минимум объявлений, иначе медиана
ALPHA_LOW = 0.4   # порог «очень дешёвых» объявлений относительно медианы
BETA_FRACTION = 0.25  # минимальная доля дешёвых, при которой сырой минимум считается валидным
GAMMA_Q1 = 0.6   # множитель для нижнего квартиля, задаёт нижнюю границу фильтра
DELTA_MEDIAN = 0.25  # страхующий порог от медианы


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Расчёт нормальной минимальной цены для артикулов'
    )
    parser.add_argument(
        '--excel',
        default='validated_missing_qwen_brands.xlsx',
        help='Excel-файл с колонками articulum и min_price (по умолчанию validated_missing_qwen_brands.xlsx)',
    )
    parser.add_argument(
        '--threshold',
        type=float,
        default=1000.0,
        help='Максимальная исходная min_price для включения в анализ (default: 1000)',
    )
    parser.add_argument(
        '--output',
        default='normal_min_prices.csv',
        help='Путь к CSV с результатом (default: normal_min_prices.csv)',
    )
    return parser.parse_args()


def load_articulums(excel_path: Path, threshold: float) -> List[str]:
    if not excel_path.exists():
        raise FileNotFoundError(f'Excel-файл не найден: {excel_path}')
    df = pd.read_excel(excel_path)
    if 'articulum' not in df.columns or 'min_price' not in df.columns:
        raise ValueError('Ожидаются колонки articulum и min_price в Excel-файле')
    if threshold is None:
        subset = df['articulum']
    else:
        subset = df[df['min_price'] < threshold]['articulum']
    articulums = subset.dropna().astype(str).tolist()
    if not articulums:
        raise ValueError('Не найдено артикулов, удовлетворяющих условию threshold')
    return articulums


async def fetch_prices(articulums: Iterable[str]) -> Dict[str, List[float]]:
    sys.path.insert(0, os.path.join(Path(__file__).resolve().parent.parent, 'container'))
    from config import DB_CONFIG  # noqa: WPS433

    query = """
        SELECT a.articulum, cl.price
        FROM catalog_listings cl
        JOIN articulums a ON cl.articulum_id = a.id
        WHERE a.articulum = ANY($1::text[])
          AND cl.price IS NOT NULL
        ORDER BY a.articulum
    """

    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        rows = await conn.fetch(query, list(articulums))
    finally:
        await conn.close()

    prices_map: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        prices_map[row['articulum']].append(float(row['price']))
    return prices_map


def percentile_25(values: List[float]) -> float:
    if len(values) < 2:
        return values[0]
    return statistics.quantiles(values, n=4, method='inclusive')[0]


def compute_normal_min(prices: List[float]) -> Dict[str, float]:
    prices_sorted = sorted(prices)
    n = len(prices_sorted)
    min_raw = prices_sorted[0]
    median_price = statistics.median(prices_sorted)
    q1_price = percentile_25(prices_sorted)
    avg_price = sum(prices_sorted) / n
    max_price = prices_sorted[-1]

    stats = {
        'samples': n,
        'min_raw': min_raw,
        'median_price': median_price,
        'q1_price': q1_price,
        'avg_price': avg_price,
        'max_price': max_price,
    }

    if n < MIN_LISTINGS:
        stats['normal_min'] = median_price
        stats['decision'] = 'insufficient_samples'
        stats['fraction_low'] = 1.0
        stats['low_threshold'] = median_price
        stats['low_count'] = n
        return stats

    very_low_threshold = median_price * ALPHA_LOW
    low_count = sum(1 for price in prices_sorted if price <= very_low_threshold)
    fraction_low = low_count / n
    stats['fraction_low'] = fraction_low
    stats['low_threshold'] = very_low_threshold
    stats['low_count'] = low_count

    if fraction_low >= BETA_FRACTION:
        stats['normal_min'] = min_raw
        stats['decision'] = 'stable_low_market'
        return stats

    cutoff = max(q1_price * GAMMA_Q1, median_price * DELTA_MEDIAN)
    filtered = [price for price in prices_sorted if price >= cutoff]
    if filtered:
        stats['normal_min'] = filtered[0]
        stats['decision'] = 'filtered_outliers'
    else:
        stats['normal_min'] = median_price
        stats['decision'] = 'fallback_median'
    stats['cutoff'] = cutoff
    return stats


async def main():
    args = parse_args()
    excel_path = Path(args.excel)
    articulums = load_articulums(excel_path, args.threshold)
    prices_map = await fetch_prices(articulums)

    rows = []
    for art in articulums:
        prices = prices_map.get(art, [])
        if not prices:
            rows.append(
                {
                    'articulum': art,
                    'samples': 0,
                    'normal_min': None,
                    'decision': 'no_prices',
                }
            )
            continue
        stats = compute_normal_min(prices)
        stats['articulum'] = art
        rows.append(stats)

    df = pd.DataFrame(rows)
    df.sort_values('normal_min', inplace=True, na_position='last')
    df.to_csv(args.output, index=False)
    print(f'Сохранено {len(df)} строк в {args.output}')


if __name__ == '__main__':
    asyncio.run(main())
