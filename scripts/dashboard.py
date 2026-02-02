"""Скрипт для мониторинга состояния системы парсинга Avito"""

import asyncio
import argparse
import sys
from typing import Dict, List

import asyncpg

# ============================================
# Конфигурация подключения к БД
# ============================================
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5432,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)

try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False
    print("Предупреждение: библиотека 'tabulate' не найдена.")
    print("Установите: pip install tabulate")
    print("Будет использован упрощенный вывод.\n")


async def get_tasks_stats(conn) -> Dict[str, Dict[str, int]]:
    """Получить статистику по задачам"""
    # Статистика catalog_tasks
    catalog_query = """
        SELECT status, COUNT(*) as count
        FROM catalog_tasks
        GROUP BY status
    """
    catalog_rows = await conn.fetch(catalog_query)
    catalog_stats = {row['status']: row['count'] for row in catalog_rows}

    # Статистика object_tasks
    object_query = """
        SELECT status, COUNT(*) as count
        FROM object_tasks
        GROUP BY status
    """
    object_rows = await conn.fetch(object_query)
    object_stats = {row['status']: row['count'] for row in object_rows}

    return {
        'catalog': catalog_stats,
        'object': object_stats
    }


async def get_proxies_stats(conn) -> Dict[str, int]:
    """Получить статистику по прокси"""
    query = """
        SELECT
            COUNT(*) FILTER (WHERE NOT is_blocked AND NOT is_in_use) as available,
            COUNT(*) FILTER (WHERE is_in_use) as in_use,
            COUNT(*) FILTER (WHERE is_blocked) as blocked,
            COUNT(*) as total
        FROM proxies
    """
    row = await conn.fetchrow(query)

    return {
        'available': row['available'],
        'in_use': row['in_use'],
        'blocked': row['blocked'],
        'total': row['total']
    }


async def get_articulums_stats(conn) -> Dict[str, int]:
    """Получить статистику по состояниям артикулов"""
    query = """
        SELECT state, COUNT(*) as count
        FROM articulums
        GROUP BY state
    """
    rows = await conn.fetch(query)
    stats = {row['state']: row['count'] for row in rows}

    # Общее количество
    total = sum(stats.values())
    stats['TOTAL'] = total

    return stats


async def get_validation_stats(conn) -> Dict[str, Dict[str, int]]:
    """Получить статистику по валидации"""
    query = """
        SELECT
            validation_type,
            passed,
            COUNT(*) as count
        FROM validation_results
        GROUP BY validation_type, passed
    """
    rows = await conn.fetch(query)

    stats = {}
    for row in rows:
        val_type = row['validation_type']
        passed = row['passed']
        count = row['count']

        if val_type not in stats:
            stats[val_type] = {'passed': 0, 'rejected': 0}

        if passed:
            stats[val_type]['passed'] = count
        else:
            stats[val_type]['rejected'] = count

    return stats


async def get_workers_stats(conn) -> Dict[str, int]:
    """Получить статистику по активным воркерам"""
    # Browser Workers из catalog_tasks
    catalog_workers_query = """
        SELECT COUNT(DISTINCT worker_id) as count
        FROM catalog_tasks
        WHERE status = 'processing' AND worker_id IS NOT NULL
    """
    catalog_workers = await conn.fetchval(catalog_workers_query)

    # Browser Workers из object_tasks
    object_workers_query = """
        SELECT COUNT(DISTINCT worker_id) as count
        FROM object_tasks
        WHERE status = 'processing' AND worker_id IS NOT NULL
    """
    object_workers = await conn.fetchval(object_workers_query)

    # Общее количество уникальных Browser Workers
    total_workers_query = """
        SELECT COUNT(DISTINCT worker_id) as count
        FROM (
            SELECT worker_id FROM catalog_tasks WHERE status = 'processing' AND worker_id IS NOT NULL
            UNION
            SELECT worker_id FROM object_tasks WHERE status = 'processing' AND worker_id IS NOT NULL
        ) t
    """
    total_workers = await conn.fetchval(total_workers_query)

    return {
        'catalog_workers': catalog_workers or 0,
        'object_workers': object_workers or 0,
        'total_browser_workers': total_workers or 0
    }


async def get_parsing_results_stats(conn) -> Dict[str, int]:
    """Получить статистику по результатам парсинга"""
    # Статистика catalog_listings
    catalog_listings_query = """
        SELECT COUNT(*) as count FROM catalog_listings
    """
    catalog_listings = await conn.fetchval(catalog_listings_query)

    # Статистика object_data
    object_data_query = """
        SELECT COUNT(*) as count FROM object_data
    """
    object_data = await conn.fetchval(object_data_query)

    # Уникальные объявления
    unique_objects_query = """
        SELECT COUNT(DISTINCT avito_item_id) as count FROM object_data
    """
    unique_objects = await conn.fetchval(unique_objects_query)

    # Статистика analytics_views
    analytics_query = """
        SELECT COUNT(*) as count FROM analytics_views
    """
    analytics = await conn.fetchval(analytics_query)

    return {
        'catalog_listings': catalog_listings or 0,
        'object_data_total': object_data or 0,
        'unique_objects': unique_objects or 0,
        'analytics_records': analytics or 0
    }


def display_dashboard_tabulate(
    tasks_stats: Dict,
    proxies_stats: Dict,
    articulums_stats: Dict,
    validation_stats: Dict,
    workers_stats: Dict,
    results_stats: Dict
):
    """Вывести дашборд с использованием tabulate"""
    print("\n" + "=" * 80)
    print("ДАШБОРД МОНИТОРИНГА СИСТЕМЫ ПАРСИНГА AVITO".center(80))
    print("=" * 80 + "\n")

    # Задачи парсинга
    print("📋 ЗАДАЧИ ПАРСИНГА")
    print("-" * 80)

    tasks_data = []
    statuses = ['pending', 'processing', 'completed', 'failed', 'invalid']

    for status in statuses:
        catalog_count = tasks_stats['catalog'].get(status, 0)
        object_count = tasks_stats['object'].get(status, 0)
        tasks_data.append([status.upper(), catalog_count, object_count])

    print(tabulate(tasks_data, headers=['Статус', 'Catalog Tasks', 'Object Tasks'], tablefmt='simple'))

    # Прокси
    print("\n🌐 ПРОКСИ")
    print("-" * 80)

    proxies_data = [
        ['Доступно', proxies_stats['available']],
        ['Используется', proxies_stats['in_use']],
        ['Заблокировано', proxies_stats['blocked']],
        ['ВСЕГО', proxies_stats['total']]
    ]

    print(tabulate(proxies_data, headers=['Статус', 'Количество'], tablefmt='simple'))

    # Артикулы
    print("\n📦 АРТИКУЛЫ")
    print("-" * 80)

    articulums_data = []
    states = ['NEW', 'CATALOG_PARSING', 'CATALOG_PARSED', 'VALIDATING', 'VALIDATED', 'OBJECT_PARSING', 'REJECTED_BY_MIN_COUNT']

    for state in states:
        count = articulums_stats.get(state, 0)
        if count > 0:
            articulums_data.append([state, count])

    if 'TOTAL' in articulums_stats:
        articulums_data.append(['ВСЕГО', articulums_stats['TOTAL']])

    print(tabulate(articulums_data, headers=['Состояние', 'Количество'], tablefmt='simple'))

    # Валидация
    if validation_stats:
        print("\n✅ ВАЛИДАЦИЯ")
        print("-" * 80)

        validation_data = []
        for val_type in ['price_filter', 'mechanical', 'ai']:
            if val_type in validation_stats:
                passed = validation_stats[val_type].get('passed', 0)
                rejected = validation_stats[val_type].get('rejected', 0)
                total = passed + rejected
                validation_data.append([val_type.upper(), passed, rejected, total])

        print(tabulate(validation_data, headers=['Тип валидации', 'Прошло', 'Отклонено', 'Всего'], tablefmt='simple'))

    # Воркеры
    print("\n⚙️  ВОРКЕРЫ")
    print("-" * 80)

    workers_data = [
        ['Catalog Workers', workers_stats['catalog_workers']],
        ['Object Workers', workers_stats['object_workers']],
        ['ВСЕГО Browser Workers', workers_stats['total_browser_workers']]
    ]

    print(tabulate(workers_data, headers=['Тип', 'Активных'], tablefmt='simple'))

    # Результаты парсинга
    print("\n📊 РЕЗУЛЬТАТЫ ПАРСИНГА")
    print("-" * 80)

    results_data = [
        ['Catalog Listings', results_stats['catalog_listings']],
        ['Object Data (замеры)', results_stats['object_data_total']],
        ['Уникальных объявлений', results_stats['unique_objects']],
        ['Analytics Records', results_stats['analytics_records']]
    ]

    print(tabulate(results_data, headers=['Таблица', 'Записей'], tablefmt='simple'))

    print("\n" + "=" * 80 + "\n")


def display_dashboard_simple(
    tasks_stats: Dict,
    proxies_stats: Dict,
    articulums_stats: Dict,
    validation_stats: Dict,
    workers_stats: Dict,
    results_stats: Dict
):
    """Вывести дашборд без tabulate (упрощенный формат)"""
    print("\n" + "=" * 80)
    print("ДАШБОРД МОНИТОРИНГА СИСТЕМЫ ПАРСИНГА AVITO".center(80))
    print("=" * 80 + "\n")

    # Задачи парсинга
    print("ЗАДАЧИ ПАРСИНГА:")
    statuses = ['pending', 'processing', 'completed', 'failed', 'invalid']
    for status in statuses:
        catalog_count = tasks_stats['catalog'].get(status, 0)
        object_count = tasks_stats['object'].get(status, 0)
        print(f"  {status.upper():15} | Catalog: {catalog_count:6} | Object: {object_count:6}")

    # Прокси
    print("\nПРОКСИ:")
    print(f"  Доступно:        {proxies_stats['available']}")
    print(f"  Используется:    {proxies_stats['in_use']}")
    print(f"  Заблокировано:   {proxies_stats['blocked']}")
    print(f"  ВСЕГО:           {proxies_stats['total']}")

    # Артикулы
    print("\nАРТИКУЛЫ:")
    states = ['NEW', 'CATALOG_PARSING', 'CATALOG_PARSED', 'VALIDATING', 'VALIDATED', 'OBJECT_PARSING', 'REJECTED_BY_MIN_COUNT']
    for state in states:
        count = articulums_stats.get(state, 0)
        if count > 0:
            print(f"  {state:30} {count}")
    if 'TOTAL' in articulums_stats:
        print(f"  {'ВСЕГО':30} {articulums_stats['TOTAL']}")

    # Валидация
    if validation_stats:
        print("\nВАЛИДАЦИЯ:")
        for val_type in ['price_filter', 'mechanical', 'ai']:
            if val_type in validation_stats:
                passed = validation_stats[val_type].get('passed', 0)
                rejected = validation_stats[val_type].get('rejected', 0)
                print(f"  {val_type.upper():15} | Прошло: {passed:6} | Отклонено: {rejected:6}")

    # Воркеры
    print("\nВОРКЕРЫ:")
    print(f"  Catalog Workers:         {workers_stats['catalog_workers']}")
    print(f"  Object Workers:          {workers_stats['object_workers']}")
    print(f"  ВСЕГО Browser Workers:   {workers_stats['total_browser_workers']}")

    # Результаты парсинга
    print("\nРЕЗУЛЬТАТЫ ПАРСИНГА:")
    print(f"  Catalog Listings:        {results_stats['catalog_listings']}")
    print(f"  Object Data (замеры):    {results_stats['object_data_total']}")
    print(f"  Уникальных объявлений:   {results_stats['unique_objects']}")
    print(f"  Analytics Records:       {results_stats['analytics_records']}")

    print("\n" + "=" * 80 + "\n")


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Дашборд мониторинга системы парсинга Avito')
    args = parser.parse_args()

    # Подключение к БД
    conn = await connect_db()

    try:
        # Сбор статистики
        tasks_stats = await get_tasks_stats(conn)
        proxies_stats = await get_proxies_stats(conn)
        articulums_stats = await get_articulums_stats(conn)
        validation_stats = await get_validation_stats(conn)
        workers_stats = await get_workers_stats(conn)
        results_stats = await get_parsing_results_stats(conn)

        # Вывод дашборда
        if TABULATE_AVAILABLE:
            display_dashboard_tabulate(
                tasks_stats, proxies_stats, articulums_stats,
                validation_stats, workers_stats, results_stats
            )
        else:
            display_dashboard_simple(
                tasks_stats, proxies_stats, articulums_stats,
                validation_stats, workers_stats, results_stats
            )

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
