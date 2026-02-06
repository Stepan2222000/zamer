"""Скрипт для полного удаления артикулов и всех связанных данных из БД

Использование:
    python delete_articulums.py                  # интерактивный режим
    python delete_articulums.py articulums.txt   # с указанием файла

Формат файла: один артикул на строку
"""

import asyncio
import argparse
import sys
from pathlib import Path
from datetime import datetime

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

# Активные состояния артикулов (парсинг в процессе)
ACTIVE_STATES = ['CATALOG_PARSING', 'VALIDATING', 'OBJECT_PARSING']


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)


def load_articulums_from_file(filepath: str) -> tuple[list[str], int, int]:
    """Прочитать артикулы из файла с дедупликацией

    Returns:
        (уникальные артикулы, кол-во дубликатов, всего строк)
    """
    seen = set()
    articulums = []
    duplicates = 0
    total_lines = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            articulum = line.strip()
            if not articulum:
                continue
            total_lines += 1
            if articulum in seen:
                duplicates += 1
                continue
            seen.add(articulum)
            articulums.append(articulum)

    return articulums, duplicates, total_lines


async def check_processing_tasks(conn, articulum_ids: list[int]) -> dict:
    """Проверить активные задачи для КОНКРЕТНЫХ артикулов"""
    active = {}

    # Проверка catalog_tasks в processing для удаляемых артикулов
    active['catalog_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM catalog_tasks
        WHERE status = 'processing' AND articulum_id = ANY($1)
    """, articulum_ids)

    # Проверка object_tasks в processing для удаляемых артикулов
    active['object_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM object_tasks
        WHERE status = 'processing' AND articulum_id = ANY($1)
    """, articulum_ids)

    return active


async def get_articulum_data(conn, articulums: list[str]) -> list[dict]:
    """Получить данные артикулов из БД (ID, название, состояние)"""
    rows = await conn.fetch("""
        SELECT id, articulum, state FROM articulums
        WHERE articulum = ANY($1)
    """, articulums)
    return [dict(row) for row in rows]


async def get_avito_item_ids(conn, articulum_ids: list[int]) -> list[str]:
    """Получить все avito_item_id для указанных артикулов"""
    rows = await conn.fetch("""
        SELECT DISTINCT avito_item_id FROM catalog_listings
        WHERE articulum_id = ANY($1)
    """, articulum_ids)
    return [row['avito_item_id'] for row in rows]


async def get_deletion_stats(conn, articulum_ids: list[int], avito_item_ids: list[str]) -> dict:
    """Подсчитать количество записей для удаления в каждой таблице"""
    stats = {}

    # Таблицы с CASCADE (удалятся автоматически при удалении артикула)
    stats['catalog_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM catalog_tasks WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['catalog_listings'] = await conn.fetchval("""
        SELECT COUNT(*) FROM catalog_listings WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['object_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM object_tasks WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['object_data'] = await conn.fetchval("""
        SELECT COUNT(*) FROM object_data WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['validation_results'] = await conn.fetchval("""
        SELECT COUNT(*) FROM validation_results WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['analytics_articulum_report'] = await conn.fetchval("""
        SELECT COUNT(*) FROM analytics_articulum_report WHERE articulum_id = ANY($1)
    """, articulum_ids)

    # Независимые таблицы (нужно удалять вручную)
    # reparse_filter_articulums - по названию артикула
    articulum_names = await conn.fetch("""
        SELECT articulum FROM articulums WHERE id = ANY($1)
    """, articulum_ids)
    articulum_names_list = [row['articulum'] for row in articulum_names]

    stats['reparse_filter_articulums'] = await conn.fetchval("""
        SELECT COUNT(*) FROM reparse_filter_articulums WHERE articulum = ANY($1)
    """, articulum_names_list) if articulum_names_list else 0

    # reparse_filter_items и analytics_views - по avito_item_id
    if avito_item_ids:
        stats['reparse_filter_items'] = await conn.fetchval("""
            SELECT COUNT(*) FROM reparse_filter_items WHERE avito_item_id = ANY($1)
        """, avito_item_ids)

        stats['analytics_views'] = await conn.fetchval("""
            SELECT COUNT(*) FROM analytics_views WHERE avito_item_id = ANY($1)
        """, avito_item_ids)
    else:
        stats['reparse_filter_items'] = 0
        stats['analytics_views'] = 0

    return stats


async def delete_articulums(conn, articulum_ids: list[int], avito_item_ids: list[str]) -> dict:
    """Удалить артикулы и все связанные данные"""
    deleted = {}

    async with conn.transaction():
        # 1. Сначала удаляем из независимых таблиц (без CASCADE)

        # Получаем названия артикулов для reparse_filter_articulums
        articulum_names = await conn.fetch("""
            SELECT articulum FROM articulums WHERE id = ANY($1)
        """, articulum_ids)
        articulum_names_list = [row['articulum'] for row in articulum_names]

        # reparse_filter_articulums
        if articulum_names_list:
            result = await conn.execute("""
                DELETE FROM reparse_filter_articulums WHERE articulum = ANY($1)
            """, articulum_names_list)
            deleted['reparse_filter_articulums'] = int(result.split()[-1])
        else:
            deleted['reparse_filter_articulums'] = 0

        # reparse_filter_items
        if avito_item_ids:
            result = await conn.execute("""
                DELETE FROM reparse_filter_items WHERE avito_item_id = ANY($1)
            """, avito_item_ids)
            deleted['reparse_filter_items'] = int(result.split()[-1])
        else:
            deleted['reparse_filter_items'] = 0

        # analytics_views
        if avito_item_ids:
            result = await conn.execute("""
                DELETE FROM analytics_views WHERE avito_item_id = ANY($1)
            """, avito_item_ids)
            deleted['analytics_views'] = int(result.split()[-1])
        else:
            deleted['analytics_views'] = 0

        # 2. Удаляем артикулы (CASCADE удалит всё связанное автоматически)
        result = await conn.execute("""
            DELETE FROM articulums WHERE id = ANY($1)
        """, articulum_ids)
        deleted['articulums'] = int(result.split()[-1])

    return deleted


def print_header(title: str):
    """Напечатать заголовок"""
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_section(title: str):
    """Напечатать секцию"""
    print()
    print("-" * 70)
    print(f"  {title}")
    print("-" * 70)


def interactive_file_select() -> str:
    """Интерактивный выбор файла"""
    print()
    print("=" * 70)
    print("  УДАЛЕНИЕ АРТИКУЛОВ ИЗ БАЗЫ ДАННЫХ")
    print("=" * 70)
    print()
    print("Этот скрипт ПОЛНОСТЬЮ УДАЛИТ артикулы и все связанные данные:")
    print("  - catalog_tasks (задачи парсинга каталогов)")
    print("  - catalog_listings (объявления из каталогов)")
    print("  - object_tasks (задачи парсинга объявлений)")
    print("  - object_data (данные объявлений)")
    print("  - validation_results (результаты валидации)")
    print("  - analytics_articulum_report (отчёты)")
    print("  - reparse_filter_articulums (фильтры)")
    print("  - reparse_filter_items (фильтры)")
    print("  - analytics_views (аналитика)")
    print()

    while True:
        try:
            filepath = input("Путь к файлу с артикулами: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n\nОперация отменена")
            sys.exit(0)

        if not filepath:
            print("  Путь не может быть пустым")
            continue
        if not Path(filepath).exists():
            print(f"  Файл '{filepath}' не найден")
            continue
        break

    return filepath


async def main():
    parser = argparse.ArgumentParser(
        description='Полное удаление артикулов и всех связанных данных',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('file', nargs='?', help='Путь к .txt файлу с артикулами для удаления')
    args = parser.parse_args()

    # Определяем файл: интерактивно или из аргумента
    if args.file:
        filepath = args.file
        if not Path(filepath).exists():
            print(f"Ошибка: файл '{filepath}' не найден")
            sys.exit(1)
        print_header("УДАЛЕНИЕ АРТИКУЛОВ ИЗ БАЗЫ ДАННЫХ")
    else:
        filepath = interactive_file_select()

    # Читаем файл
    print("\nЧтение файла...")
    articulums, duplicates, total_lines = load_articulums_from_file(filepath)

    print(f"  Всего строк в файле: {total_lines}")
    print(f"  Уникальных артикулов: {len(articulums)}")
    if duplicates:
        print(f"  Дубликатов пропущено: {duplicates}")

    if not articulums:
        print("\nНет артикулов для удаления")
        sys.exit(0)

    # Подключаемся к БД
    print("\nПодключение к БД...")
    try:
        conn = await connect_db()
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

    try:
        # Ищем артикулы в БД
        print("Поиск артикулов в БД...")
        articulum_data = await get_articulum_data(conn, articulums)

        articulum_map = {row['articulum']: row['id'] for row in articulum_data}
        articulum_states = {row['articulum']: row['state'] for row in articulum_data}

        found_count = len(articulum_map)
        not_found = set(articulums) - set(articulum_map.keys())

        print(f"  Найдено в БД: {found_count} из {len(articulums)}")

        if not_found:
            print(f"\n  Не найдено в БД ({len(not_found)} шт):")
            for art in sorted(not_found)[:5]:
                print(f"    • {art}")
            if len(not_found) > 5:
                print(f"    ... и ещё {len(not_found) - 5}")

        if not articulum_map:
            print("\nНет артикулов для удаления в БД")
            sys.exit(0)

        articulum_ids = list(articulum_map.values())

        # Проверяем активные задачи для ЭТИХ артикулов
        print("\nПроверка активных задач...")
        active = await check_processing_tasks(conn, articulum_ids)

        has_active = any(v > 0 for v in active.values())
        if has_active:
            print()
            print("!" * 70)
            print("  ВНИМАНИЕ: ОБНАРУЖЕНЫ АКТИВНЫЕ ЗАДАЧИ!")
            print("!" * 70)
            print()
            if active['catalog_tasks'] > 0:
                print(f"  • catalog_tasks в обработке: {active['catalog_tasks']}")
            if active['object_tasks'] > 0:
                print(f"  • object_tasks в обработке: {active['object_tasks']}")
            print()
            print("  Рекомендуется остановить воркеры перед удалением.")
        else:
            print("  ✓ Активных задач нет")

        # Показываем артикулы в активных состояниях (как предупреждение)
        active_articulums = [(art, state) for art, state in articulum_states.items()
                            if state in ACTIVE_STATES]
        if active_articulums:
            print(f"\n  ⚠ Артикулы в активных состояниях ({len(active_articulums)} шт):")
            for art, state in active_articulums[:5]:
                print(f"    • {art} [{state}]")
            if len(active_articulums) > 5:
                print(f"    ... и ещё {len(active_articulums) - 5}")

        # Получаем avito_item_id для независимых таблиц
        print("\nСбор связанных данных...")
        avito_item_ids = await get_avito_item_ids(conn, articulum_ids)
        print(f"  Найдено {len(avito_item_ids):,} уникальных avito_item_id")

        # Подсчитываем статистику удаления
        print("Подсчёт записей для удаления...")
        stats = await get_deletion_stats(conn, articulum_ids, avito_item_ids)

        # Выводим отчёт
        print_section("ДАННЫЕ ДЛЯ УДАЛЕНИЯ")

        print("\n  Таблицы с CASCADE удалением (автоматически):")
        print(f"    catalog_tasks:              {stats['catalog_tasks']:>10,}")
        print(f"    catalog_listings:           {stats['catalog_listings']:>10,}")
        print(f"    object_tasks:               {stats['object_tasks']:>10,}")
        print(f"    object_data:                {stats['object_data']:>10,}")
        print(f"    validation_results:         {stats['validation_results']:>10,}")
        print(f"    analytics_articulum_report: {stats['analytics_articulum_report']:>10,}")

        print("\n  Независимые таблицы (ручное удаление):")
        print(f"    reparse_filter_articulums:  {stats['reparse_filter_articulums']:>10,}")
        print(f"    reparse_filter_items:       {stats['reparse_filter_items']:>10,}")
        print(f"    analytics_views:            {stats['analytics_views']:>10,}")

        total_records = sum(stats.values())
        print(f"\n  АРТИКУЛОВ:                    {found_count:>10,}")
        print(f"  ЗАПИСЕЙ ВСЕГО:                {total_records:>10,}")

        # Выводим список артикулов для удаления
        print_section("АРТИКУЛЫ ДЛЯ УДАЛЕНИЯ")
        for art in sorted(articulum_map.keys())[:20]:
            art_id = articulum_map[art]
            state = articulum_states.get(art, '?')
            print(f"  [{art_id:>4}] {art:<30} [{state}]")
        if len(articulum_map) > 20:
            print(f"  ... и ещё {len(articulum_map) - 20}")

    except Exception as e:
        print(f"\nОшибка при сборе данных: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()

    # Подтверждение (ПОСЛЕ закрытия соединения, чтобы не было таймаута)
    print()
    print("!" * 70)
    print("  ВНИМАНИЕ: Это действие НЕОБРАТИМО!")
    print("  Все данные будут удалены безвозвратно.")
    print("!" * 70)
    print()

    try:
        confirm = input("Введите 'да' или 'yes' для подтверждения удаления: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print("\n\nОперация отменена")
        sys.exit(0)

    if confirm not in ('да', 'yes', 'y'):
        print("\nОперация отменена")
        sys.exit(0)

    # Новое подключение для удаления
    print()
    print("Подключение к БД для удаления...")
    try:
        conn = await connect_db()
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        sys.exit(1)

    try:
        print("Удаление данных...")
        start_time = datetime.now()

        deleted = await delete_articulums(conn, articulum_ids, avito_item_ids)

        elapsed = (datetime.now() - start_time).total_seconds()

        # Финальный отчёт
        print_header("УДАЛЕНИЕ ЗАВЕРШЕНО")

        print(f"\n  Удалено артикулов:                 {deleted['articulums']:>10,}")
        print(f"  reparse_filter_articulums:         {deleted['reparse_filter_articulums']:>10,}")
        print(f"  reparse_filter_items:              {deleted['reparse_filter_items']:>10,}")
        print(f"  analytics_views:                   {deleted['analytics_views']:>10,}")
        print(f"\n  (Остальные таблицы очищены каскадно)")
        print(f"\n  Время выполнения: {elapsed:.2f} сек")
        print()

    except Exception as e:
        print(f"\nОшибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
