"""Скрипт для загрузки и сброса артикулов

Загружает артикулы из файла в БД (если отсутствуют) и сбрасывает
историю обработки для уже существующих артикулов.

Использование:
    python reset_articulums.py                     # интерактивный режим
    python reset_articulums.py data/articulums.txt # с указанием файла
"""

import asyncio
import argparse
import sys
from pathlib import Path

import asyncpg

# ============================================
# Конфигурация подключения к БД
# ============================================
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)


async def load_articulums_from_file(filepath: str) -> tuple[list[str], int]:
    """Прочитать артикулы из файла с дедупликацией"""
    seen = set()
    articulums = []
    duplicates = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            articulum = line.strip()
            if not articulum:
                continue
            if articulum in seen:
                duplicates += 1
                continue
            seen.add(articulum)
            articulums.append(articulum)

    return articulums, duplicates


async def get_articulum_ids(conn, articulums: list[str]) -> dict[str, int]:
    """Получить ID артикулов из БД"""
    rows = await conn.fetch("""
        SELECT id, articulum FROM articulums
        WHERE articulum = ANY($1)
    """, articulums)
    return {row['articulum']: row['id'] for row in rows}


async def get_stats_before_reset(conn, articulum_ids: list[int]) -> dict:
    """Получить статистику данных до сброса"""
    stats = {}

    # Количество записей в каждой таблице
    stats['catalog_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM catalog_tasks WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['catalog_listings'] = await conn.fetchval("""
        SELECT COUNT(*) FROM catalog_listings WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['validation_results'] = await conn.fetchval("""
        SELECT COUNT(*) FROM validation_results WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['object_tasks'] = await conn.fetchval("""
        SELECT COUNT(*) FROM object_tasks WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['object_data'] = await conn.fetchval("""
        SELECT COUNT(*) FROM object_data WHERE articulum_id = ANY($1)
    """, articulum_ids)

    stats['analytics_report'] = await conn.fetchval("""
        SELECT COUNT(*) FROM analytics_articulum_report WHERE articulum_id = ANY($1)
    """, articulum_ids)

    return stats


async def insert_new_articulums(conn, articulums: list[str]) -> int:
    """Вставить новые артикулы в БД со state=NEW"""
    if not articulums:
        return 0
    await conn.executemany("""
        INSERT INTO articulums (articulum) VALUES ($1)
    """, [(a,) for a in articulums])
    return len(articulums)


async def reset_articulums(conn, articulum_ids: list[int]) -> dict:
    """Сбросить все данные для указанных артикулов"""
    deleted = {}

    async with conn.transaction():
        # 1. Удаляем analytics_articulum_report
        result = await conn.execute("""
            DELETE FROM analytics_articulum_report WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['analytics_report'] = int(result.split()[-1])

        # 2. Удаляем object_data
        result = await conn.execute("""
            DELETE FROM object_data WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['object_data'] = int(result.split()[-1])

        # 3. Удаляем object_tasks
        result = await conn.execute("""
            DELETE FROM object_tasks WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['object_tasks'] = int(result.split()[-1])

        # 4. Удаляем validation_results
        result = await conn.execute("""
            DELETE FROM validation_results WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['validation_results'] = int(result.split()[-1])

        # 5. Удаляем catalog_listings
        result = await conn.execute("""
            DELETE FROM catalog_listings WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['catalog_listings'] = int(result.split()[-1])

        # 6. Удаляем catalog_tasks
        result = await conn.execute("""
            DELETE FROM catalog_tasks WHERE articulum_id = ANY($1)
        """, articulum_ids)
        deleted['catalog_tasks'] = int(result.split()[-1])

        # 7. Сбрасываем состояние артикулов на NEW
        result = await conn.execute("""
            UPDATE articulums
            SET state = 'NEW',
                state_updated_at = NOW(),
                updated_at = NOW()
            WHERE id = ANY($1)
        """, articulum_ids)
        deleted['articulums_reset'] = int(result.split()[-1])

    return deleted


def interactive_mode() -> str:
    """Интерактивный выбор файла"""
    print("=" * 60)
    print("ЗАГРУЗКА И СБРОС АРТИКУЛОВ")
    print("=" * 60)
    print()
    print("Этот скрипт:")
    print("  1. Загрузит НОВЫЕ артикулы в БД (state=NEW)")
    print("  2. Сбросит историю для УЖЕ СУЩЕСТВУЮЩИХ артикулов")
    print()

    while True:
        filepath = input("Путь к файлу с артикулами: ").strip()
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
        description='Загрузка и сброс артикулов',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('file', nargs='?', help='Путь к .txt файлу с артикулами')
    args = parser.parse_args()

    # Определяем файл
    if args.file:
        filepath = args.file
        if not Path(filepath).exists():
            print(f"Ошибка: файл {filepath} не найден")
            sys.exit(1)
    else:
        filepath = interactive_mode()

    # Читаем артикулы
    print()
    print("Чтение файла...")
    articulums, duplicates = await load_articulums_from_file(filepath)
    print(f"Прочитано: {len(articulums)} уникальных артикулов" +
          (f" ({duplicates} дубликатов пропущено)" if duplicates else ""))

    if not articulums:
        print("Нет артикулов для обработки")
        sys.exit(0)

    # Подключаемся к БД
    print()
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # Получаем ID артикулов
        articulum_map = await get_articulum_ids(conn, articulums)
        found_count = len(articulum_map)
        not_found = sorted(set(articulums) - set(articulum_map.keys()))

        print(f"Найдено в БД: {found_count} артикулов")
        print(f"Новых (нет в БД): {len(not_found)} артикулов")

        # === Загрузка новых артикулов ===
        if not_found:
            print()
            print(f"Загрузка {len(not_found)} новых артикулов в БД...")
            inserted = await insert_new_articulums(conn, not_found)
            print(f"Загружено: {inserted} артикулов (state=NEW)")

        # === Сброс существующих артикулов ===
        if articulum_map:
            articulum_ids = list(articulum_map.values())

            # Получаем статистику
            print()
            print("Анализ данных существующих артикулов...")
            stats = await get_stats_before_reset(conn, articulum_ids)

            total_records = sum(stats.values())
            if total_records > 0:
                print()
                print("-" * 60)
                print("ДАННЫЕ ДЛЯ УДАЛЕНИЯ:")
                print("-" * 60)
                print(f"  catalog_tasks:            {stats['catalog_tasks']:,}")
                print(f"  catalog_listings:         {stats['catalog_listings']:,}")
                print(f"  validation_results:       {stats['validation_results']:,}")
                print(f"  object_tasks:             {stats['object_tasks']:,}")
                print(f"  object_data:              {stats['object_data']:,}")
                print(f"  analytics_report:         {stats['analytics_report']:,}")
                print(f"  артикулов для сброса:     {found_count}")
                print("-" * 60)

                # Подтверждение
                print()
                print("ВНИМАНИЕ: Сброс данных НЕОБРАТИМ!")
                confirm = input("Сбросить существующие артикулы? (yes/no): ").strip().lower()

                if confirm == 'yes':
                    print()
                    print("Удаление данных...")
                    deleted = await reset_articulums(conn, articulum_ids)

                    print(f"  catalog_tasks удалено:        {deleted['catalog_tasks']:,}")
                    print(f"  catalog_listings удалено:     {deleted['catalog_listings']:,}")
                    print(f"  validation_results удалено:   {deleted['validation_results']:,}")
                    print(f"  object_tasks удалено:         {deleted['object_tasks']:,}")
                    print(f"  object_data удалено:          {deleted['object_data']:,}")
                    print(f"  analytics_report удалено:     {deleted['analytics_report']:,}")
                    print(f"  артикулов сброшено на NEW:    {deleted['articulums_reset']:,}")
                else:
                    print("Сброс отменён (новые артикулы уже загружены)")
            else:
                print(f"Существующие {found_count} артикулов уже чистые (нет данных для удаления)")

        # Итого
        print()
        print("=" * 60)
        total_in_db = await conn.fetchval("SELECT COUNT(*) FROM articulums")
        print(f"Всего артикулов в БД: {total_in_db}")
        print("=" * 60)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
