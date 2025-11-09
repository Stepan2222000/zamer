"""Скрипт для загрузки фильтра объявлений (avito_item_id) для повторного парсинга"""

import asyncio
import argparse
import sys
from pathlib import Path

from database import connect_db


BATCH_SIZE = 1000  # Размер батча для вставки


async def load_items_from_file(filepath: str) -> list[str]:
    """Прочитать avito_item_id из файла"""
    items = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                item_id = line.strip()

                # Пропустить пустые строки
                if not item_id:
                    continue

                items.append(item_id)

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return items


async def validate_items_exist(conn, items: list[str]) -> tuple[list[str], list[str]]:
    """Проверить существование avito_item_id в object_data"""
    print("Проверка существования объявлений в БД...")

    # Запрашиваем все существующие avito_item_id из object_data
    existing_ids = set(
        await conn.fetch(
            "SELECT DISTINCT avito_item_id FROM object_data WHERE avito_item_id = ANY($1)",
            items
        )
    )
    existing_ids = {row['avito_item_id'] for row in existing_ids}

    valid_items = []
    invalid_items = []

    for item_id in items:
        if item_id in existing_ids:
            valid_items.append(item_id)
        else:
            invalid_items.append(item_id)

    return valid_items, invalid_items


async def insert_items_batch(
    conn,
    items: list[str],
    mode: str
) -> dict:
    """Вставить avito_item_id батчами"""
    total = len(items)
    total_inserted = 0

    # Подготовка SQL запроса в зависимости от режима
    if mode == 'add':
        # В режиме add игнорируем дубликаты
        sql = """
            INSERT INTO reparse_filter_items (avito_item_id)
            VALUES ($1)
            ON CONFLICT (avito_item_id) DO NOTHING
        """
    else:  # replace
        # В режиме replace просто вставляем
        sql = """
            INSERT INTO reparse_filter_items (avito_item_id)
            VALUES ($1)
        """

    # Вставка батчами
    for i in range(0, total, BATCH_SIZE):
        batch = items[i:i + BATCH_SIZE]

        async with conn.transaction():
            # Подсчет строк до вставки (для режима add)
            if mode == 'add':
                count_before = await conn.fetchval('SELECT COUNT(*) FROM reparse_filter_items')

            # Батчевая вставка через executemany
            await conn.executemany(sql, [(item,) for item in batch])

            # Подсчет вставленных строк
            if mode == 'add':
                count_after = await conn.fetchval('SELECT COUNT(*) FROM reparse_filter_items')
                batch_inserted = count_after - count_before
            else:
                batch_inserted = len(batch)

            total_inserted += batch_inserted

        print(f"Обработано {min(i + BATCH_SIZE, total)}/{total}...")

    duplicates = total - total_inserted if mode == 'add' else 0

    return {
        'total': total,
        'inserted': total_inserted,
        'duplicates': duplicates
    }


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Загрузка фильтра объявлений для повторного парсинга')
    parser.add_argument('file', help='Путь к .txt файлу с avito_item_id')
    parser.add_argument('--mode', choices=['add', 'replace'], default='add',
                        help='Режим: add (добавить) или replace (заменить)')
    args = parser.parse_args()

    # Проверка существования файла
    if not Path(args.file).exists():
        print(f"Ошибка: файл {args.file} не найден")
        sys.exit(1)

    print(f"Режим: {args.mode}")
    print(f"Файл: {args.file}")
    print()

    # Подключение к БД
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # Создание таблицы если не существует
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reparse_filter_items (
                id SERIAL PRIMARY KEY,
                avito_item_id VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # В режиме replace очищаем таблицу
        if args.mode == 'replace':
            print("Очистка таблицы reparse_filter_items...")
            await conn.execute('TRUNCATE TABLE reparse_filter_items')
            print("Таблица очищена")
            print()

        # Загрузка item_id из файла
        print("Чтение файла...")
        all_items = await load_items_from_file(args.file)
        print(f"Прочитано строк: {len(all_items)}")
        print()

        if not all_items:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Проверка существования в БД
        valid_items, invalid_items = await validate_items_exist(conn, all_items)

        # Вывод предупреждений для несуществующих ID
        if invalid_items:
            print()
            print("ПРЕДУПРЕЖДЕНИЕ: Следующие avito_item_id НЕ найдены в object_data и будут пропущены:")
            for item_id in invalid_items[:10]:  # Показываем первые 10
                print(f"  - {item_id}")
            if len(invalid_items) > 10:
                print(f"  ... и еще {len(invalid_items) - 10} объявлений")
            print()

        if not valid_items:
            print("Нет валидных объявлений для загрузки")
            sys.exit(0)

        # Вставка в БД
        print(f"Загрузка {len(valid_items)} валидных объявлений в БД...")
        stats = await insert_items_batch(conn, valid_items, args.mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего строк в файле:       {len(all_items)}")
        print(f"  Валидных объявлений:       {len(valid_items)}")
        print(f"  Несуществующих в БД:       {len(invalid_items)}")
        print(f"  Загружено в фильтр:        {stats['inserted']}")

        if args.mode == 'add' and stats['duplicates'] > 0:
            print(f"  Дубликаты (пропущено):     {stats['duplicates']}")

        print("=" * 50)

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
