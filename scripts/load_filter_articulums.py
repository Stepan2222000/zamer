"""Скрипт для загрузки фильтра артикулов для повторного парсинга"""

import asyncio
import argparse
import sys
from pathlib import Path

from database import connect_db


BATCH_SIZE = 1000  # Размер батча для вставки


async def load_articulums_from_file(filepath: str) -> list[str]:
    """Прочитать артикулы из файла"""
    articulums = []

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                articulum = line.strip()

                # Пропустить пустые строки
                if not articulum:
                    continue

                # Валидация формата артикула (непустая строка, базовые символы)
                if len(articulum) > 255:
                    print(f"ПРЕДУПРЕЖДЕНИЕ: Строка {line_num} содержит слишком длинный артикул (>255 символов, пропущено)")
                    continue

                articulums.append(articulum)

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return articulums


async def validate_articulums_exist(conn, articulums: list[str]) -> tuple[list[str], list[str]]:
    """Проверить существование артикулов в таблице articulums"""
    print("Проверка существования артикулов в БД...")

    # Запрашиваем все существующие артикулы
    existing_articulums = set(
        await conn.fetch(
            "SELECT articulum FROM articulums WHERE articulum = ANY($1)",
            articulums
        )
    )
    existing_articulums = {row['articulum'] for row in existing_articulums}

    valid_articulums = []
    invalid_articulums = []

    for articulum in articulums:
        if articulum in existing_articulums:
            valid_articulums.append(articulum)
        else:
            invalid_articulums.append(articulum)

    return valid_articulums, invalid_articulums


async def insert_articulums_batch(
    conn,
    articulums: list[str],
    mode: str
) -> dict:
    """Вставить артикулы батчами"""
    total = len(articulums)
    total_inserted = 0

    # Подготовка SQL запроса в зависимости от режима
    if mode == 'add':
        # В режиме add игнорируем дубликаты
        sql = """
            INSERT INTO reparse_filter_articulums (articulum)
            VALUES ($1)
            ON CONFLICT (articulum) DO NOTHING
        """
    else:  # replace
        # В режиме replace просто вставляем
        sql = """
            INSERT INTO reparse_filter_articulums (articulum)
            VALUES ($1)
        """

    # Вставка батчами
    for i in range(0, total, BATCH_SIZE):
        batch = articulums[i:i + BATCH_SIZE]

        async with conn.transaction():
            # Подсчет строк до вставки (для режима add)
            if mode == 'add':
                count_before = await conn.fetchval('SELECT COUNT(*) FROM reparse_filter_articulums')

            # Батчевая вставка через executemany
            await conn.executemany(sql, [(art,) for art in batch])

            # Подсчет вставленных строк
            if mode == 'add':
                count_after = await conn.fetchval('SELECT COUNT(*) FROM reparse_filter_articulums')
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
    parser = argparse.ArgumentParser(description='Загрузка фильтра артикулов для повторного парсинга')
    parser.add_argument('file', help='Путь к .txt файлу с артикулами')
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
            CREATE TABLE IF NOT EXISTS reparse_filter_articulums (
                id SERIAL PRIMARY KEY,
                articulum VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # В режиме replace очищаем таблицу
        if args.mode == 'replace':
            print("Очистка таблицы reparse_filter_articulums...")
            await conn.execute('TRUNCATE TABLE reparse_filter_articulums')
            print("Таблица очищена")
            print()

        # Загрузка артикулов из файла
        print("Чтение файла...")
        all_articulums = await load_articulums_from_file(args.file)
        print(f"Прочитано строк: {len(all_articulums)}")
        print()

        if not all_articulums:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Проверка существования в БД
        valid_articulums, invalid_articulums = await validate_articulums_exist(conn, all_articulums)

        # Вывод предупреждений для несуществующих артикулов
        if invalid_articulums:
            print()
            print("ПРЕДУПРЕЖДЕНИЕ: Следующие артикулы НЕ найдены в таблице articulums и будут пропущены:")
            for articulum in invalid_articulums[:10]:  # Показываем первые 10
                print(f"  - {articulum}")
            if len(invalid_articulums) > 10:
                print(f"  ... и еще {len(invalid_articulums) - 10} артикулов")
            print()

        if not valid_articulums:
            print("Нет валидных артикулов для загрузки")
            sys.exit(0)

        # Вставка в БД
        print(f"Загрузка {len(valid_articulums)} валидных артикулов в БД...")
        stats = await insert_articulums_batch(conn, valid_articulums, args.mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего строк в файле:       {len(all_articulums)}")
        print(f"  Валидных артикулов:        {len(valid_articulums)}")
        print(f"  Несуществующих в БД:       {len(invalid_articulums)}")
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
