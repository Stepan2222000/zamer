"""Скрипт для загрузки артикулов из .txt файла в БД"""

import asyncio
import argparse
import sys
from pathlib import Path

from database import connect_db


async def load_articulums_from_file(filepath: str, min_length: int = 0) -> tuple[list[str], int]:
    """Прочитать артикулы из файла с фильтром по длине"""
    articulums = []
    skipped = 0

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                articulum = line.strip()

                # Пропустить пустые строки
                if not articulum:
                    continue

                # Фильтр по минимальной длине
                if min_length > 0 and len(articulum) < min_length:
                    skipped += 1
                    continue

                articulums.append(articulum)

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return articulums, skipped


async def insert_articulums_batch(
    conn,
    articulums: list[str],
    mode: str
) -> dict:
    """Вставить все артикулы одним запросом через unnest()"""
    total = len(articulums)

    # Подготовка SQL запроса в зависимости от режима
    if mode == 'add':
        # В режиме add игнорируем дубликаты и используем RETURNING для подсчета
        sql = """
            INSERT INTO articulums (articulum, state)
            SELECT unnest($1::text[]), 'NEW'
            ON CONFLICT (articulum) DO NOTHING
            RETURNING id
        """
    else:  # replace
        # В режиме replace просто вставляем все
        sql = """
            INSERT INTO articulums (articulum, state)
            SELECT unnest($1::text[]), 'NEW'
            RETURNING id
        """

    # Одна транзакция для всех артикулов
    async with conn.transaction():
        result = await conn.fetch(sql, articulums)
        total_inserted = len(result)

    duplicates = total - total_inserted if mode == 'add' else 0

    print(f"Обработано {total}/{total}...")

    return {
        'total': total,
        'inserted': total_inserted,
        'duplicates': duplicates
    }


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Загрузка артикулов в БД')
    parser.add_argument('--file', required=True, help='Путь к .txt файлу с артикулами')
    parser.add_argument('--mode', choices=['add', 'replace'], default='add',
                        help='Режим: add (добавить) или replace (заменить)')
    parser.add_argument('--min-length', type=int, default=0,
                        help='Минимальная длина артикула (по умолчанию 0 — без фильтра)')
    args = parser.parse_args()

    # Проверка существования файла
    if not Path(args.file).exists():
        print(f"Ошибка: файл {args.file} не найден")
        sys.exit(1)

    print(f"Режим: {args.mode}")
    print(f"Файл: {args.file}")
    if args.min_length > 0:
        print(f"Мин. длина: {args.min_length}")
    print()

    # Подключение к БД
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # В режиме replace очищаем таблицу
        if args.mode == 'replace':
            print("Очистка таблицы articulums...")
            await conn.execute('TRUNCATE TABLE articulums CASCADE')
            print("Таблица очищена")
            print()

        # Загрузка артикулов из файла
        print("Чтение файла...")
        articulums, skipped = await load_articulums_from_file(args.file, args.min_length)
        print(f"Прочитано строк: {len(articulums)}")
        if skipped > 0:
            print(f"Пропущено (короче {args.min_length} символов): {skipped}")
        print()

        if not articulums:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Вставка в БД
        print("Загрузка в БД...")
        stats = await insert_articulums_batch(conn, articulums, args.mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего строк:      {stats['total']}")
        print(f"  Загружено:        {stats['inserted']}")

        if args.mode == 'add' and stats['duplicates'] > 0:
            print(f"  Дубликаты:        {stats['duplicates']}")

        if skipped > 0:
            print(f"  Пропущено:        {skipped}")

        print("=" * 50)

    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
