"""Скрипт для загрузки артикулов из .txt файла в БД"""

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

                articulums.append(articulum)

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return articulums


async def insert_articulums_batch(
    conn,
    articulums: list[str],
    mode: str
) -> dict:
    """Вставить артикулы батчами"""
    total = len(articulums)
    inserted = 0
    duplicates = 0

    # Подготовка SQL запроса в зависимости от режима
    if mode == 'add':
        # В режиме add игнорируем дубликаты
        sql = """
            INSERT INTO articulums (articulum, status)
            VALUES ($1, 'NEW')
            ON CONFLICT (articulum) DO NOTHING
        """
    else:  # replace
        # В режиме replace просто вставляем
        sql = """
            INSERT INTO articulums (articulum, status)
            VALUES ($1, 'NEW')
        """

    # Вставка батчами
    for i in range(0, total, BATCH_SIZE):
        batch = articulums[i:i + BATCH_SIZE]

        async with conn.transaction():
            for articulum in batch:
                try:
                    await conn.execute(sql, articulum)
                    inserted += 1
                except Exception as e:
                    # В режиме add дубликаты не вызовут ошибку благодаря ON CONFLICT
                    if mode == 'add':
                        duplicates += 1
                    else:
                        print(f"Ошибка при вставке '{articulum}': {e}")
                        raise

        print(f"Обработано {min(i + BATCH_SIZE, total)}/{total}...")

    return {
        'total': total,
        'inserted': inserted,
        'duplicates': duplicates
    }


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Загрузка артикулов в БД')
    parser.add_argument('--file', required=True, help='Путь к .txt файлу с артикулами')
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
        # В режиме replace очищаем таблицу
        if args.mode == 'replace':
            print("Очистка таблицы articulums...")
            await conn.execute('TRUNCATE TABLE articulums CASCADE')
            print("Таблица очищена")
            print()

        # Загрузка артикулов из файла
        print("Чтение файла...")
        articulums = await load_articulums_from_file(args.file)
        print(f"Прочитано строк: {len(articulums)}")
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

        print("=" * 50)

    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
