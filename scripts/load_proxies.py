"""Скрипт для загрузки прокси из .txt файла в БД"""

import asyncio
import argparse
import sys
from pathlib import Path

from database import connect_db


BATCH_SIZE = 1000  # Размер батча для вставки


def parse_proxy_line(line: str, line_num: int) -> dict | None:
    """
    Парсинг строки прокси в формате host:port:username:password
    Возвращает dict с данными или None если строка невалидна
    """
    parts = line.strip().split(':', maxsplit=3)

    # Проверка формата (минимум host:port)
    if len(parts) < 2:
        print(f"  Строка {line_num}: неверный формат (ожидается host:port:username:password)")
        return None

    host = parts[0].strip()
    port_str = parts[1].strip()
    username = parts[2].strip() if len(parts) > 2 else None
    password = parts[3].strip() if len(parts) > 3 else None

    # Валидация host
    if not host:
        print(f"  Строка {line_num}: пустой host")
        return None

    # Валидация port
    try:
        port = int(port_str)
        if port < 1 or port > 65535:
            print(f"  Строка {line_num}: порт {port} вне диапазона 1-65535")
            return None
    except ValueError:
        print(f"  Строка {line_num}: невалидный порт '{port_str}'")
        return None

    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }


async def load_proxies_from_file(filepath: str) -> tuple[list[dict], int]:
    """
    Прочитать прокси из файла
    Возвращает (список валидных прокси, количество невалидных)
    """
    proxies = []
    invalid_count = 0

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # Пропустить пустые строки
                if not line.strip():
                    continue

                # Парсинг и валидация
                proxy = parse_proxy_line(line, line_num)
                if proxy:
                    proxies.append(proxy)
                else:
                    invalid_count += 1

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return proxies, invalid_count


async def insert_proxies_batch(
    conn,
    proxies: list[dict],
    mode: str
) -> dict:
    """Вставить прокси батчами используя executemany для производительности"""
    total = len(proxies)
    total_inserted = 0

    # Подготовка SQL запроса в зависимости от режима
    if mode == 'add':
        # В режиме add игнорируем дубликаты
        sql = """
            INSERT INTO proxies (host, port, username, password, is_blocked, is_in_use)
            VALUES ($1, $2, $3, $4, FALSE, FALSE)
            ON CONFLICT (host, port, username) DO NOTHING
        """
    else:  # replace
        # В режиме replace просто вставляем
        sql = """
            INSERT INTO proxies (host, port, username, password, is_blocked, is_in_use)
            VALUES ($1, $2, $3, $4, FALSE, FALSE)
        """

    # Вставка батчами
    for i in range(0, total, BATCH_SIZE):
        batch = proxies[i:i + BATCH_SIZE]

        async with conn.transaction():
            # Подсчет строк до вставки (для режима add)
            if mode == 'add':
                count_before = await conn.fetchval('SELECT COUNT(*) FROM proxies')

            # Батчевая вставка через executemany
            await conn.executemany(
                sql,
                [(p['host'], p['port'], p['username'], p['password']) for p in batch]
            )

            # Подсчет вставленных строк
            if mode == 'add':
                count_after = await conn.fetchval('SELECT COUNT(*) FROM proxies')
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
    parser = argparse.ArgumentParser(description='Загрузка прокси в БД')
    parser.add_argument('--file', required=True, help='Путь к .txt файлу с прокси')
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
            print("Очистка таблицы proxies...")
            await conn.execute('TRUNCATE TABLE proxies CASCADE')
            print("Таблица очищена")
            print()

        # Загрузка прокси из файла
        print("Чтение и валидация файла...")
        proxies, invalid_count = await load_proxies_from_file(args.file)
        print(f"Прочитано валидных строк: {len(proxies)}")
        if invalid_count > 0:
            print(f"Невалидных строк пропущено: {invalid_count}")
        print()

        if not proxies:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Вставка в БД
        print("Загрузка в БД...")
        stats = await insert_proxies_batch(conn, proxies, args.mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Валидных строк:   {stats['total']}")
        print(f"  Загружено:        {stats['inserted']}")

        if args.mode == 'add' and stats['duplicates'] > 0:
            print(f"  Дубликаты:        {stats['duplicates']}")

        if invalid_count > 0:
            print(f"  Невалидных:       {invalid_count}")

        print("=" * 50)

    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
