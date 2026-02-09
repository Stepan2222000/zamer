"""Скрипт для загрузки прокси из .txt файла в БД

Формат файла: host:port:username:password (по одному на строку)

Интерактивный режим: python load_proxies.py
С аргументами:      python load_proxies.py --file data/proxies.txt --mode replace
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


# ============================================
# Функции загрузки данных
# ============================================

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
            count_before = 0
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


# ============================================
# Интерактивный режим
# ============================================

def get_file_preview(filepath: str, max_lines: int = 5) -> tuple[list[str], int]:
    """Получить превью файла (первые N строк и общее количество)"""
    lines = []
    total = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                total += 1
                if len(lines) < max_lines:
                    # Маскируем пароль для безопасности
                    parts = stripped.split(':', maxsplit=3)
                    if len(parts) >= 4:
                        masked = f"{parts[0]}:{parts[1]}:{parts[2]}:****"
                    elif len(parts) >= 3:
                        masked = f"{parts[0]}:{parts[1]}:{parts[2]}"
                    else:
                        masked = stripped
                    lines.append(masked)

    return lines, total


def interactive_mode() -> tuple[str, str]:
    """Интерактивный выбор параметров загрузки"""
    print("=" * 50)
    print("ЗАГРУЗКА ПРОКСИ")
    print("=" * 50)
    print()
    print("Формат файла: host:port:username:password")
    print()

    # 1. Запросить путь к файлу
    while True:
        filepath = input("Путь к файлу с прокси: ").strip()
        if not filepath:
            print("  Путь не может быть пустым")
            continue
        if not Path(filepath).exists():
            print(f"  Файл '{filepath}' не найден")
            continue
        break

    print()

    # 2. Показать превью
    try:
        preview, total_lines = get_file_preview(filepath)
        print(f"Найдено записей: {total_lines}")
        print()
        print("Превью (первые 5 строк, пароли скрыты):")
        for i, line in enumerate(preview, 1):
            print(f"  {i}. {line}")
        if total_lines > 5:
            print(f"  ... и ещё {total_lines - 5} записей")
        print()
    except Exception as e:
        print(f"Ошибка чтения файла: {e}")
        sys.exit(1)

    # 3. Выбор режима
    print("Выберите режим:")
    print("  [1] Добавить к существующим (add)")
    print("  [2] Заменить все (replace) - ОЧИСТИТ таблицу!")
    print()

    while True:
        choice = input("Ваш выбор (1/2): ").strip()
        if choice == '1':
            mode = 'add'
            break
        elif choice == '2':
            mode = 'replace'
            break
        else:
            print("  Введите 1 или 2")

    print()

    # 4. Подтверждение
    print("-" * 50)
    print(f"Файл:    {filepath}")
    print(f"Записей: {total_lines}")
    print(f"Режим:   {mode}")
    print("-" * 50)
    print()

    confirm = input("Начать загрузку? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Отменено")
        sys.exit(0)

    print()
    return filepath, mode


# ============================================
# Главная функция
# ============================================

async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description='Загрузка прокси в БД',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('--file', help='Путь к .txt файлу с прокси')
    parser.add_argument('--mode', choices=['add', 'replace'], default='add',
                        help='Режим: add (добавить) или replace (заменить)')
    args = parser.parse_args()

    # Определяем режим работы
    if args.file:
        # CLI режим с аргументами
        filepath = args.file
        mode = args.mode

        # Проверка существования файла
        if not Path(filepath).exists():
            print(f"Ошибка: файл {filepath} не найден")
            sys.exit(1)
    else:
        # Интерактивный режим
        filepath, mode = interactive_mode()

    print(f"Режим: {mode}")
    print(f"Файл: {filepath}")
    print()

    # Подключение к БД
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # В режиме replace очищаем таблицу
        if mode == 'replace':
            print("Очистка таблицы proxies...")
            await conn.execute('TRUNCATE TABLE proxies CASCADE')
            print("Таблица очищена")
            print()

        # Загрузка прокси из файла
        print("Чтение и валидация файла...")
        proxies, invalid_count = await load_proxies_from_file(filepath)
        print(f"Прочитано валидных строк: {len(proxies)}")
        if invalid_count > 0:
            print(f"Невалидных строк пропущено: {invalid_count}")
        print()

        if not proxies:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Вставка в БД
        print("Загрузка в БД...")
        stats = await insert_proxies_batch(conn, proxies, mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Валидных строк:   {stats['total']}")
        print(f"  Загружено:        {stats['inserted']}")

        if mode == 'add' and stats['duplicates'] > 0:
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
