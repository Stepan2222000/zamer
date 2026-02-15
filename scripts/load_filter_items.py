"""Скрипт для загрузки фильтра объявлений (avito_item_id) для повторного парсинга

Интерактивный режим: python load_filter_items.py
С аргументами:      python load_filter_items.py data/items.txt --mode replace
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

                # Валидация формата avito_item_id (должен быть числовым)
                if not item_id.isdigit():
                    print(f"ПРЕДУПРЕЖДЕНИЕ: Строка {line_num} содержит невалидный ID '{item_id}' (пропущено)")
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
            count_before = 0
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
                    lines.append(stripped)

    return lines, total


def interactive_mode() -> tuple[str, str]:
    """Интерактивный выбор параметров загрузки"""
    print("=" * 50)
    print("ЗАГРУЗКА ФИЛЬТРА ОБЪЯВЛЕНИЙ (для REPARSE_MODE)")
    print("=" * 50)
    print()
    print("avito_item_id будут добавлены в таблицу reparse_filter_items.")
    print("Система будет парсить только эти конкретные объявления.")
    print()

    # 1. Запросить путь к файлу
    while True:
        filepath = input("Путь к файлу с avito_item_id: ").strip()
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
        print("Превью (первые 5 строк):")
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
    print("  [2] Заменить все (replace) - ОЧИСТИТ фильтр!")
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
        description='Загрузка фильтра объявлений для повторного парсинга',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('file', nargs='?', help='Путь к .txt файлу с avito_item_id')
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
        # Создание таблицы если не существует
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reparse_filter_items (
                id SERIAL PRIMARY KEY,
                avito_item_id VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # В режиме replace очищаем таблицу
        if mode == 'replace':
            print("Очистка таблицы reparse_filter_items...")
            await conn.execute('TRUNCATE TABLE reparse_filter_items')
            print("Таблица очищена")
            print()

        # Загрузка item_id из файла
        print("Чтение файла...")
        all_items = await load_items_from_file(filepath)
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
        stats = await insert_items_batch(conn, valid_items, mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего строк в файле:       {len(all_items)}")
        print(f"  Валидных объявлений:       {len(valid_items)}")
        print(f"  Несуществующих в БД:       {len(invalid_items)}")
        print(f"  Загружено в фильтр:        {stats['inserted']}")

        if mode == 'add' and stats['duplicates'] > 0:
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
