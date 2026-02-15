"""Скрипт для загрузки фильтра артикулов для повторного парсинга

Интерактивный режим: python load_filter_articulums.py
С аргументами:      python load_filter_articulums.py data/filter.txt --mode replace
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


async def load_articulums_from_file(filepath: str) -> tuple[list[str], int]:
    """Прочитать артикулы из файла с дедупликацией

    Returns:
        tuple: (уникальные артикулы, количество дубликатов в файле)
    """
    seen = set()
    articulums = []
    duplicates_in_file = 0

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

                # Дедупликация внутри файла
                if articulum in seen:
                    duplicates_in_file += 1
                    continue

                seen.add(articulum)
                articulums.append(articulum)

    except FileNotFoundError:
        print(f"Ошибка: файл {filepath} не найден")
        sys.exit(1)
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        sys.exit(1)

    return articulums, duplicates_in_file


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
            count_before = 0
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
    print("ЗАГРУЗКА ФИЛЬТРА АРТИКУЛОВ (для REPARSE_MODE)")
    print("=" * 50)
    print()
    print("Артикулы будут добавлены в таблицу reparse_filter_articulums.")
    print("Система будет парсить только объявления с этими артикулами.")
    print()

    # 1. Запросить путь к файлу
    while True:
        filepath = input("Путь к файлу с артикулами: ").strip()
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
        description='Загрузка фильтра артикулов для повторного парсинга',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('file', nargs='?', help='Путь к .txt файлу с артикулами')
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
            CREATE TABLE IF NOT EXISTS reparse_filter_articulums (
                id SERIAL PRIMARY KEY,
                articulum VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # В режиме replace очищаем таблицу
        if mode == 'replace':
            print("Очистка таблицы reparse_filter_articulums...")
            await conn.execute('TRUNCATE TABLE reparse_filter_articulums')
            print("Таблица очищена")
            print()

        # Загрузка артикулов из файла
        print("Чтение файла...")
        all_articulums, duplicates_in_file = await load_articulums_from_file(filepath)
        total_lines = len(all_articulums) + duplicates_in_file
        print(f"Прочитано строк: {total_lines}")
        if duplicates_in_file > 0:
            print(f"Дубликатов в файле: {duplicates_in_file} (удалены)")
        print(f"Уникальных артикулов: {len(all_articulums)}")
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
        stats = await insert_articulums_batch(conn, valid_articulums, mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего строк в файле:       {total_lines}")
        if duplicates_in_file > 0:
            print(f"  Дубликатов в файле:        {duplicates_in_file}")
        print(f"  Уникальных артикулов:      {len(all_articulums)}")
        print(f"  Валидных (есть в БД):      {len(valid_articulums)}")
        print(f"  Несуществующих в БД:       {len(invalid_articulums)}")
        print(f"  Загружено в фильтр:        {stats['inserted']}")

        if mode == 'add' and stats['duplicates'] > 0:
            print(f"  Уже в фильтре:             {stats['duplicates']}")

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
