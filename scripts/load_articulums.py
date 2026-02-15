"""Скрипт для загрузки артикулов из .txt файла в БД

Интерактивный режим: python load_articulums.py
С аргументами:      python load_articulums.py --file data/articulums.txt --mode replace
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

async def load_articulums_from_file(filepath: str, min_length: int = 0) -> tuple[list[str], int, int]:
    """Прочитать артикулы из файла с фильтром по длине и удалением дубликатов

    Returns:
        tuple: (уникальные артикулы, пропущено по длине, дубликатов в файле)
    """
    seen = set()
    articulums = []
    skipped = 0
    duplicates_in_file = 0

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

                # Проверка на дубликаты в файле
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

    return articulums, skipped, duplicates_in_file


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


def interactive_mode() -> tuple[str, str, int]:
    """Интерактивный выбор параметров загрузки"""
    print("=" * 50)
    print("ЗАГРУЗКА АРТИКУЛОВ")
    print("=" * 50)
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

    # 4. Минимальная длина (опционально)
    min_length = 0
    min_len_input = input("Минимальная длина артикула (Enter = без фильтра): ").strip()
    if min_len_input.isdigit():
        min_length = int(min_len_input)

    print()

    # 5. Подтверждение
    print("-" * 50)
    print(f"Файл:      {filepath}")
    print(f"Записей:   {total_lines}")
    print(f"Режим:     {mode}")
    if min_length > 0:
        print(f"Мин.длина: {min_length}")
    print("-" * 50)
    print()

    confirm = input("Начать загрузку? (yes/no): ").strip().lower()
    if confirm != 'yes':
        print("Отменено")
        sys.exit(0)

    print()
    return filepath, mode, min_length


# ============================================
# Главная функция
# ============================================

async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description='Загрузка артикулов в БД',
        epilog='Запустите без аргументов для интерактивного режима'
    )
    parser.add_argument('--file', help='Путь к .txt файлу с артикулами')
    parser.add_argument('--mode', choices=['add', 'replace'], default='add',
                        help='Режим: add (добавить) или replace (заменить)')
    parser.add_argument('--min-length', type=int, default=0,
                        help='Минимальная длина артикула (по умолчанию 0 — без фильтра)')
    args = parser.parse_args()

    # Определяем режим работы
    if args.file:
        # CLI режим с аргументами
        filepath = args.file
        mode = args.mode
        min_length = args.min_length

        # Проверка существования файла
        if not Path(filepath).exists():
            print(f"Ошибка: файл {filepath} не найден")
            sys.exit(1)
    else:
        # Интерактивный режим
        filepath, mode, min_length = interactive_mode()

    print(f"Режим: {mode}")
    print(f"Файл: {filepath}")
    if min_length > 0:
        print(f"Мин. длина: {min_length}")
    print()

    # Подключение к БД
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # В режиме replace очищаем таблицу
        if mode == 'replace':
            print("Очистка таблицы articulums...")
            await conn.execute('TRUNCATE TABLE articulums CASCADE')
            print("Таблица очищена")
            print()

        # Загрузка артикулов из файла
        print("Чтение файла...")
        articulums, skipped, duplicates_in_file = await load_articulums_from_file(filepath, min_length)
        print(f"Уникальных артикулов: {len(articulums)}")
        if duplicates_in_file > 0:
            print(f"Дубликатов в файле: {duplicates_in_file}")
        if skipped > 0:
            print(f"Пропущено (короче {min_length} символов): {skipped}")
        print()

        if not articulums:
            print("Нет данных для загрузки")
            sys.exit(0)

        # Вставка в БД
        print("Загрузка в БД...")
        stats = await insert_articulums_batch(conn, articulums, mode)

        # Вывод статистики
        print()
        print("=" * 50)
        print("Статистика:")
        print(f"  Всего уникальных: {stats['total']}")
        print(f"  Загружено в БД:   {stats['inserted']}")

        if duplicates_in_file > 0:
            print(f"  Дубликатов в файле: {duplicates_in_file}")

        if mode == 'add' and stats['duplicates'] > 0:
            print(f"  Уже в БД:         {stats['duplicates']}")

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
