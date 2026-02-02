"""Скрипт для очистки таблиц БД"""

import asyncio
import argparse
import sys

import asyncpg

# ============================================
# Конфигурация подключения к БД
# ============================================
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5432,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)


# Таблицы доступные для очистки (БЕЗ таблиц результатов: object_data, analytics_views)
AVAILABLE_TABLES = [
    ('articulums', 'Артикулы'),
    ('proxies', 'Прокси'),
    ('catalog_tasks', 'Задачи парсинга каталогов'),
    ('object_tasks', 'Задачи парсинга объявлений'),
    ('catalog_listings', 'Объявления из каталогов'),
    ('validation_results', 'Результаты валидации'),
    ('reparse_filter_items', 'Фильтр объявлений для повторного парсинга'),
    ('reparse_filter_articulums', 'Фильтр артикулов для повторного парсинга'),
]


async def clear_all_tables(conn) -> None:
    """Очистить все таблицы (кроме object_data и analytics_views)"""
    print("\nОчистка всех служебных таблиц (БЕЗ таблиц результатов)...\n")

    for table_name, description in AVAILABLE_TABLES:
        print(f"Очистка {description} ({table_name})...")
        try:
            await conn.execute(f'TRUNCATE TABLE {table_name} CASCADE')
        except Exception as e:
            print(f"  Предупреждение: не удалось очистить {table_name}: {e}")

    print("\nВсе служебные таблицы очищены!")
    print("ПРИМЕЧАНИЕ: Таблицы результатов (object_data, analytics_views) НЕ очищены.")


async def clear_selected_tables(conn) -> None:
    """Очистить выбранные таблицы через интерактивное меню"""
    print()
    print("Выберите таблицы для очистки:")
    print()

    for idx, (table_name, description) in enumerate(AVAILABLE_TABLES, 1):
        print(f"  [{idx}] {description} ({table_name})")

    print(f"  [0] Отмена")
    print()

    choices_input = input("Введите номера таблиц через запятую (например: 1,2,5): ").strip()

    if not choices_input or choices_input == '0':
        print("Операция отменена")
        sys.exit(0)

    # Парсинг выбора
    try:
        choices = [int(c.strip()) for c in choices_input.split(',')]
    except ValueError:
        print("Ошибка: неверный формат ввода")
        sys.exit(1)

    # Валидация выбора
    if any(c < 1 or c > len(AVAILABLE_TABLES) for c in choices):
        print(f"Ошибка: номера должны быть от 1 до {len(AVAILABLE_TABLES)}")
        sys.exit(1)

    # Очистка выбранных таблиц
    print()
    for choice in choices:
        table_name, description = AVAILABLE_TABLES[choice - 1]
        print(f"Очистка {description} ({table_name})...")
        try:
            await conn.execute(f'TRUNCATE TABLE {table_name} CASCADE')
            print(f"  ✓ Таблица {table_name} очищена")
        except Exception as e:
            print(f"  ✗ Ошибка при очистке {table_name}: {e}")

    print("\nОперация завершена!")


def confirm_action(mode: str, triple_confirm: bool = False) -> bool:
    """Запросить подтверждение действия"""
    if mode == 'all':
        message = "ВСЕ СЛУЖЕБНЫЕ ТАБЛИЦЫ БУДУТ ОЧИЩЕНЫ! Продолжить?"
    else:
        message = "ВЫБРАННЫЕ ТАБЛИЦЫ БУДУТ ОЧИЩЕНЫ! Продолжить?"

    print()
    print("=" * 70)
    print(f"ВНИМАНИЕ: {message}")
    if mode == 'all':
        print("ПРИМЕЧАНИЕ: Таблицы результатов (object_data, analytics_views) НЕ будут затронуты.")
    print("=" * 70)

    # Для режима 'all' используем тройное подтверждение
    if triple_confirm:
        confirm1 = input("Первое подтверждение - введите 'yes': ").strip().lower()
        if confirm1 != 'yes':
            return False

        confirm2 = input("Второе подтверждение - введите 'yes': ").strip().lower()
        if confirm2 != 'yes':
            return False

        confirm3 = input("Третье подтверждение - введите 'yes': ").strip().lower()
        return confirm3 == 'yes'
    else:
        # Для режима 'select' двойное подтверждение
        confirm1 = input("Первое подтверждение - введите 'yes': ").strip().lower()
        if confirm1 != 'yes':
            return False

        confirm2 = input("Второе подтверждение - введите 'yes': ").strip().lower()
        return confirm2 == 'yes'


async def reset_proxies(conn) -> None:
    """Сбросить состояние прокси без удаления"""
    print("Сброс состояния прокси (освобождение всех прокси)...")
    result = await conn.execute("""
        UPDATE proxies
        SET is_in_use = FALSE,
            worker_id = NULL,
            updated_at = NOW()
        WHERE is_in_use = TRUE
    """)
    print(f"Прокси освобождены: {result}")


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Очистка таблиц БД и управление прокси')
    parser.add_argument('--mode', choices=['all', 'select', 'reset-proxies'], required=True,
                        help='Режим: all (все таблицы), select (выборочная очистка), reset-proxies (освободить прокси)')
    parser.add_argument('--yes', action='store_true',
                        help='Автоматическое подтверждение без запроса')
    args = parser.parse_args()

    # Для reset-proxies не требуется подтверждение
    if args.mode == 'reset-proxies':
        print()
        print("Подключение к БД...")
        conn = await connect_db()
        try:
            await reset_proxies(conn)
        except Exception as e:
            print(f"Ошибка: {e}")
            sys.exit(1)
        finally:
            await conn.close()
        return

    # Подтверждение для очистки таблиц (если не передан --yes)
    # Для режима 'all' - тройное подтверждение, для 'select' - двойное
    if not args.yes:
        triple = (args.mode == 'all')
        if not confirm_action(args.mode, triple_confirm=triple):
            print("Операция отменена")
            sys.exit(0)

    # Подключение к БД
    print()
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        if args.mode == 'all':
            await clear_all_tables(conn)
        else:  # select
            await clear_selected_tables(conn)

    except Exception as e:
        print(f"Ошибка: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
