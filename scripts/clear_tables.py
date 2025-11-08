"""Скрипт для очистки таблиц БД"""

import asyncio
import argparse
import sys

from database import connect_db


async def clear_all_tables(conn) -> None:
    """Очистить все таблицы"""
    print("Очистка таблицы articulums...")
    await conn.execute('TRUNCATE TABLE articulums CASCADE')

    print("Очистка таблицы proxies...")
    await conn.execute('TRUNCATE TABLE proxies CASCADE')

    print("Все таблицы очищены!")


async def clear_selected_tables(conn) -> None:
    """Очистить выбранные таблицы через интерактивное меню"""
    print()
    print("Выберите таблицы для очистки:")
    print("  [1] articulums")
    print("  [2] proxies")
    print("  [3] Обе таблицы")
    print()

    choice = input("Ваш выбор (1-3): ").strip()

    if choice == '1':
        print("Очистка таблицы articulums...")
        await conn.execute('TRUNCATE TABLE articulums CASCADE')
        print("Таблица articulums очищена!")

    elif choice == '2':
        print("Очистка таблицы proxies...")
        await conn.execute('TRUNCATE TABLE proxies CASCADE')
        print("Таблица proxies очищена!")

    elif choice == '3':
        print("Очистка таблицы articulums...")
        await conn.execute('TRUNCATE TABLE articulums CASCADE')
        print("Очистка таблицы proxies...")
        await conn.execute('TRUNCATE TABLE proxies CASCADE')
        print("Обе таблицы очищены!")

    else:
        print("Неверный выбор. Отмена операции.")
        sys.exit(0)


def confirm_action(mode: str) -> bool:
    """Запросить подтверждение действия"""
    if mode == 'all':
        message = "ВСЕ ТАБЛИЦЫ БУДУТ ОЧИЩЕНЫ! Продолжить?"
    else:
        message = "ВЫБРАННЫЕ ТАБЛИЦЫ БУДУТ ОЧИЩЕНЫ! Продолжить?"

    print()
    print("=" * 60)
    print(f"ВНИМАНИЕ: {message}")
    print("=" * 60)

    confirm = input("Введите 'yes' для подтверждения: ").strip().lower()
    return confirm == 'yes'


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Очистка таблиц БД')
    parser.add_argument('--mode', choices=['all', 'select'], required=True,
                        help='Режим: all (все таблицы) или select (выборочная очистка)')
    args = parser.parse_args()

    # Двойное подтверждение
    if not confirm_action(args.mode):
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
