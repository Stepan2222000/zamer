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

    # Двойное подтверждение для очистки таблиц
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
