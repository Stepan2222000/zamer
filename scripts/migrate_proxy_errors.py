"""
Скрипт миграции БД для добавления счетчика ошибок прокси.

Добавляет поля в таблицу proxies:
- consecutive_errors INTEGER DEFAULT 0
- last_error_at TIMESTAMP

ВАЖНО: Перед запуском остановите все контейнеры!
"""

import asyncio
import asyncpg
import sys
import os

# Добавляем container в путь для импорта конфига
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'container'))

from config import DB_CONFIG


async def migrate_database():
    """Выполняет миграцию БД"""
    print("=" * 60)
    print("МИГРАЦИЯ БД: Счетчик ошибок прокси")
    print("=" * 60)
    print()
    print("Эта миграция:")
    print("1. Добавит поле consecutive_errors INTEGER DEFAULT 0")
    print("2. Добавит поле last_error_at TIMESTAMP")
    print("3. Применится к таблице: proxies")
    print()
    print("ВАЖНО: Убедитесь что все контейнеры остановлены!")
    print()

    response = input("Продолжить миграцию? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Миграция отменена")
        return

    print()
    print("Подключение к БД...")

    try:
        conn = await asyncpg.connect(**DB_CONFIG)
        print(f"✓ Подключено к {DB_CONFIG['database']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}")
        print()

        # Проверяем, существует ли уже поле consecutive_errors
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = 'proxies'
                AND column_name = 'consecutive_errors'
            )
        """)

        if column_exists:
            print("✓ Миграция уже выполнена!")
            await conn.close()
            return

        print("Начало миграции...")
        print()

        # Добавляем поле consecutive_errors
        print("[1/2] Добавление поля consecutive_errors...")
        await conn.execute("""
            ALTER TABLE proxies
            ADD COLUMN consecutive_errors INTEGER DEFAULT 0
        """)
        print("  ✓ Поле consecutive_errors добавлено")

        # Добавляем поле last_error_at
        print("[2/2] Добавление поля last_error_at...")
        await conn.execute("""
            ALTER TABLE proxies
            ADD COLUMN last_error_at TIMESTAMP
        """)
        print("  ✓ Поле last_error_at добавлено")

        print()
        print("=" * 60)
        print("✓ МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        print()
        print("Теперь можно запускать контейнеры с механизмом счетчика ошибок.")

        await conn.close()

    except Exception as e:
        print()
        print("=" * 60)
        print(f"✗ ОШИБКА МИГРАЦИИ: {e}")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(migrate_database())
    except KeyboardInterrupt:
        print("\nМиграция прервана пользователем")
        sys.exit(1)
