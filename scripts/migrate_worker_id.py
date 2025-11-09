"""
Скрипт миграции БД для поддержки multi-server развертывания.

Изменяет тип worker_id с INTEGER на VARCHAR(50) в таблицах:
- proxies
- catalog_tasks
- object_tasks

ВАЖНО: Перед запуском остановите все контейнеры!
"""

import asyncio
import asyncpg
import sys
import os

# Добавляем container в путь для импорта конфига
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'container'))

from config import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME


async def migrate_database():
    """Выполняет миграцию БД"""
    print("=" * 60)
    print("МИГРАЦИЯ БД: Worker ID (INTEGER → VARCHAR)")
    print("=" * 60)
    print()
    print("Эта миграция:")
    print("1. Изменит тип worker_id: INTEGER → VARCHAR(50)")
    print("2. Обнулит текущие значения worker_id (они больше не валидны)")
    print("3. Применится к таблицам: proxies, catalog_tasks, object_tasks")
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
        conn = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print(f"✓ Подключено к {DB_NAME}@{DB_HOST}:{DB_PORT}")
        print()

        # Проверяем текущий тип
        current_type = await conn.fetchval("""
            SELECT data_type
            FROM information_schema.columns
            WHERE table_name = 'proxies'
            AND column_name = 'worker_id'
        """)

        print(f"Текущий тип worker_id в proxies: {current_type}")

        if current_type == 'character varying':
            print("✓ Миграция уже выполнена!")
            await conn.close()
            return

        print()
        print("Начало миграции...")
        print()

        # Миграция таблицы proxies
        print("[1/3] Миграция таблицы proxies...")
        await conn.execute("""
            ALTER TABLE proxies
            ALTER COLUMN worker_id TYPE VARCHAR(50)
        """)
        # Обнуляем worker_id и освобождаем прокси
        affected = await conn.execute("""
            UPDATE proxies
            SET worker_id = NULL,
                is_in_use = FALSE,
                updated_at = NOW()
            WHERE worker_id IS NOT NULL
        """)
        print(f"  ✓ Тип изменен, освобождено прокси: {affected.split()[-1]}")

        # Миграция таблицы catalog_tasks
        print("[2/3] Миграция таблицы catalog_tasks...")
        await conn.execute("""
            ALTER TABLE catalog_tasks
            ALTER COLUMN worker_id TYPE VARCHAR(50)
        """)
        # Возвращаем processing задачи в pending
        affected = await conn.execute("""
            UPDATE catalog_tasks
            SET status = 'pending',
                worker_id = NULL,
                updated_at = NOW()
            WHERE status = 'processing'
        """)
        print(f"  ✓ Тип изменен, возвращено задач в очередь: {affected.split()[-1]}")

        # Миграция таблицы object_tasks
        print("[3/3] Миграция таблицы object_tasks...")
        await conn.execute("""
            ALTER TABLE object_tasks
            ALTER COLUMN worker_id TYPE VARCHAR(50)
        """)
        # Возвращаем processing задачи в pending
        affected = await conn.execute("""
            UPDATE object_tasks
            SET status = 'pending',
                worker_id = NULL,
                updated_at = NOW()
            WHERE status = 'processing'
        """)
        print(f"  ✓ Тип изменен, возвращено задач в очередь: {affected.split()[-1]}")

        print()
        print("=" * 60)
        print("✓ МИГРАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")
        print("=" * 60)
        print()
        print("Теперь можно запускать контейнеры с поддержкой multi-server.")

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
