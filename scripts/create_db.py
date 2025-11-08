"""Скрипт для создания таблиц БД из schema.sql"""

import asyncio
import sys
from pathlib import Path

from database import connect_db, execute_sql_file


async def main():
    """Создать таблицы в БД из schema.sql"""
    schema_path = Path(__file__).parent / 'schema.sql'

    if not schema_path.exists():
        print(f"Ошибка: файл {schema_path} не найден")
        sys.exit(1)

    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # Миграция: переименование status → state в articulums (если таблица существует)
        print("Проверка необходимости миграции...")
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'articulums'
            )
        """)

        if table_exists:
            column_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.columns
                    WHERE table_name = 'articulums' AND column_name = 'status'
                )
            """)

            if column_exists:
                print("Миграция: переименование поля status → state в таблице articulums...")
                await conn.execute("ALTER TABLE articulums RENAME COLUMN status TO state")
                await conn.execute("ALTER TABLE articulums ADD COLUMN IF NOT EXISTS state_updated_at TIMESTAMP DEFAULT NOW()")
                await conn.execute("DROP INDEX IF EXISTS idx_articulums_status")
                print("Миграция завершена!")

        print(f"Выполнение SQL из {schema_path}...")
        await execute_sql_file(conn, str(schema_path))
        print("Таблицы успешно созданы/обновлены!")

    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
