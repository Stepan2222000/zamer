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
        print(f"Выполнение SQL из {schema_path}...")
        await execute_sql_file(conn, str(schema_path))
        print("Таблицы успешно созданы!")

    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
