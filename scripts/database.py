"""Модуль для работы с PostgreSQL"""

import asyncpg

# Параметры подключения (хардкод)
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


async def execute_sql_file(conn: asyncpg.Connection, filepath: str) -> None:
    """Выполнить SQL команды из файла"""
    with open(filepath, 'r', encoding='utf-8') as f:
        sql = f.read()
    await conn.execute(sql)
