"""Модуль для работы с PostgreSQL"""

import asyncpg
from config import DB_CONFIG


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)


async def create_pool() -> asyncpg.Pool:
    """Создать пул подключений к БД"""
    return await asyncpg.create_pool(**DB_CONFIG)


async def execute_sql_file(conn: asyncpg.Connection, filepath: str) -> None:
    """Выполнить SQL команды из файла"""
    with open(filepath, 'r', encoding='utf-8') as f:
        sql = f.read()
    await conn.execute(sql)
