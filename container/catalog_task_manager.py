"""Управление задачами парсинга каталогов"""

import asyncpg
from typing import Optional
from datetime import datetime

from config import MAX_CATALOG_WORKERS, TaskStatus
from state_machine import transition_to_catalog_parsing, transition_to_catalog_parsed
# ВРЕМЕННО для Stage 5: автоматическое создание object_tasks
from object_task_manager import create_object_tasks_for_articulum


async def create_catalog_task(conn: asyncpg.Connection, articulum_id: int) -> Optional[int]:
    """
    Создает catalog_task для артикула и переводит его в CATALOG_PARSING.

    Возвращает ID созданной задачи или None если переход в CATALOG_PARSING не удался.
    """
    # Переводим артикул в CATALOG_PARSING атомарно
    success = await transition_to_catalog_parsing(conn, articulum_id)

    if not success:
        return None

    # Создаем catalog_task
    task_id = await conn.fetchval("""
        INSERT INTO catalog_tasks (articulum_id, status, checkpoint_page)
        VALUES ($1, $2, 1)
        RETURNING id
    """, articulum_id, TaskStatus.PENDING)

    return task_id


async def acquire_catalog_task(conn: asyncpg.Connection, worker_id: int) -> Optional[dict]:
    """
    Атомарно берет catalog_task из очереди.

    Проверяет лимит MAX_CATALOG_WORKERS и возвращает задачу только если лимит не превышен.
    """
    async with conn.transaction():
        # Проверка лимита активных воркеров
        active_count = await conn.fetchval("""
            SELECT COUNT(*) FROM catalog_tasks
            WHERE status = $1
        """, TaskStatus.PROCESSING)

        if active_count >= MAX_CATALOG_WORKERS:
            return None

        # Атомарная выдача задачи
        task = await conn.fetchrow("""
            SELECT * FROM catalog_tasks
            WHERE status = $1
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """, TaskStatus.PENDING)

        if not task:
            return None

        # Помечаем задачу как обрабатываемую
        await conn.execute("""
            UPDATE catalog_tasks
            SET status = $1,
                worker_id = $2,
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = $3
        """, TaskStatus.PROCESSING, worker_id, task['id'])

        # Возвращаем обновленную задачу
        updated_task = await conn.fetchrow("""
            SELECT ct.*, a.articulum
            FROM catalog_tasks ct
            JOIN articulums a ON a.id = ct.articulum_id
            WHERE ct.id = $1
        """, task['id'])

        return dict(updated_task)


async def complete_catalog_task(conn: asyncpg.Connection, task_id: int, articulum_id: int) -> None:
    """
    Завершает catalog_task и переводит артикул в CATALOG_PARSED.

    ВРЕМЕННО для Stage 5: автоматически создает object_tasks для артикула.
    """
    async with conn.transaction():
        # Обновляем статус задачи
        await conn.execute("""
            UPDATE catalog_tasks
            SET status = $1,
                updated_at = NOW()
            WHERE id = $2
        """, TaskStatus.COMPLETED, task_id)

        # Переводим артикул в CATALOG_PARSED
        await transition_to_catalog_parsed(conn, articulum_id)

        # TODO: ВРЕМЕННОЕ РЕШЕНИЕ ДЛЯ STAGE 5!
        # После реализации Validation Workers (Stage 6-7) убрать отсюда
        # Автоматически создаем object_tasks для всех объявлений артикула
        await create_object_tasks_for_articulum(conn, articulum_id)


async def fail_catalog_task(conn: asyncpg.Connection, task_id: int, reason: str = None) -> None:
    """
    Помечает catalog_task как failed.
    """
    await conn.execute("""
        UPDATE catalog_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.FAILED, task_id)


async def invalidate_catalog_task(conn: asyncpg.Connection, task_id: int, reason: str = None) -> None:
    """
    Помечает catalog_task как invalid.
    """
    await conn.execute("""
        UPDATE catalog_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.INVALID, task_id)


async def return_catalog_task_to_queue(conn: asyncpg.Connection, task_id: int) -> None:
    """
    Возвращает catalog_task в очередь (статус pending).
    """
    await conn.execute("""
        UPDATE catalog_tasks
        SET status = $1,
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.PENDING, task_id)


async def update_catalog_task_checkpoint(conn: asyncpg.Connection, task_id: int, page_num: int) -> None:
    """
    Обновляет чекпоинт (номер последней обработанной страницы).
    """
    await conn.execute("""
        UPDATE catalog_tasks
        SET checkpoint_page = $1,
            updated_at = NOW()
        WHERE id = $2
    """, page_num, task_id)


async def update_catalog_task_heartbeat(conn: asyncpg.Connection, task_id: int) -> None:
    """
    Обновляет heartbeat задачи (показывает что воркер жив).
    """
    await conn.execute("""
        UPDATE catalog_tasks
        SET heartbeat_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
    """, task_id)
