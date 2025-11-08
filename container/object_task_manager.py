"""Управление задачами парсинга объявлений"""

import asyncpg
from typing import Optional
from config import MAX_OBJECT_WORKERS, TaskStatus


async def create_object_tasks_for_articulum(
    conn: asyncpg.Connection,
    articulum_id: int
) -> int:
    """
    Создает object_tasks для всех объявлений артикула из catalog_listings.

    ВАЖНО: В Этапе 5 создает задачи для ВСЕХ объявлений (временно).
    В Этапе 7 будет изменено на создание только для прошедших валидацию.

    Возвращает количество созданных задач.
    """
    # Получаем список avito_item_id для артикула
    items = await conn.fetch("""
        SELECT DISTINCT avito_item_id
        FROM catalog_listings
        WHERE articulum_id = $1
    """, articulum_id)

    created_count = 0
    for item in items:
        await conn.execute("""
            INSERT INTO object_tasks (articulum_id, avito_item_id, status)
            VALUES ($1, $2, $3)
            ON CONFLICT DO NOTHING
        """, articulum_id, item['avito_item_id'], TaskStatus.PENDING)
        created_count += 1

    return created_count


async def acquire_object_task(conn: asyncpg.Connection, worker_id: int) -> Optional[dict]:
    """
    Атомарно берет object_task из очереди.

    Проверяет лимит MAX_OBJECT_WORKERS и возвращает задачу только если лимит не превышен.
    """
    async with conn.transaction():
        # Проверка лимита активных воркеров
        active_count = await conn.fetchval("""
            SELECT COUNT(*) FROM object_tasks
            WHERE status = $1
        """, TaskStatus.PROCESSING)

        if active_count >= MAX_OBJECT_WORKERS:
            return None

        # Атомарная выдача задачи
        task = await conn.fetchrow("""
            SELECT * FROM object_tasks
            WHERE status = $1
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """, TaskStatus.PENDING)

        if not task:
            return None

        # Помечаем задачу как обрабатываемую
        await conn.execute("""
            UPDATE object_tasks
            SET status = $1,
                worker_id = $2,
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = $3
        """, TaskStatus.PROCESSING, worker_id, task['id'])

        # Возвращаем обновленную задачу с join артикула
        updated_task = await conn.fetchrow("""
            SELECT ot.*, a.articulum
            FROM object_tasks ot
            JOIN articulums a ON a.id = ot.articulum_id
            WHERE ot.id = $1
        """, task['id'])

        return dict(updated_task)


async def complete_object_task(conn: asyncpg.Connection, task_id: int) -> None:
    """
    Завершает object_task.

    Артикул остается в состоянии OBJECT_PARSING (финальное состояние).
    """
    await conn.execute("""
        UPDATE object_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.COMPLETED, task_id)


async def fail_object_task(conn: asyncpg.Connection, task_id: int, reason: str = None) -> None:
    """
    Помечает object_task как failed.
    """
    await conn.execute("""
        UPDATE object_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.FAILED, task_id)


async def invalidate_object_task(conn: asyncpg.Connection, task_id: int, reason: str = None) -> None:
    """
    Помечает object_task как invalid (например, REMOVED_DETECTOR_ID).
    """
    await conn.execute("""
        UPDATE object_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.INVALID, task_id)


async def return_object_task_to_queue(conn: asyncpg.Connection, task_id: int) -> None:
    """
    Возвращает object_task в очередь (статус pending).
    """
    await conn.execute("""
        UPDATE object_tasks
        SET status = $1,
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.PENDING, task_id)


async def update_object_task_heartbeat(conn: asyncpg.Connection, task_id: int) -> None:
    """
    Обновляет heartbeat задачи (показывает что воркер жив).
    """
    await conn.execute("""
        UPDATE object_tasks
        SET heartbeat_at = NOW(),
            updated_at = NOW()
        WHERE id = $1
    """, task_id)
