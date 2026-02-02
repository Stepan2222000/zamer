"""Управление задачами парсинга каталогов"""

import asyncpg
from typing import Optional
from datetime import datetime

from config import TaskStatus, ArticulumState
from state_machine import (
    transition_to_catalog_parsing,
    transition_to_catalog_parsed,
    StateTransitionError
)


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


async def acquire_catalog_task(conn: asyncpg.Connection, worker_id: str) -> Optional[dict]:
    """
    Атомарно берет catalog_task из очереди.

    Использует SELECT FOR UPDATE SKIP LOCKED для предотвращения race condition.
    """
    async with conn.transaction():
        # Берем задачу для артикулов в состоянии NEW
        # FOR UPDATE OF ct SKIP LOCKED блокирует строку задачи и пропускает уже заблокированные
        task = await conn.fetchrow("""
            SELECT ct.*, a.articulum
            FROM catalog_tasks ct
            JOIN articulums a ON a.id = ct.articulum_id
            WHERE ct.status = $1 AND a.state = $2
            ORDER BY ct.created_at ASC
            LIMIT 1
            FOR UPDATE OF ct SKIP LOCKED
        """, TaskStatus.PENDING, ArticulumState.NEW)

        if not task:
            return None

        # Переводим артикул в CATALOG_PARSING и обновляем задачу атомарно
        success = await transition_to_catalog_parsing(conn, task['articulum_id'])

        if not success:
            # Артикул уже в другом состоянии (другой воркер успел взять)
            return None

        # Обновляем задачу
        await conn.execute("""
            UPDATE catalog_tasks
            SET status = $1,
                worker_id = $2,
                heartbeat_at = NOW(),
                updated_at = NOW()
            WHERE id = $3
        """, TaskStatus.PROCESSING, worker_id, task['id'])

        return dict(task)


async def complete_catalog_task(conn: asyncpg.Connection, task_id: int, articulum_id: int) -> None:
    """
    Завершает catalog_task и переводит артикул в CATALOG_PARSED.

    ВАЖНО: Вызывается внутри транзакции в browser_worker.py.
    Вызывает StateTransitionError если переход состояния не удался.
    """
    # Обновляем статус задачи
    await conn.execute("""
        UPDATE catalog_tasks
        SET status = $1,
            updated_at = NOW()
        WHERE id = $2
    """, TaskStatus.COMPLETED, task_id)

    # Переводим артикул в CATALOG_PARSED
    # После этого Validation Worker заберет его для валидации
    success = await transition_to_catalog_parsed(conn, articulum_id)

    if not success:
        # Критическая ошибка: артикул не в ожидаемом состоянии
        # Транзакция должна быть откачена для возврата задачи в очередь
        raise StateTransitionError(
            f"Не удалось завершить catalog_task#{task_id}: "
            f"артикул#{articulum_id} не в состоянии CATALOG_PARSING"
        )


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


async def increment_wrong_page_count(conn: asyncpg.Connection, task_id: int) -> int:
    """
    ВРЕМЕННОЕ РЕШЕНИЕ: Увеличить счетчик WRONG_PAGE для диагностики.
    Счетчик накопительный (не сбрасывается).
    TODO: удалить после анализа проблем с WRONG_PAGE

    Возвращает новое значение счетчика.
    """
    return await conn.fetchval("""
        UPDATE catalog_tasks
        SET wrong_page_count = COALESCE(wrong_page_count, 0) + 1,
            updated_at = NOW()
        WHERE id = $1
        RETURNING wrong_page_count
    """, task_id)
