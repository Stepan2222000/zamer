"""Управление задачами парсинга объявлений"""

import asyncpg
from typing import Optional
from config import MAX_OBJECT_WORKERS, TaskStatus


async def create_object_tasks_for_articulum(
    conn: asyncpg.Connection,
    articulum_id: int
) -> int:
    """
    Создает object_tasks только для объявлений, прошедших ВСЕ этапы валидации.

    Объявление должно пройти все 3 этапа:
    - price_filter (passed=true)
    - mechanical (passed=true)
    - ai (passed=true, если включена ИИ-валидация)

    Возвращает количество созданных задач.
    """
    # Получаем список всех validation_type для проверки
    validation_types = await conn.fetch("""
        SELECT DISTINCT validation_type
        FROM validation_results
        WHERE articulum_id = $1
        ORDER BY validation_type
    """, articulum_id)

    types_set = {row['validation_type'] for row in validation_types}

    # Определяем какие типы валидации должны быть пройдены
    # Минимум: price_filter и mechanical
    # Если есть 'ai' результаты - значит ИИ-валидация была включена
    required_types = ['price_filter', 'mechanical']
    if 'ai' in types_set:
        required_types.append('ai')

    # Создаем задачи только для объявлений, прошедших ВСЕ требуемые этапы
    created_count = await conn.fetchval("""
        WITH validated_items AS (
            -- Объявления, прошедшие ВСЕ этапы валидации
            SELECT DISTINCT vr.avito_item_id
            FROM validation_results vr
            WHERE vr.articulum_id = $1
              AND vr.passed = true
            GROUP BY vr.avito_item_id
            HAVING COUNT(DISTINCT vr.validation_type) = $3
              AND ARRAY_AGG(DISTINCT vr.validation_type ORDER BY vr.validation_type) = $4::text[]
        ),
        new_tasks AS (
            INSERT INTO object_tasks (articulum_id, avito_item_id, status)
            SELECT $1, vi.avito_item_id, $2
            FROM validated_items vi
            WHERE NOT EXISTS (
                SELECT 1
                FROM object_tasks ot
                WHERE ot.articulum_id = $1
                  AND ot.avito_item_id = vi.avito_item_id
            )
            RETURNING 1
        )
        SELECT COUNT(*) FROM new_tasks
    """, articulum_id, TaskStatus.PENDING, len(required_types), sorted(required_types))

    return created_count or 0


async def acquire_object_task(conn: asyncpg.Connection, worker_id: int) -> Optional[dict]:
    """
    Атомарно берет object_task из очереди.

    Проверяет лимит MAX_OBJECT_WORKERS и возвращает задачу только если лимит не превышен.
    """
    async with conn.transaction():
        # Advisory lock для сериализации доступа к object очереди
        # Ключ 2 = object queue (предотвращает race condition при проверке лимита)
        await conn.execute("SELECT pg_advisory_xact_lock(2)")

        # Проверяем лимит активных задач
        active_count = await conn.fetchval("""
            SELECT COUNT(*)
            FROM object_tasks
            WHERE status = $1
        """, TaskStatus.PROCESSING)

        # Если лимит превышен - откатываем транзакцию
        if active_count >= MAX_OBJECT_WORKERS:
            return None

        # Берем задачу
        task = await conn.fetchrow("""
            SELECT *
            FROM object_tasks
            WHERE status = $1
            ORDER BY created_at ASC
            LIMIT 1
        """, TaskStatus.PENDING)

        if not task:
            return None

        # Обновляем задачу
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
