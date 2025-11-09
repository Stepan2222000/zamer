"""Фоновая проверка зависших задач через heartbeat"""

import asyncio
import logging
import asyncpg

from config import HEARTBEAT_TIMEOUT_SECONDS, HEARTBEAT_CHECK_INTERVAL, TaskStatus

logger = logging.getLogger(__name__)


async def check_expired_catalog_tasks(pool: asyncpg.Pool) -> int:
    """
    Проверяет catalog_tasks и возвращает зависшие в очередь.

    Возвращает количество возвращенных задач.
    """
    async with pool.acquire() as conn:
        # Находим зависшие задачи
        expired_tasks = await conn.fetch(f"""
            SELECT id, worker_id, articulum_id
            FROM catalog_tasks
            WHERE status = $1
              AND heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT_SECONDS} seconds'
        """, TaskStatus.PROCESSING)

        if not expired_tasks:
            return 0

        returned_count = 0

        for task in expired_tasks:
            task_id = task['id']
            worker_id = task['worker_id']
            articulum_id = task['articulum_id']

            async with conn.transaction():
                # СНАЧАЛА освобождаем прокси (чтобы избежать race condition)
                if worker_id:
                    await conn.execute("""
                        UPDATE proxies
                        SET is_in_use = FALSE,
                            worker_id = NULL,
                            updated_at = NOW()
                        WHERE worker_id = $1
                    """, worker_id)

                # Возвращаем артикул в NEW если он в CATALOG_PARSING
                # (если артикул уже в другом состоянии - не трогаем)
                await conn.execute("""
                    UPDATE articulums
                    SET state = 'NEW',
                        state_updated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $1 AND state = 'CATALOG_PARSING'
                """, articulum_id)

                # Возвращаем задачу в очередь
                await conn.execute("""
                    UPDATE catalog_tasks
                    SET status = $1,
                        worker_id = NULL,
                        updated_at = NOW()
                    WHERE id = $2
                """, TaskStatus.PENDING, task_id)

                logger.warning(f"Задача catalog_task#{task_id} (артикул#{articulum_id}) "
                              f"возвращена в очередь (worker#{worker_id} зависнул)")

                returned_count += 1

        return returned_count


async def fix_orphaned_catalog_tasks(pool: asyncpg.Pool) -> int:
    """
    Исправляет orphaned состояния: артикулы в CATALOG_PARSING с pending задачами.

    Такие состояния возникают когда:
    - Артикул был переведен в CATALOG_PARSING
    - Но задача осталась в pending (не была обновлена на processing)

    Возвращает количество исправленных артикулов.
    """
    async with pool.acquire() as conn:
        fixed_count = await conn.fetchval("""
            WITH orphaned AS (
                SELECT DISTINCT a.id, a.articulum
                FROM articulums a
                INNER JOIN catalog_tasks ct ON ct.articulum_id = a.id
                WHERE a.state = 'CATALOG_PARSING'
                  AND ct.status = 'pending'
            )
            UPDATE articulums
            SET state = 'NEW',
                state_updated_at = NOW(),
                updated_at = NOW()
            FROM orphaned
            WHERE articulums.id = orphaned.id
            RETURNING articulums.id
        """)

        if fixed_count:
            logger.warning(f"Исправлено orphaned артикулов: {fixed_count}")

        return fixed_count or 0


async def check_expired_object_tasks(pool: asyncpg.Pool) -> int:
    """
    Проверяет object_tasks и возвращает зависшие в очередь.

    Возвращает количество возвращенных задач.
    """
    async with pool.acquire() as conn:
        # Находим зависшие задачи
        expired_tasks = await conn.fetch(f"""
            SELECT id, worker_id, articulum_id, avito_item_id
            FROM object_tasks
            WHERE status = $1
              AND heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT_SECONDS} seconds'
        """, TaskStatus.PROCESSING)

        if not expired_tasks:
            return 0

        returned_count = 0

        for task in expired_tasks:
            task_id = task['id']
            worker_id = task['worker_id']
            articulum_id = task['articulum_id']
            avito_item_id = task['avito_item_id']

            async with conn.transaction():
                # СНАЧАЛА освобождаем прокси
                if worker_id:
                    await conn.execute("""
                        UPDATE proxies
                        SET is_in_use = FALSE,
                            worker_id = NULL,
                            updated_at = NOW()
                        WHERE worker_id = $1
                    """, worker_id)

                # ЗАТЕМ возвращаем задачу в очередь
                await conn.execute("""
                    UPDATE object_tasks
                    SET status = $1,
                        worker_id = NULL,
                        updated_at = NOW()
                    WHERE id = $2
                """, TaskStatus.PENDING, task_id)

                logger.warning(f"Задача object_task#{task_id} (объявление {avito_item_id}) "
                              f"возвращена в очередь (worker#{worker_id} зависнул)")

                returned_count += 1

        return returned_count


async def heartbeat_check_loop(pool: asyncpg.Pool) -> None:
    """
    Бесконечный цикл проверки зависших задач.

    Проверяет обе таблицы: catalog_tasks и object_tasks.
    Запускается как фоновая задача в main.py.
    """
    logger.info(f"Запущена фоновая проверка зависших задач "
                f"(интервал: {HEARTBEAT_CHECK_INTERVAL}с, таймаут: {HEARTBEAT_TIMEOUT_SECONDS}с)")

    while True:
        try:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)

            # Проверяем и исправляем orphaned артикулы (CATALOG_PARSING с pending задачами)
            orphaned_fixed = await fix_orphaned_catalog_tasks(pool)

            # Проверяем catalog_tasks
            catalog_returned = await check_expired_catalog_tasks(pool)

            # Проверяем object_tasks
            object_returned = await check_expired_object_tasks(pool)

            # Логируем только если есть возвраты или исправления
            total = catalog_returned + object_returned + orphaned_fixed
            if total > 0:
                logger.info(f"Возвращено задач: catalog={catalog_returned}, object={object_returned}, orphaned={orphaned_fixed}")

        except asyncio.CancelledError:
            logger.info("Остановка фоновой проверки...")
            break
        except Exception as e:
            logger.error(f"Ошибка при проверке зависших задач: {e}", exc_info=True)
            # Продолжаем работу даже при ошибке
            continue
