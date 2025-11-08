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

                # ЗАТЕМ возвращаем задачу в очередь
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


async def heartbeat_check_loop(pool: asyncpg.Pool) -> None:
    """
    Бесконечный цикл проверки зависших задач.

    Запускается как фоновая задача в main.py.
    """
    logger.info(f"Запущена фоновая проверка зависших задач "
                f"(интервал: {HEARTBEAT_CHECK_INTERVAL}с, таймаут: {HEARTBEAT_TIMEOUT_SECONDS}с)")

    while True:
        try:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL)

            # Проверяем catalog_tasks
            returned = await check_expired_catalog_tasks(pool)

            if returned > 0:
                logger.info(f"Возвращено задач: {returned}")

        except asyncio.CancelledError:
            logger.info("Остановка фоновой проверки...")
            break
        except Exception as e:
            logger.error(f"Ошибка при проверке зависших задач: {e}", exc_info=True)
            # Продолжаем работу даже при ошибке
            continue
