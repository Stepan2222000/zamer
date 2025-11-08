"""Главный процесс оркестрации системы парсинга"""

import asyncio
import logging
import signal
import sys
from typing import List, Dict

import asyncpg

from config import TOTAL_BROWSER_WORKERS, ArticulumState
from database import create_pool
from xvfb_manager import init_xvfb_displays, cleanup_displays, get_display_env
from heartbeat_manager import heartbeat_check_loop
from catalog_task_manager import create_catalog_task
from object_task_manager import create_object_tasks_for_articulum

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MainProcess')


class MainProcess:
    """Главный процесс системы"""

    def __init__(self):
        self.pool: asyncpg.Pool = None
        self.worker_processes: Dict[int, asyncio.subprocess.Process] = {}
        self.heartbeat_task: asyncio.Task = None
        self.shutdown_event = asyncio.Event()

    async def init_system(self):
        """Инициализация системы"""
        logger.info("Инициализация системы...")

        # Создание Xvfb дисплеев
        logger.info("Создание виртуальных дисплеев...")
        init_xvfb_displays()

        # Подключение к БД
        logger.info("Подключение к БД...")
        self.pool = await create_pool()

        # Создание catalog_tasks для NEW артикулов
        logger.info("Создание catalog_tasks для NEW артикулов...")
        await self.create_catalog_tasks_from_new_articulums()

        # Создание object_tasks для VALIDATED артикулов
        logger.info("Создание object_tasks для VALIDATED артикулов...")
        await self.create_object_tasks_from_validated_articulums()

        logger.info("Система инициализирована")

    async def create_catalog_tasks_from_new_articulums(self):
        """Создает catalog_tasks для всех артикулов в состоянии NEW"""
        # Получаем список NEW артикулов
        async with self.pool.acquire() as conn:
            new_articulums = await conn.fetch("""
                SELECT id, articulum
                FROM articulums
                WHERE state = $1
                ORDER BY created_at ASC
            """, ArticulumState.NEW)

        if not new_articulums:
            logger.info("Нет NEW артикулов для обработки")
            return

        logger.info(f"Найдено {len(new_articulums)} NEW артикулов")

        created_count = 0
        for articulum in new_articulums:
            articulum_id = articulum['id']
            articulum_value = articulum['articulum']

            # Создаем catalog_task в отдельной транзакции
            async with self.pool.acquire() as conn:
                task_id = await create_catalog_task(conn, articulum_id)

                if task_id:
                    logger.info(f"Создана catalog_task#{task_id} для артикула '{articulum_value}'")
                    created_count += 1

        logger.info(f"Создано {created_count} задач")

    async def create_object_tasks_from_validated_articulums(self):
        """
        Создает object_tasks для всех артикулов в состоянии VALIDATED.

        ВАЖНО: В Этапе 5 создает задачи для ВСЕХ объявлений (временно).
        В Этапе 7 будет изменено на создание только для прошедших валидацию.
        """
        async with self.pool.acquire() as conn:
            # TODO: ВРЕМЕННОЕ РЕШЕНИЕ ДЛЯ STAGE 5!
            # После реализации Validation Workers (Stage 6-7) вернуть на VALIDATED
            validated_articulums = await conn.fetch("""
                SELECT id, articulum
                FROM articulums
                WHERE state = $1
                ORDER BY created_at ASC
            """, ArticulumState.CATALOG_PARSED)  # ВРЕМЕННО: CATALOG_PARSED вместо VALIDATED

        if not validated_articulums:
            logger.info("Нет CATALOG_PARSED артикулов для создания object_tasks")  # ВРЕМЕННО
            return

        logger.info(f"Найдено {len(validated_articulums)} CATALOG_PARSED артикулов")  # ВРЕМЕННО

        total_tasks_created = 0
        for articulum in validated_articulums:
            articulum_id = articulum['id']
            articulum_value = articulum['articulum']

            async with self.pool.acquire() as conn:
                count = await create_object_tasks_for_articulum(conn, articulum_id)

                if count > 0:
                    logger.info(f"Создано {count} object_tasks для артикула '{articulum_value}'")
                    total_tasks_created += count

        logger.info(f"Всего создано {total_tasks_created} object_tasks")

    async def spawn_browser_workers(self):
        """Запускает browser workers"""
        logger.info(f"Запуск {TOTAL_BROWSER_WORKERS} browser workers...")

        for worker_id in range(1, TOTAL_BROWSER_WORKERS + 1):
            display = get_display_env(worker_id)

            # Формируем аргументы для subprocess
            args = [sys.executable, 'browser_worker.py', str(worker_id)]
            if display:
                args.append(display)

            # Запускаем воркер как subprocess
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            self.worker_processes[worker_id] = process
            logger.info(f"Запущен Worker#{worker_id} (PID={process.pid}, DISPLAY={display or 'headless'})")

        logger.info(f"Все {TOTAL_BROWSER_WORKERS} workers запущены")

    async def monitor_workers(self):
        """Мониторинг воркеров и перезапуск при падении"""
        logger.info("Запущен мониторинг воркеров...")

        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(10)

                # Проверяем статус каждого воркера
                for worker_id, process in list(self.worker_processes.items()):
                    if process.returncode is not None:
                        # Воркер завершился
                        logger.warning(f"Worker#{worker_id} завершен (код={process.returncode})")

                        # Перезапускаем воркер
                        display = get_display_env(worker_id)

                        # Формируем аргументы для subprocess
                        args = [sys.executable, 'browser_worker.py', str(worker_id)]
                        if display:
                            args.append(display)

                        new_process = await asyncio.create_subprocess_exec(
                            *args,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )

                        self.worker_processes[worker_id] = new_process
                        logger.info(f"Worker#{worker_id} перезапущен (PID={new_process.pid})")

            except asyncio.CancelledError:
                logger.info("Остановка мониторинга воркеров")
                break
            except Exception as e:
                logger.error(f"Ошибка мониторинга воркеров: {e}")

    async def shutdown(self):
        """Graceful shutdown системы"""
        logger.info("Начало graceful shutdown...")

        # Устанавливаем флаг остановки
        self.shutdown_event.set()

        # Останавливаем heartbeat checker
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass

        # Останавливаем все воркеры
        logger.info("Остановка всех воркеров...")
        for worker_id, process in self.worker_processes.items():
            try:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=10)
                logger.info(f"Worker#{worker_id} остановлен")
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                logger.warning(f"Worker#{worker_id} убит (SIGKILL)")

        # Закрываем пул БД
        if self.pool:
            await self.pool.close()
            logger.info("Пул БД закрыт")

        # Останавливаем Xvfb дисплеи
        cleanup_displays()

        logger.info("Shutdown завершен")

    async def run(self):
        """Главная функция запуска системы"""
        try:
            # Инициализация
            await self.init_system()

            # Запуск heartbeat checker в фоне
            self.heartbeat_task = asyncio.create_task(heartbeat_check_loop(self.pool))

            # Запуск browser workers
            await self.spawn_browser_workers()

            # Мониторинг воркеров
            await self.monitor_workers()

        except KeyboardInterrupt:
            logger.info("Получен сигнал остановки (Ctrl+C)")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
        finally:
            await self.shutdown()


def setup_signal_handlers(main_process: MainProcess):
    """Настройка обработчиков сигналов"""
    def signal_handler(signum, frame):
        logger.info(f"Получен сигнал {signum}")
        # Устанавливаем флаг остановки
        main_process.shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


async def main():
    """Точка входа"""
    logger.info("Запуск главного процесса...")

    main_process = MainProcess()
    setup_signal_handlers(main_process)

    await main_process.run()


if __name__ == '__main__':
    asyncio.run(main())
