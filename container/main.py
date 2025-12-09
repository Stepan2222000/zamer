"""Главный процесс оркестрации системы парсинга"""

import asyncio
import hashlib
import logging
import signal
import socket
import sys
from typing import List, Dict

import asyncpg

from config import (
    TOTAL_BROWSER_WORKERS, TOTAL_VALIDATION_WORKERS, SKIP_OBJECT_PARSING,
    REPARSE_MODE, MIN_REPARSE_INTERVAL_HOURS,
    ArticulumState, TaskStatus,
    AI_PROVIDER, HF_API_TOKEN, HF_ENDPOINT_NAME, ENABLE_AI_VALIDATION,
)
from database import create_pool
from xvfb_manager import init_xvfb_displays, cleanup_displays, get_display_env
from heartbeat_manager import heartbeat_check_loop
from object_task_manager import create_object_tasks_for_articulum


def get_container_id() -> str:
    """Генерирует уникальный ID контейнера на основе hostname"""
    hostname = socket.gethostname()
    # Используем первые 8 символов MD5 хеша для короткого ID
    return hashlib.md5(hostname.encode()).hexdigest()[:8]


# Глобальный уникальный ID контейнера
CONTAINER_ID = get_container_id()

async def _get_hf_endpoint():
    """Получить объект HF Inference Endpoint (DRY helper)"""
    from huggingface_hub import get_inference_endpoint
    return await asyncio.to_thread(
        get_inference_endpoint, HF_ENDPOINT_NAME, token=HF_API_TOKEN
    )


async def start_hf_endpoint() -> str:
    """
    Запускает HuggingFace Inference Endpoint при старте программы.
    Возвращает URL endpoint для использования в воркерах.
    Все синхронные вызовы HF SDK обернуты в asyncio.to_thread().
    """
    if AI_PROVIDER != 'huggingface' or not ENABLE_AI_VALIDATION:
        return None

    if not HF_API_TOKEN or not HF_ENDPOINT_NAME:
        logging.warning("HF_API_TOKEN или HF_ENDPOINT_NAME не заданы, HF Endpoint не запущен")
        return None

    try:
        logging.info(f"Запуск HuggingFace Endpoint '{HF_ENDPOINT_NAME}'...")

        endpoint = await _get_hf_endpoint()
        current_status = endpoint.status

        logging.info(f"Текущий статус endpoint: {current_status}")

        # Обработка статуса 'failed' — требует ручного вмешательства
        if current_status == 'failed':
            logging.error("HF Endpoint в статусе FAILED — требуется ручное вмешательство")
            logging.error("Проверьте логи endpoint в HuggingFace Dashboard")
            return None

        if current_status in ['paused', 'scaledToZero']:
            logging.info("Endpoint остановлен, запускаем...")
            await asyncio.to_thread(endpoint.resume)
            logging.info("Ожидаем готовности endpoint (может занять 2-5 минут)...")
            try:
                await asyncio.to_thread(endpoint.wait, timeout=600)
            except Exception as wait_error:
                if 'timeout' in str(wait_error).lower():
                    logging.error("Timeout при ожидании готовности HF Endpoint (600s)")
                    return None
                raise
        elif current_status == 'running':
            logging.info("Endpoint уже запущен")
        elif current_status in ['pending', 'initializing', 'updating']:
            logging.info(f"Endpoint в процессе запуска ({current_status}), ожидаем...")
            try:
                await asyncio.to_thread(endpoint.wait, timeout=600)
            except Exception as wait_error:
                if 'timeout' in str(wait_error).lower():
                    logging.error("Timeout при ожидании готовности HF Endpoint (600s)")
                    return None
                raise
        else:
            logging.warning(f"Неизвестный статус endpoint: {current_status}")
            return None

        # Получаем URL
        endpoint_url = endpoint.url
        logging.info(f"HF Endpoint запущен: {endpoint_url}")

        return endpoint_url

    except Exception as e:
        logging.error(f"Ошибка при запуске HF Endpoint: {e}", exc_info=True)
        return None


async def stop_hf_endpoint():
    """
    Останавливает HuggingFace Inference Endpoint для экономии ресурсов.
    Все синхронные вызовы HF SDK обернуты в asyncio.to_thread().
    """
    if AI_PROVIDER != 'huggingface' or not ENABLE_AI_VALIDATION:
        return

    if not HF_API_TOKEN or not HF_ENDPOINT_NAME:
        return

    try:
        logging.info(f"Остановка HuggingFace Endpoint '{HF_ENDPOINT_NAME}'...")

        endpoint = await _get_hf_endpoint()

        if endpoint.status == 'running':
            await asyncio.to_thread(endpoint.pause)
            logging.info("HF Endpoint остановлен (pause)")
        else:
            logging.info(f"HF Endpoint уже остановлен (статус: {endpoint.status})")

    except Exception as e:
        logging.error(f"Ошибка при остановке HF Endpoint: {e}", exc_info=True)


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
        self.worker_processes: Dict[str, asyncio.subprocess.Process] = {}
        self.validation_processes: Dict[str, asyncio.subprocess.Process] = {}
        self.heartbeat_task: asyncio.Task = None
        self.shutdown_event = asyncio.Event()
        # Флаг: все validation workers упали (проблемы с AI API)
        # Когда True: GPU выключен, воркеры не перезапускаются
        self.validation_workers_disabled = False

    async def create_catalog_tasks_from_new_articulums(self):
        """Создает catalog_tasks для всех артикулов в состоянии NEW"""
        # Создаем задачи батчем в одной транзакции для производительности
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Получаем NEW артикулы
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

                # Создаем catalog_tasks батчем
                # ВАЖНО: НЕ переводим артикулы в CATALOG_PARSING!
                # Переход произойдет когда воркер возьмет задачу в acquire_catalog_task()
                await conn.executemany("""
                    INSERT INTO catalog_tasks (articulum_id, status)
                    VALUES ($1, $2)
                """, [(a['id'], TaskStatus.PENDING) for a in new_articulums])

                logger.info(f"Создано {len(new_articulums)} catalog_tasks")

    async def create_object_tasks_from_validated_articulums(self):
        """
        Создает object_tasks для всех артикулов в состоянии VALIDATED.
        Создаются задачи только для объявлений, прошедших валидацию.
        """
        if SKIP_OBJECT_PARSING:
            logger.info("Парсинг объявлений отключен (SKIP_OBJECT_PARSING=true)")
            return

        async with self.pool.acquire() as conn:
            validated_articulums = await conn.fetch("""
                SELECT id, articulum
                FROM articulums
                WHERE state = $1
                ORDER BY created_at ASC
            """, ArticulumState.VALIDATED)

        if not validated_articulums:
            logger.info("Нет VALIDATED артикулов для создания object_tasks")
            return

        logger.info(f"Найдено {len(validated_articulums)} VALIDATED артикулов")

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

    async def create_object_tasks_for_reparse(self):
        """
        Создает object_tasks для повторного парсинга.
        Источник: объявления из object_data (уже спарсенные).
        Применяются фильтры (если заполнены) и проверка MIN_REPARSE_INTERVAL_HOURS.
        """
        if SKIP_OBJECT_PARSING:
            logger.info("Парсинг объявлений отключен (SKIP_OBJECT_PARSING=true)")
            return

        logger.info("Создание object_tasks для повторного парсинга...")

        try:
            async with self.pool.acquire() as conn:
                # Проверяем заполненность фильтров
                filters_exist = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT 1 FROM reparse_filter_items
                        UNION ALL
                        SELECT 1 FROM reparse_filter_articulums
                        LIMIT 1
                    )
                """)

                # Формируем список объявлений с учетом фильтров
                if filters_exist:
                    logger.info("Обнаружены фильтры, применяем фильтрацию...")
                    # Берем объявления из фильтров (UNION фильтра по ID и фильтра по артикулам)
                    target_items_query = """
                        WITH filter_items AS (
                            -- Фильтр по avito_item_id
                            SELECT avito_item_id FROM reparse_filter_items

                            UNION

                            -- Фильтр по артикулам (через catalog_listings)
                            SELECT DISTINCT cl.avito_item_id
                            FROM catalog_listings cl
                            INNER JOIN articulums a ON a.id = cl.articulum_id
                            INNER JOIN reparse_filter_articulums rfa ON rfa.articulum = a.articulum
                        )
                        SELECT fi.avito_item_id
                        FROM filter_items fi
                        WHERE EXISTS (
                            SELECT 1 FROM object_data od
                            WHERE od.avito_item_id = fi.avito_item_id
                        )
                    """
                else:
                    logger.info("Фильтры не заполнены, берем ВСЕ ранее спарсенные объявления")
                    # Берем ВСЕ спарсенные объявления
                    target_items_query = """
                        SELECT DISTINCT avito_item_id
                        FROM object_data
                    """

                # Создаем задачи с проверкой MIN_REPARSE_INTERVAL_HOURS
                created_count = await conn.fetchval(f"""
                    WITH target_items AS (
                        {target_items_query}
                    ),
                    latest_parses AS (
                        SELECT
                            od.avito_item_id,
                            od.articulum_id,
                            MAX(od.parsed_at) as last_parsed_at
                        FROM object_data od
                        INNER JOIN target_items ti ON ti.avito_item_id = od.avito_item_id
                        GROUP BY od.avito_item_id, od.articulum_id
                        HAVING (EXTRACT(EPOCH FROM (NOW() - MAX(od.parsed_at))) / 3600) >= $1
                    ),
                    new_tasks AS (
                        INSERT INTO object_tasks (articulum_id, avito_item_id, status)
                        SELECT DISTINCT ON (lp.avito_item_id)
                            lp.articulum_id,
                            lp.avito_item_id,
                            $2
                        FROM latest_parses lp
                        WHERE NOT EXISTS (
                            SELECT 1 FROM object_tasks ot
                            WHERE ot.avito_item_id = lp.avito_item_id
                              AND ot.status IN ($2, $3)
                        )
                        ORDER BY lp.avito_item_id, lp.last_parsed_at ASC
                        RETURNING 1
                    )
                    SELECT COUNT(*) FROM new_tasks
                """, MIN_REPARSE_INTERVAL_HOURS, TaskStatus.PENDING, TaskStatus.PROCESSING)

                # Статистика создания задач
                logger.info(f"""Статистика создания задач для повторного парсинга:
  - Фильтры: {'активны' if filters_exist else 'не используются'}
  - Создано задач: {created_count}
  - Минимальный интервал: {MIN_REPARSE_INTERVAL_HOURS} ч""")

                # Предупреждение при отсутствии задач
                if created_count == 0:
                    has_data = await conn.fetchval("SELECT EXISTS(SELECT 1 FROM object_data LIMIT 1)")

                    if not has_data:
                        logger.warning("Нет спарсенных объявлений в object_data для повторного парсинга")
                    else:
                        logger.warning(
                            f"Все объявления не прошли проверку MIN_REPARSE_INTERVAL_HOURS ({MIN_REPARSE_INTERVAL_HOURS}ч) "
                            "или имеют активные задачи"
                        )

        except Exception as e:
            logger.error(f"Ошибка при создании object_tasks для повторного парсинга: {e}", exc_info=True)
            raise

    async def spawn_browser_workers(self):
        """Запускает browser workers"""
        logger.info(f"Запуск {TOTAL_BROWSER_WORKERS} browser workers...")

        for local_worker_id in range(1, TOTAL_BROWSER_WORKERS + 1):
            # Генерируем глобально уникальный worker_id
            global_worker_id = f"{CONTAINER_ID}_{local_worker_id}"
            display = get_display_env(local_worker_id)

            # Формируем аргументы для subprocess
            args = [sys.executable, 'browser_worker.py', global_worker_id]
            if display:
                args.append(display)

            # Запускаем воркер как subprocess
            process = await asyncio.create_subprocess_exec(
                *args,
                stdout=None,  # Логи выводятся напрямую в консоль
                stderr=None,  # Ошибки выводятся напрямую в консоль
            )

            self.worker_processes[global_worker_id] = process
            logger.info(f"Запущен Worker#{global_worker_id} (PID={process.pid}, DISPLAY={display or 'headless'})")

        logger.info(f"Все {TOTAL_BROWSER_WORKERS} browser workers запущены")

    async def spawn_validation_workers(self):
        """Запускает validation workers (БЕЗ браузера, БЕЗ Xvfb)"""
        if TOTAL_VALIDATION_WORKERS == 0:
            logger.info("Validation Workers отключены (TOTAL_VALIDATION_WORKERS=0)")
            return

        logger.info(f"Запуск {TOTAL_VALIDATION_WORKERS} validation workers...")

        for local_worker_id in range(1, TOTAL_VALIDATION_WORKERS + 1):
            # Генерируем глобально уникальный worker_id
            global_worker_id = f"{CONTAINER_ID}_V{local_worker_id}"

            # Validation Workers НЕ используют DISPLAY
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                'validation_worker.py',
                global_worker_id,
                stdout=None,
                stderr=None,
            )

            self.validation_processes[global_worker_id] = process
            logger.info(f"Запущен ValidationWorker#{global_worker_id} (PID={process.pid})")

        logger.info(f"Все {TOTAL_VALIDATION_WORKERS} validation workers запущены")

    async def monitor_workers(self):
        """Мониторинг воркеров и перезапуск при падении"""
        logger.info("Запущен мониторинг воркеров...")

        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(10)

                # Проверяем Browser Workers
                for worker_id, process in list(self.worker_processes.items()):
                    if process.returncode is not None:
                        logger.warning(f"BrowserWorker#{worker_id} завершен (код={process.returncode})")

                        # Освобождаем ресурсы зависшего воркера
                        try:
                            async with self.pool.acquire() as conn:
                                # Освобождаем прокси
                                await conn.execute("""
                                    UPDATE proxies
                                    SET is_in_use = FALSE,
                                        worker_id = NULL,
                                        updated_at = NOW()
                                    WHERE worker_id = $1
                                """, worker_id)

                                # Возвращаем catalog_tasks в очередь
                                catalog_tasks = await conn.fetch("""
                                    UPDATE catalog_tasks
                                    SET status = $1,
                                        worker_id = NULL,
                                        updated_at = NOW()
                                    WHERE worker_id = $2 AND status = $3
                                    RETURNING id
                                """, TaskStatus.PENDING, worker_id, TaskStatus.PROCESSING)

                                # Возвращаем object_tasks в очередь
                                object_tasks = await conn.fetch("""
                                    UPDATE object_tasks
                                    SET status = $1,
                                        worker_id = NULL,
                                        updated_at = NOW()
                                    WHERE worker_id = $2 AND status = $3
                                    RETURNING id
                                """, TaskStatus.PENDING, worker_id, TaskStatus.PROCESSING)

                                logger.info(
                                    f"Освобождены ресурсы Worker#{worker_id}: "
                                    f"catalog_tasks={len(catalog_tasks)}, "
                                    f"object_tasks={len(object_tasks)}"
                                )
                        except Exception as e:
                            logger.error(f"Ошибка при освобождении ресурсов Worker#{worker_id}: {e}")

                        # Перезапускаем воркер (только если не идет shutdown)
                        if not self.shutdown_event.is_set():
                            # Извлекаем local_worker_id из global_worker_id
                            local_worker_id = int(worker_id.split('_')[-1])
                            display = get_display_env(local_worker_id)
                            args = [sys.executable, 'browser_worker.py', worker_id]
                            if display:
                                args.append(display)

                            new_process = await asyncio.create_subprocess_exec(
                                *args,
                                stdout=None,
                                stderr=None,
                            )

                            self.worker_processes[worker_id] = new_process
                            logger.info(f"BrowserWorker#{worker_id} перезапущен (PID={new_process.pid})")
                        else:
                            logger.info(f"BrowserWorker#{worker_id} не перезапускается (идет shutdown)")

                # Проверяем Validation Workers
                if not self.validation_workers_disabled:
                    api_error_workers = 0  # Счетчик воркеров с кодом 2
                    total_workers = len(self.validation_processes)

                    for worker_id, process in list(self.validation_processes.items()):
                        if process.returncode is not None:
                            exit_code = process.returncode
                            logger.warning(f"ValidationWorker#{worker_id} завершен (код={exit_code})")

                            if exit_code == 2:
                                # Код 2 = проблема с AI API → НЕ перезапускаем
                                logger.error(f"ValidationWorker#{worker_id}: код=2 (проблема с API) — НЕ перезапускаем")
                                api_error_workers += 1
                            elif not self.shutdown_event.is_set():
                                # Другой код → перезапускаем
                                new_process = await asyncio.create_subprocess_exec(
                                    sys.executable,
                                    'validation_worker.py',
                                    worker_id,
                                    stdout=None,
                                    stderr=None,
                                )
                                self.validation_processes[worker_id] = new_process
                                logger.info(f"ValidationWorker#{worker_id} перезапущен (PID={new_process.pid})")

                    # Проверяем: ВСЕ ли validation workers упали с кодом 2?
                    if api_error_workers == total_workers and total_workers > 0:
                        # ВСЕ воркеры упали с кодом 2 — проблема с AI API
                        logger.error("=" * 60)
                        logger.error("ВСЕ Validation Workers упали с кодом 2 — проблема с AI API!")
                        logger.error("Выключаем GPU...")
                        logger.error("=" * 60)

                        await stop_hf_endpoint()
                        self.validation_workers_disabled = True

                        logger.warning("GPU выключен. Browser Workers продолжают работу.")
                        logger.warning("Для возобновления валидации — перезапустите контейнер.")

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

        # Останавливаем Browser Workers
        logger.info("Остановка browser workers...")
        for worker_id, process in self.worker_processes.items():
            if process.returncode is None:
                # Процесс еще работает, нужно остановить
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=10)
                    logger.info(f"BrowserWorker#{worker_id} остановлен")
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                    logger.warning(f"BrowserWorker#{worker_id} убит (SIGKILL)")
            else:
                # Процесс уже завершен
                logger.info(f"BrowserWorker#{worker_id} уже завершен (код={process.returncode})")

        # Останавливаем Validation Workers
        logger.info("Остановка validation workers...")
        for worker_id, process in self.validation_processes.items():
            if process.returncode is None:
                # Процесс еще работает, нужно остановить
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=10)
                    logger.info(f"ValidationWorker#{worker_id} остановлен")
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                    logger.warning(f"ValidationWorker#{worker_id} убит (SIGKILL)")
            else:
                # Процесс уже завершен
                logger.info(f"ValidationWorker#{worker_id} уже завершен (код={process.returncode})")

        # Закрываем пул БД
        if self.pool:
            await self.pool.close()
            logger.info("Пул БД закрыт")

        # Останавливаем Xvfb дисплеи
        cleanup_displays()

        # Останавливаем HuggingFace Endpoint (для экономии)
        await stop_hf_endpoint()

        logger.info("Shutdown завершен")

    async def run(self):
        """Главная функция запуска системы"""
        try:
            # Базовая инициализация (Xvfb, БД)
            logger.info("=" * 60)
            logger.info("Инициализация системы...")
            logger.info("=" * 60)

            # Запуск HuggingFace Endpoint (если используется)
            if AI_PROVIDER == 'huggingface' and ENABLE_AI_VALIDATION:
                logger.info("Этап 0: Запуск HuggingFace Inference Endpoint...")
                endpoint_url = await start_hf_endpoint()
                if endpoint_url:
                    # Устанавливаем URL в переменную окружения для воркеров
                    import os
                    os.environ['HF_ENDPOINT_URL'] = endpoint_url
                    logger.info(f"✓ HF Endpoint URL установлен: {endpoint_url}")
                else:
                    logger.warning("✗ HF Endpoint не запущен, ИИ-валидация будет недоступна")

            logger.info("Этап 1/3: Создание виртуальных дисплеев Xvfb...")
            try:
                init_xvfb_displays()
                logger.info("✓ Виртуальные дисплеи успешно созданы")
            except Exception as e:
                logger.error(f"✗ Ошибка при создании Xvfb дисплеев: {e}", exc_info=True)
                raise

            logger.info("Этап 2/3: Подключение к базе данных...")
            try:
                self.pool = await create_pool()
                logger.info("✓ Подключение к БД установлено")
            except Exception as e:
                logger.error(f"✗ Ошибка при подключении к БД: {e}", exc_info=True)
                raise

            # Запускаем воркеры и heartbeat ДО создания задач
            # (воркеры будут ждать, пока задачи не появятся)
            logger.info("Этап 3/3: Запуск воркеров и heartbeat...")
            try:
                self.heartbeat_task = asyncio.create_task(heartbeat_check_loop(self.pool))
                await self.spawn_browser_workers()
                await self.spawn_validation_workers()
                logger.info("✓ Все воркеры успешно запущены")
            except Exception as e:
                logger.error(f"✗ Ошибка при запуске воркеров: {e}", exc_info=True)
                raise

            logger.info("=" * 60)
            # Создаем задачи в зависимости от режима
            if REPARSE_MODE:
                logger.info("Режим работы: REPARSE_MODE (повторный парсинг)")
                # В режиме повторного парсинга - создаем задачи синхронно (все известны заранее)
                await self.create_object_tasks_for_reparse()
            else:
                logger.info("Режим работы: ОБЫЧНЫЙ (новые артикулы)")
                # В обычном режиме - создаем catalog_tasks и object_tasks из валидированных
                asyncio.create_task(self.create_catalog_tasks_from_new_articulums())
                asyncio.create_task(self.create_object_tasks_from_validated_articulums())

            logger.info("=" * 60)
            logger.info("✓ СИСТЕМА УСПЕШНО ИНИЦИАЛИЗИРОВАНА И ЗАПУЩЕНА")
            logger.info("=" * 60)

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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful shutdown - не показываем traceback
        logger.info("Главный процесс остановлен пользователем")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logger.error(f"Критическая ошибка главного процесса: {e}", exc_info=True)
        sys.exit(1)
