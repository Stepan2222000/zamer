"""Browser Worker - воркер парсинга каталогов"""

import asyncio
import logging
import os
import sys
import platform
from typing import Optional

import asyncpg
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from avito_library.parsers.catalog_parser import (
    wait_for_page_request,
    supply_page,
    CatalogParseStatus,
)
from avito_library.parsers.card_parser import parse_card, CardParsingError
from avito_library.detectors import detect_page_state

from config import HEARTBEAT_UPDATE_INTERVAL, OBJECT_FIELDS, OBJECT_INCLUDE_HTML
from database import create_pool
from proxy_manager import acquire_proxy_with_wait, block_proxy, release_proxy
from catalog_task_manager import (
    acquire_catalog_task,
    complete_catalog_task,
    fail_catalog_task,
    return_catalog_task_to_queue,
    update_catalog_task_heartbeat,
    update_catalog_task_checkpoint,
)
from object_task_manager import (
    acquire_object_task,
    complete_object_task,
    fail_object_task,
    invalidate_object_task,
    return_object_task_to_queue,
    update_object_task_heartbeat,
)
from catalog_parser import parse_catalog_for_articulum, save_listings_to_db
from object_parser import save_object_data_to_db
from detector_handler import handle_detector_state, DetectorContext
from state_machine import transition_to_object_parsing

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class BrowserWorker:
    """Воркер для парсинга каталогов и объявлений"""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.logger = logging.getLogger(f'Worker#{worker_id}')
        self.pool: Optional[asyncpg.Pool] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.current_proxy_id: Optional[int] = None
        self.playwright = None
        self.current_mode: Optional[str] = None  # 'catalog' или 'object'

        # Флаги для остановки фоновых задач
        self.stop_heartbeat = False
        self.stop_page_provider = False

    async def init(self):
        """Инициализация воркера"""
        self.logger.info("Инициализация...")

        # Подключение к БД
        self.pool = await create_pool()
        self.logger.info("Подключен к БД")

        # Запуск Playwright
        self.playwright = await async_playwright().start()
        self.logger.info("Playwright запущен")

    async def create_browser_with_proxy(self):
        """Создает браузер с прокси"""
        async with self.pool.acquire() as conn:
            # Получаем прокси (ждем если нет свободных)
            proxy = await acquire_proxy_with_wait(conn, self.worker_id)
            self.current_proxy_id = proxy['id']

            # Формируем конфиг прокси для Playwright
            proxy_config = {
                'server': f"http://{proxy['host']}:{proxy['port']}",
            }

            if proxy['username']:
                proxy_config['username'] = proxy['username']
                proxy_config['password'] = proxy['password']

            # Создаем браузер
            self.browser = await self.playwright.chromium.launch(
                headless=False,  # используем Xvfb
                proxy=proxy_config,
            )

            # Создаем контекст
            self.context = await self.browser.new_context()

            # Создаем страницу
            self.page = await self.context.new_page()

            self.logger.info(f"Браузер создан с прокси {proxy['host']}:{proxy['port']}")

    async def recreate_page_with_new_proxy(self):
        """Пересоздает страницу с новым прокси"""
        # Закрываем старый контекст и страницу
        if self.context:
            await self.context.close()
        if self.page:
            await self.page.close()

        # Получаем новый прокси
        async with self.pool.acquire() as conn:
            proxy = await acquire_proxy_with_wait(conn, self.worker_id)
            self.current_proxy_id = proxy['id']

            # Формируем конфиг прокси
            proxy_config = {
                'server': f"http://{proxy['host']}:{proxy['port']}",
            }

            if proxy['username']:
                proxy_config['username'] = proxy['username']
                proxy_config['password'] = proxy['password']

            # ВАЖНО: пересоздаем браузер целиком (согласно CLAUDE.md)
            if self.browser:
                await self.browser.close()

            self.browser = await self.playwright.chromium.launch(
                headless=False,
                proxy=proxy_config,
            )

            # Создаем новый контекст и страницу
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()

            self.logger.info(f"Создана новая страница с прокси {proxy['host']}:{proxy['port']}")

    async def update_heartbeat_loop(self, task_id: int, task_type: str):
        """Фоновая задача обновления heartbeat"""
        self.stop_heartbeat = False

        while not self.stop_heartbeat:
            try:
                await asyncio.sleep(HEARTBEAT_UPDATE_INTERVAL)

                async with self.pool.acquire() as conn:
                    if task_type == 'catalog':
                        await update_catalog_task_heartbeat(conn, task_id)
                    elif task_type == 'object':
                        await update_object_task_heartbeat(conn, task_id)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Ошибка обновления heartbeat: {e}")

    async def page_provider_loop(self, task_id: int):
        """Фоновая задача обработки запросов новых страниц от parse_catalog_until_complete"""
        self.stop_page_provider = False

        while not self.stop_page_provider:
            try:
                # Ждем запрос от библиотеки
                request = await wait_for_page_request()

                self.logger.info(f"PageRequest: attempt={request.attempt}, "
                                f"status={request.status.value}, next_page={request.next_start_page}")

                # Обновляем чекпоинт
                async with self.pool.acquire() as conn:
                    await update_catalog_task_checkpoint(conn, task_id, request.next_start_page)

                # Если прокси заблокирован - блокируем его и получаем новый
                if request.status in {
                    CatalogParseStatus.PROXY_BLOCKED,
                    CatalogParseStatus.PROXY_AUTH_REQUIRED,
                }:
                    self.logger.warning("Прокси заблокирован, меняем...")

                    async with self.pool.acquire() as conn:
                        await block_proxy(conn, self.current_proxy_id)

                    # Пересоздаем страницу с новым прокси
                    await self.recreate_page_with_new_proxy()

                # Отдаем новую страницу парсеру
                supply_page(self.page)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Ошибка в page_provider: {e}")
                break

    async def handle_parse_result(self, task: dict, listings, meta):
        """Обработка результата парсинга"""
        task_id = task['id']
        articulum_id = task['articulum_id']
        articulum = task['articulum']

        self.logger.info(f"Результат парсинга '{articulum}': status={meta.status.value}, "
                        f"pages={meta.processed_pages}, cards={meta.processed_cards}")

        async with self.pool.acquire() as conn:
            if meta.status == CatalogParseStatus.SUCCESS:
                # Успех - сохраняем объявления
                saved_count = await save_listings_to_db(conn, articulum_id, listings)
                self.logger.info(f"Сохранено {saved_count} объявлений")

                # Завершаем задачу (переводит артикул в CATALOG_PARSED)
                await complete_catalog_task(conn, task_id, articulum_id)

            elif meta.status == CatalogParseStatus.EMPTY:
                # Пустой каталог - сохраняем 0 объявлений, но завершаем задачу
                self.logger.info("Каталог пуст (0 объявлений)")
                await complete_catalog_task(conn, task_id, articulum_id)

            elif meta.status in {CatalogParseStatus.PROXY_BLOCKED, CatalogParseStatus.PROXY_AUTH_REQUIRED}:
                # Прокси заблокирован - блокируем его и возвращаем задачу
                self.logger.error(f"Прокси заблокирован: {meta.status.value}")
                await block_proxy(conn, self.current_proxy_id, f"Catalog parsing: {meta.status.value}")
                await return_catalog_task_to_queue(conn, task_id)

            elif meta.status == CatalogParseStatus.CAPTCHA_UNSOLVED:
                # Капча не решилась - возвращаем задачу и прокси в очередь
                self.logger.warning("Капча не решилась, возвращаем задачу")
                await return_catalog_task_to_queue(conn, task_id)
                await release_proxy(conn, self.current_proxy_id)

            elif meta.status == CatalogParseStatus.NOT_DETECTED:
                # Неопределенное состояние - помечаем как failed
                self.logger.error(f"NOT_DETECTED - помечаем как failed: {meta.details}")
                await fail_catalog_task(conn, task_id, f"NOT_DETECTED: {meta.details}")

            else:
                # Прочие ошибки - возвращаем в очередь
                self.logger.warning(f"Неожиданная ошибка {meta.status.value}, возвращаем в очередь")
                await return_catalog_task_to_queue(conn, task_id)

    async def process_catalog_task(self, task: dict):
        """Обработка одной catalog_task"""
        task_id = task['id']
        articulum = task['articulum']
        checkpoint_page = task['checkpoint_page']

        self.logger.info(f"Обработка задачи #{task_id}: артикул='{articulum}', checkpoint={checkpoint_page}")

        # Запускаем фоновые задачи
        heartbeat_task = asyncio.create_task(self.update_heartbeat_loop(task_id, 'catalog'))
        page_provider_task = asyncio.create_task(self.page_provider_loop(task_id))

        try:
            # Парсим каталог
            listings, meta = await parse_catalog_for_articulum(
                self.page,
                articulum,
                start_page=checkpoint_page
            )

            # Обрабатываем результат
            await self.handle_parse_result(task, listings, meta)

        except Exception as e:
            self.logger.error(f"Ошибка при обработке задачи #{task_id}: {e}", exc_info=True)

            # Возвращаем задачу в очередь
            async with self.pool.acquire() as conn:
                await return_catalog_task_to_queue(conn, task_id)

        finally:
            # Останавливаем фоновые задачи
            self.stop_heartbeat = True
            self.stop_page_provider = True

            heartbeat_task.cancel()
            page_provider_task.cancel()

            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

            try:
                await page_provider_task
            except asyncio.CancelledError:
                pass

    async def process_object_task(self, task: dict):
        """Обработка одной object_task"""
        task_id = task['id']
        avito_item_id = task['avito_item_id']
        articulum_id = task['articulum_id']
        articulum = task['articulum']

        self.logger.info(f"Обработка задачи #{task_id}: объявление={avito_item_id}")

        # Запускаем heartbeat
        heartbeat_task = asyncio.create_task(self.update_heartbeat_loop(task_id, 'object'))

        # Переводим артикул в OBJECT_PARSING при взятии первой задачи (идемпотентно)
        async with self.pool.acquire() as conn:
            await transition_to_object_parsing(conn, articulum_id)

        # Флаг для отслеживания необходимости смены прокси
        need_proxy_recreate = False

        try:
            # Переходим на страницу объявления
            url = f"https://www.avito.ru/{avito_item_id}"
            response = await self.page.goto(url, wait_until="domcontentloaded")

            # Детекция состояния страницы
            state = await detect_page_state(self.page, last_response=response)

            # Подготовка контекста для обработчика детекторов
            context = DetectorContext(
                page=self.page,
                proxy_id=self.current_proxy_id,
                task_id=task_id,
                worker_id=self.worker_id,
                task_type='object'
            )

            # Обработка детектора
            result = await handle_detector_state(state, context)

            async with self.pool.acquire() as conn:
                if result['action'] == 'continue':
                    # Успех - парсим карточку
                    try:
                        html = await self.page.content()
                        card_data = parse_card(
                            html,
                            fields=OBJECT_FIELDS,
                            ensure_card=True,
                            include_html=OBJECT_INCLUDE_HTML
                        )

                        # Сохраняем данные в БД
                        await save_object_data_to_db(conn, articulum_id, avito_item_id, card_data, html)

                        # Завершаем задачу
                        await complete_object_task(conn, task_id)

                        self.logger.info(f"Объявление {avito_item_id} успешно спарсено")

                    except CardParsingError as e:
                        # HTML не является карточкой - помечаем как failed
                        self.logger.error(f"Ошибка парсинга карточки {avito_item_id}: {e}")
                        await fail_object_task(conn, task_id, f"CardParsingError: {str(e)}")

                elif result['action'] == 'block_proxy':
                    # Блокируем прокси и возвращаем задачу в очередь
                    await block_proxy(conn, self.current_proxy_id, result.get('reason'))
                    self.logger.warning(f"Прокси заблокирован: {result.get('reason')}")

                    # Возвращаем задачу в очередь
                    await return_object_task_to_queue(conn, task_id)

                    # Помечаем, что нужно пересоздать браузер после закрытия conn
                    need_proxy_recreate = True

                elif result['action'] == 'return_task_and_proxy':
                    # Возвращаем задачу и прокси в очередь
                    await return_object_task_to_queue(conn, task_id)
                    await release_proxy(conn, self.current_proxy_id)
                    self.logger.info(f"Задача и прокси возвращены в очередь: {result.get('reason')}")

                elif result['action'] == 'mark_invalid':
                    # REMOVED_DETECTOR_ID - объявление удалено
                    await invalidate_object_task(conn, task_id, result.get('reason'))
                    self.logger.info(f"Объявление {avito_item_id} помечено как invalid: {result.get('reason')}")

                elif result['action'] == 'mark_failed':
                    # NOT_DETECTED_STATE_ID
                    await fail_object_task(conn, task_id, result.get('reason'))
                    self.logger.error(f"Задача помечена как failed: {result.get('reason')}")

            # Пересоздаем браузер с новым прокси ВНЕ блока conn (избегаем deadlock)
            if need_proxy_recreate:
                await self.recreate_page_with_new_proxy()

        except Exception as e:
            self.logger.error(f"Ошибка при обработке object_task #{task_id}: {e}", exc_info=True)

            # Возвращаем задачу в очередь
            async with self.pool.acquire() as conn:
                await return_object_task_to_queue(conn, task_id)

        finally:
            # Останавливаем heartbeat
            self.stop_heartbeat = True
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def main_loop(self):
        """Главный цикл воркера с динамическим переключением между типами задач"""
        self.logger.info("Запуск главного цикла...")

        while True:
            try:
                async with self.pool.acquire() as conn:
                    # ПРИОРИТЕТ 1: пробуем взять catalog_task
                    task = await acquire_catalog_task(conn, self.worker_id)

                    if task:
                        # Создаем браузер при первой задаче (ленивая инициализация)
                        if not self.browser:
                            await self.create_browser_with_proxy()

                        self.current_mode = 'catalog'
                        await self.process_catalog_task(task)
                        continue

                    # ПРИОРИТЕТ 2: пробуем взять object_task
                    task = await acquire_object_task(conn, self.worker_id)

                    if task:
                        # Создаем браузер при первой задаче (ленивая инициализация)
                        if not self.browser:
                            await self.create_browser_with_proxy()

                        self.current_mode = 'object'
                        await self.process_object_task(task)
                        continue

                # Нет задач обоих типов - ждем
                self.logger.debug("Нет доступных задач, ожидание...")
                await asyncio.sleep(5)

            except KeyboardInterrupt:
                self.logger.info("Получен сигнал остановки")
                break
            except Exception as e:
                self.logger.error(f"Ошибка в главном цикле: {e}", exc_info=True)
                await asyncio.sleep(5)
                continue

    async def cleanup(self):
        """Очистка ресурсов"""
        self.logger.info("Очистка ресурсов...")

        if self.context:
            await self.context.close()

        if self.browser:
            await self.browser.close()

        if self.playwright:
            await self.playwright.stop()

        # Освобождаем прокси
        if self.current_proxy_id and self.pool:
            async with self.pool.acquire() as conn:
                await release_proxy(conn, self.current_proxy_id, self.worker_id)

        if self.pool:
            await self.pool.close()

        self.logger.info("Завершен")

    async def run(self):
        """Запуск воркера"""
        try:
            await self.init()
            await self.main_loop()
        finally:
            await self.cleanup()


async def main():
    """Точка входа воркера"""
    if len(sys.argv) < 2:
        logging.error("Usage: python browser_worker.py <worker_id> [display]")
        sys.exit(1)

    worker_id = int(sys.argv[1])

    # Устанавливаем DISPLAY из аргумента (если передан)
    if len(sys.argv) >= 3:
        display = sys.argv[2]
        os.environ['DISPLAY'] = display
        logging.info(f"Worker#{worker_id}: DISPLAY установлен: {display}")

    worker = BrowserWorker(worker_id)
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
