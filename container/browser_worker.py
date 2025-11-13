"""Browser Worker - воркер парсинга каталогов"""

import asyncio
import logging
import os
from socket import timeout
import sys
import platform
import time
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

from config import (
    HEARTBEAT_UPDATE_INTERVAL,
    OBJECT_FIELDS,
    OBJECT_INCLUDE_HTML,
    SKIP_OBJECT_PARSING,
    REPARSE_MODE,
    SERVER_ERROR_RETRY_ATTEMPTS,
    SERVER_ERROR_RETRY_DELAY,
    CATALOG_BUFFER_SIZE,
    ArticulumState,
    TaskStatus,
)
from database import create_pool
from proxy_manager import acquire_proxy_with_wait, block_proxy, release_proxy, increment_proxy_error, reset_proxy_error_counter
from network_error_handler import (
    is_transient_network_error,
    is_permanent_proxy_error,
    get_error_description,
)
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
from detector_handler import handle_detector_state, DetectorContext, enhanced_detect_page_state
from state_machine import transition_to_object_parsing, StateTransitionError
from server_error_detector import is_server_error

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class BrowserWorker:
    """Воркер для парсинга каталогов и объявлений"""

    def __init__(self, worker_id: str):
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

    def _is_used_condition(self, characteristics: dict) -> bool:
        """
        Проверяет, содержат ли характеристики состояние "б/у".

        Args:
            characteristics: Словарь характеристик из card_data

        Returns:
            True если найдено состояние "б/у", иначе False
        """
        if not characteristics or not isinstance(characteristics, dict):
            return False

        # Список вариантов написания "б/у" (независимо от регистра)
        used_condition_variants = [
            'б/у', 'бу', 'б у', 'б.у.', 'б.у',
            'б/у.', 'б./у.', 'б./у', 'б /у',
        ]

        # Проверяем все ключи, которые могут означать "состояние"
        condition_keys = ['состояние', 'condition', 'статус', 'status']

        for key, value in characteristics.items():
            key_lower = key.lower()

            # Проверяем только ключи, связанные с состоянием
            if any(cond_key in key_lower for cond_key in condition_keys):
                if value:
                    value_lower = str(value).lower().strip()

                    # Проверяем на наличие любого варианта "б/у"
                    if any(variant in value_lower for variant in used_condition_variants):
                        return True

        return False

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
        CLOSE_TIMEOUT = 10  # секунд

        # Сохраняем ссылку на старый браузер для корректного закрытия
        old_browser = self.browser
        old_proxy_id = self.current_proxy_id

        # КРИТИЧЕСКИ ВАЖНО: Сначала освобождаем старый прокси
        if old_proxy_id:
            async with self.pool.acquire() as conn:
                await release_proxy(conn, old_proxy_id)
                self.logger.info(f"Освобожден старый прокси #{old_proxy_id}")

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

            # Создаем НОВЫЙ браузер ДО закрытия старого
            # Это предотвращает EPIPE ошибки при закрытии
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                proxy=proxy_config,
            )

            # Создаем новый контекст и страницу
            self.context = await self.browser.new_context()
            self.page = await self.context.new_page()

            self.logger.info(f"Создана новая страница с прокси {proxy['host']}:{proxy['port']}")

        # Закрываем старый браузер ПОСЛЕ создания нового
        # Даем небольшую задержку для обработки pending events
        if old_browser:
            try:
                await asyncio.sleep(0.5)  # Даем время обработать события
                await asyncio.wait_for(old_browser.close(), timeout=CLOSE_TIMEOUT)
            except asyncio.TimeoutError:
                self.logger.warning("old browser.close() TIMEOUT - оставляем на GC")
            except Exception as e:
                # Игнорируем все ошибки при закрытии старого браузера
                # включая EPIPE, BrokenPipeError и т.д.
                self.logger.debug(f"Ошибка при закрытии старого браузера (игнорируется): {e}")

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
                # Успех - сохраняем объявления и завершаем задачу атомарно
                try:
                    async with conn.transaction():
                        saved_count = await save_listings_to_db(conn, articulum_id, listings)
                        self.logger.info(f"Сохранено {saved_count} объявлений")

                        # Завершаем задачу (переводит артикул в CATALOG_PARSED)
                        await complete_catalog_task(conn, task_id, articulum_id)

                        # Сбрасываем счетчик ошибок прокси после успешного выполнения
                        await reset_proxy_error_counter(conn, self.current_proxy_id)

                except StateTransitionError as e:
                    # Критическая ошибка: артикул не в ожидаемом состоянии
                    # Транзакция откачена автоматически
                    self.logger.error(f"Ошибка перехода состояния: {e}")
                    # Задача вернется в очередь через heartbeat timeout
                    return

            elif meta.status == CatalogParseStatus.EMPTY:
                # Пустой каталог - сохраняем 0 объявлений, но завершаем задачу
                self.logger.info("Каталог пуст (0 объявлений)")
                try:
                    async with conn.transaction():
                        await complete_catalog_task(conn, task_id, articulum_id)

                        # Сбрасываем счетчик ошибок прокси после успешного выполнения
                        await reset_proxy_error_counter(conn, self.current_proxy_id)

                except StateTransitionError as e:
                    # Критическая ошибка: артикул не в ожидаемом состоянии
                    # Транзакция откачена автоматически
                    self.logger.error(f"Ошибка перехода состояния: {e}")
                    # Задача вернется в очередь через heartbeat timeout
                    return

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

        # Флаг успешной обработки (для race condition protection)
        task_completed = False

        try:
            # Парсим каталог
            listings, meta = await parse_catalog_for_articulum(
                self.page,
                articulum,
                start_page=checkpoint_page
            )

            # Обрабатываем результат
            await self.handle_parse_result(task, listings, meta)

            # ВАЖНО: handle_parse_result сам решает судьбу задачи
            # (complete, fail, invalid, return to queue)
            # Поэтому мы НЕ возвращаем задачу в finally
            task_completed = True

        except asyncio.CancelledError:
            # Получен сигнал остановки (Ctrl+C или shutdown)
            self.logger.info(f"Задача #{task_id} отменена (shutdown)")
            raise  # Пробрасываем для корректной отмены

        except Exception as e:
            # Специальная обработка network errors
            if is_permanent_proxy_error(e):
                # Постоянная проблема прокси - блокируем и пересоздаем браузер
                self.logger.error(
                    f"Worker#{self.worker_id} - catalog_task #{task_id} - "
                    f"PERMANENT ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {get_error_description(e)}"
                )
                async with self.pool.acquire() as conn:
                    await block_proxy(conn, self.current_proxy_id, f"Permanent error: {get_error_description(e)}")
                # Браузер будет пересоздан при следующей задаче (в main_loop)
                # task_completed остается False - задача вернется в очередь в finally

            elif is_transient_network_error(e):
                # Временная сетевая ошибка - увеличиваем счетчик ошибок прокси
                self.logger.warning(
                    f"Worker#{self.worker_id} - catalog_task #{task_id} - "
                    f"TRANSIENT ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {get_error_description(e)}"
                )
                async with self.pool.acquire() as conn:
                    await increment_proxy_error(conn, self.current_proxy_id, get_error_description(e))
                # Браузер будет пересоздан при следующей задаче
                # task_completed остается False - задача вернется в очередь в finally

            else:
                # Неизвестная ошибка - логируем для анализа
                error_msg = str(e)[:500]  # Ограничиваем длину для читаемости
                self.logger.error(
                    f"Worker#{self.worker_id} - catalog_task #{task_id} - "
                    f"UNKNOWN ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {error_msg}",
                    exc_info=True
                )

        finally:
            # 1. Останавливаем фоновые задачи
            self.stop_heartbeat = True
            self.stop_page_provider = True

            heartbeat_task.cancel()
            page_provider_task.cancel()

            # 2. Ждем завершения фоновых задач с timeout
            try:
                await asyncio.wait_for(heartbeat_task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

            try:
                await asyncio.wait_for(page_provider_task, timeout=15)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                self.logger.warning("page_provider_task не завершился за 15 сек")

            # 3. ТОЛЬКО ПОСЛЕ cleanup возвращаем задачу (если не была обработана)
            if not task_completed:
                try:
                    async with self.pool.acquire() as conn:
                        await return_catalog_task_to_queue(conn, task_id)
                        self.logger.info(f"Задача #{task_id} возвращена в очередь после ошибки")
                except Exception as e:
                    self.logger.error(f"Ошибка при возврате задачи в очередь: {e}")

    async def process_object_task(self, task: dict):
        """Обработка одной object_task"""
        task_id = task['id']
        avito_item_id = task['avito_item_id']
        articulum_id = task['articulum_id']
        articulum = task['articulum']

        self.logger.info(f"Обработка задачи #{task_id}: объявление={avito_item_id}")

        # Запускаем heartbeat
        heartbeat_task = asyncio.create_task(self.update_heartbeat_loop(task_id, 'object'))

        # Переводим артикул в OBJECT_PARSING (только в обычном режиме)
        if not REPARSE_MODE:
            async with self.pool.acquire() as conn:
                await transition_to_object_parsing(conn, articulum_id)

        # Флаг для управления задачей
        task_completed = False

        try:
            # Переходим на страницу объявления
            url = f"https://www.avito.ru/{avito_item_id}"
            response = await self.page.goto(url, wait_until="domcontentloaded", timeout=150000)


            # Детекция состояния страницы (с проверкой server errors)
            state = await enhanced_detect_page_state(self.page, last_response=response)

            # Retry механизм для server errors (502/503/504)
            if is_server_error(state):
                self.logger.warning(f"Server error обнаружен: {state}, начинаем retry")

                for attempt in range(SERVER_ERROR_RETRY_ATTEMPTS):
                    self.logger.info(
                        f"Server error retry попытка {attempt + 1}/{SERVER_ERROR_RETRY_ATTEMPTS} "
                        f"после {SERVER_ERROR_RETRY_DELAY}s задержки"
                    )
                    await asyncio.sleep(SERVER_ERROR_RETRY_DELAY)

                    try:
                        # Перезагружаем страницу
                        response = await self.page.reload(wait_until="domcontentloaded")

                        # Проверяем состояние после reload
                        state = await enhanced_detect_page_state(self.page, last_response=response)

                        if not is_server_error(state):
                            self.logger.info(f"Server error устранен после {attempt + 1} попытки(ок)")
                            break
                        else:
                            self.logger.warning(f"Server error {state} всё ещё присутствует")

                    except Exception as e:
                        self.logger.error(f"Ошибка при retry #{attempt + 1}: {e}")
                        # Продолжаем попытки даже при ошибке reload
                        continue

                # После всех попыток логируем итоговое состояние
                if is_server_error(state):
                    self.logger.warning(
                        f"Server error {state} не устранен после {SERVER_ERROR_RETRY_ATTEMPTS} попыток, "
                        f"будет смена прокси"
                    )

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

                        # Проверяем состояние "б/у" в характеристиках
                        if self._is_used_condition(card_data.characteristics):
                            rejection_reason = 'Найдено состояние "б/у" в характеристиках'
                            await invalidate_object_task(conn, task_id, rejection_reason)
                            self.logger.info(f"Объявление {avito_item_id} отклонено: {rejection_reason}")
                            task_completed = True
                        else:
                            # Сохраняем данные в БД
                            await save_object_data_to_db(conn, articulum_id, avito_item_id, card_data, html)

                            # Завершаем задачу
                            await complete_object_task(conn, task_id)

                            # Сбрасываем счетчик ошибок прокси после успешного выполнения
                            await reset_proxy_error_counter(conn, self.current_proxy_id)

                            self.logger.info(f"Объявление {avito_item_id} успешно спарсено")
                            task_completed = True

                    except CardParsingError as e:
                        # HTML не является карточкой - помечаем как failed
                        self.logger.error(f"Ошибка парсинга карточки {avito_item_id}: {e}")
                        await fail_object_task(conn, task_id, f"CardParsingError: {str(e)}")
                        task_completed = True

                elif result['action'] == 'block_proxy':
                    # Блокируем прокси и возвращаем задачу в очередь
                    await block_proxy(conn, self.current_proxy_id, result.get('reason'))
                    self.logger.warning(f"Прокси заблокирован: {result.get('reason')}")

                    # Возвращаем задачу в очередь
                    await return_object_task_to_queue(conn, task_id)
                    task_completed = True

                elif result['action'] == 'return_task_and_proxy':
                    # Возвращаем задачу и прокси в очередь
                    await return_object_task_to_queue(conn, task_id)
                    await release_proxy(conn, self.current_proxy_id)
                    self.logger.info(f"Задача и прокси возвращены в очередь: {result.get('reason')}")
                    task_completed = True

                elif result['action'] == 'mark_invalid':
                    # REMOVED_DETECTOR_ID - объявление удалено
                    await invalidate_object_task(conn, task_id, result.get('reason'))
                    if REPARSE_MODE:
                        # В режиме повторного парсинга - просто пропускаем удаленные объявления
                        self.logger.info(f"Объявление {avito_item_id} удалено, пропускаем (REPARSE_MODE)")
                    else:
                        self.logger.info(f"Объявление {avito_item_id} помечено как invalid: {result.get('reason')}")
                    task_completed = True

                elif result['action'] == 'mark_failed':
                    # NOT_DETECTED_STATE_ID
                    await fail_object_task(conn, task_id, result.get('reason'))
                    self.logger.error(f"Задача помечена как failed: {result.get('reason')}")
                    task_completed = True

                elif result['action'] == 'change_proxy_and_retry':
                    # ВРЕМЕННОЕ РЕШЕНИЕ для server errors (502/503/504)
                    # TODO: возможно в будущем изменить стратегию
                    self.logger.warning(
                        f"Server error обнаружен, меняем прокси и повторяем: {result.get('reason')}"
                    )
                    # Прокси возвращаем (НЕ блокируем - он рабочий!)
                    await release_proxy(conn, self.current_proxy_id)

            # Закрываем браузер ВНЕ блока conn если прокси заблокирован или server error (избегаем deadlock)
            if result.get('action') in {'block_proxy', 'change_proxy_and_retry'}:
                # Закрываем браузер с заблокированным прокси или при server error
                try:
                    if self.browser:
                        await asyncio.wait_for(self.browser.close(), timeout=10)
                        self.browser = None
                        self.context = None
                        self.page = None
                        self.current_proxy_id = None
                        self.logger.info(f"Браузер закрыт после {result.get('action')}")
                except Exception as e:
                    self.logger.warning(f"Ошибка при закрытии браузера: {e}")
                    # Обнуляем в любом случае
                    self.browser = None
                    self.context = None
                    self.page = None
                    self.current_proxy_id = None

                # При server error задача НЕ помечается completed - воркер возьмет новый прокси и повторит
                if result.get('action') == 'change_proxy_and_retry':
                    # Задача автоматически вернется в очередь в finally блоке
                    # т.к. task_completed остался False
                    pass

        except asyncio.CancelledError:
            # Получен сигнал остановки (Ctrl+C или shutdown)
            self.logger.info(f"Задача #{task_id} отменена (shutdown)")
            raise  # Пробрасываем для корректной отмены

        except Exception as e:
            # Специальная обработка network errors
            if is_permanent_proxy_error(e):
                # Постоянная проблема прокси - блокируем и пересоздаем браузер
                self.logger.error(
                    f"Worker#{self.worker_id} - object_task #{task_id} (item={avito_item_id}) - "
                    f"PERMANENT ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {get_error_description(e)}"
                )
                async with self.pool.acquire() as conn:
                    await block_proxy(conn, self.current_proxy_id, f"Permanent error: {get_error_description(e)}")
                # Браузер будет пересоздан при следующей задаче (в main_loop)
                # task_completed остается False - задача вернется в очередь в finally

            elif is_transient_network_error(e):
                # Временная сетевая ошибка - увеличиваем счетчик ошибок прокси
                self.logger.warning(
                    f"Worker#{self.worker_id} - object_task #{task_id} (item={avito_item_id}) - "
                    f"TRANSIENT ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {get_error_description(e)}"
                )
                async with self.pool.acquire() as conn:
                    await increment_proxy_error(conn, self.current_proxy_id, get_error_description(e))
                # Браузер будет пересоздан при следующей задаче
                # task_completed остается False - задача вернется в очередь в finally

            else:
                # Неизвестная ошибка - логируем для анализа
                error_msg = str(e)[:500]  # Ограничиваем длину для читаемости
                self.logger.error(
                    f"Worker#{self.worker_id} - object_task #{task_id} (item={avito_item_id}) - "
                    f"UNKNOWN ERROR on proxy #{self.current_proxy_id}: "
                    f"{type(e).__name__} - {error_msg}",
                    exc_info=True
                )

        finally:
            # 1. Останавливаем heartbeat
            self.stop_heartbeat = True
            heartbeat_task.cancel()

            try:
                await asyncio.wait_for(heartbeat_task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

            # 2. ТОЛЬКО ПОСЛЕ cleanup возвращаем задачу (если не была обработана)
            if not task_completed:
                try:
                    async with self.pool.acquire() as conn:
                        await return_object_task_to_queue(conn, task_id)
                        self.logger.info(f"Задача #{task_id} возвращена в очередь после ошибки")
                except Exception as e:
                    self.logger.error(f"Ошибка при возврате задачи в очередь: {e}")

    async def get_catalog_buffer_size(self, conn: asyncpg.Connection) -> int:
        """
        Подсчитывает размер буфера каталогов.

        Буфер = количество артикулов со спарсенными каталогами (VALIDATED),
        у которых есть pending object_tasks (готовы к парсингу объявлений).
        """
        buffer_size = await conn.fetchval("""
            SELECT COUNT(DISTINCT a.id)
            FROM articulums a
            WHERE a.state = $1
              AND EXISTS (
                  SELECT 1
                  FROM object_tasks ot
                  WHERE ot.articulum_id = a.id
                    AND ot.status = $2
              )
        """, ArticulumState.VALIDATED, TaskStatus.PENDING)

        return buffer_size or 0

    async def main_loop(self):
        """Главный цикл воркера с динамическим переключением между типами задач"""
        self.logger.info("Запуск главного цикла...")

        while True:
            try:
                async with self.pool.acquire() as conn:
                    # Проверяем размер буфера каталогов для определения приоритета
                    buffer_size = await self.get_catalog_buffer_size(conn)

                    # Динамический выбор приоритета:
                    # Если buffer < CATALOG_BUFFER_SIZE → приоритет каталогам (пополнение буфера)
                    # Если buffer >= CATALOG_BUFFER_SIZE → приоритет объявлениям (обработка буфера)

                    if buffer_size < CATALOG_BUFFER_SIZE:
                        # Буфер мал → сначала каталоги, потом объявления
                        if not REPARSE_MODE:
                            task = await acquire_catalog_task(conn, self.worker_id)
                            if task:
                                if not self.browser:
                                    await self.create_browser_with_proxy()
                                self.current_mode = 'catalog'
                                self.logger.debug(f"Буфер={buffer_size}/{CATALOG_BUFFER_SIZE} → парсим каталог")
                                await self.process_catalog_task(task)
                                continue

                        if not SKIP_OBJECT_PARSING:
                            task = await acquire_object_task(conn, self.worker_id)
                            if task:
                                if not self.browser:
                                    await self.create_browser_with_proxy()
                                self.current_mode = 'object'
                                await self.process_object_task(task)
                                continue
                    else:
                        # Буфер полон → сначала объявления, потом каталоги
                        if not SKIP_OBJECT_PARSING:
                            task = await acquire_object_task(conn, self.worker_id)
                            if task:
                                if not self.browser:
                                    await self.create_browser_with_proxy()
                                self.current_mode = 'object'
                                self.logger.debug(f"Буфер={buffer_size}/{CATALOG_BUFFER_SIZE} → парсим объявление")
                                await self.process_object_task(task)
                                continue

                        if not REPARSE_MODE:
                            task = await acquire_catalog_task(conn, self.worker_id)
                            if task:
                                if not self.browser:
                                    await self.create_browser_with_proxy()
                                self.current_mode = 'catalog'
                                await self.process_catalog_task(task)
                                continue

                # Нет задач обоих типов - ждем
                self.logger.debug(f"Нет доступных задач (буфер={buffer_size}), ожидание...")
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
        CLOSE_TIMEOUT = 10  # секунд

        # Закрываем контекст с timeout
        try:
            if self.context and self.context.browser:
                await asyncio.wait_for(self.context.close(), timeout=CLOSE_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger.warning("context.close() TIMEOUT при cleanup")
        except Exception as e:
            # Игнорируем ошибки от уже закрытого соединения
            if "closed" not in str(e).lower():
                self.logger.warning(f"Ошибка при закрытии контекста: {e}")

        # Закрываем браузер с timeout
        try:
            if self.browser and self.browser.is_connected():
                await asyncio.wait_for(self.browser.close(), timeout=CLOSE_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger.error("browser.close() TIMEOUT при cleanup")
        except Exception as e:
            # Игнорируем ошибки от уже закрытого соединения
            if "closed" not in str(e).lower():
                self.logger.warning(f"Ошибка при закрытии браузера: {e}")

        # Останавливаем Playwright
        try:
            if self.playwright:
                await asyncio.wait_for(self.playwright.stop(), timeout=CLOSE_TIMEOUT)
        except asyncio.TimeoutError:
            self.logger.warning("playwright.stop() TIMEOUT")
        except Exception as e:
            # Игнорируем ошибки от уже остановленного Playwright
            if "closed" not in str(e).lower():
                self.logger.warning(f"Ошибка при остановке Playwright: {e}")

        # Освобождаем прокси
        if self.current_proxy_id and self.pool:
            try:
                async with self.pool.acquire() as conn:
                    await release_proxy(conn, self.current_proxy_id)
            except Exception as e:
                self.logger.error(f"Ошибка при освобождении прокси: {e}")

        # Закрываем пул БД
        if self.pool:
            try:
                await self.pool.close()
            except Exception as e:
                self.logger.warning(f"Ошибка при закрытии пула БД: {e}")

        self.logger.info("Завершен")

    async def run(self):
        """Запуск воркера"""
        try:
            await self.init()
            await self.main_loop()
        except KeyboardInterrupt:
            self.logger.info("Получен сигнал остановки (KeyboardInterrupt)")
        except asyncio.CancelledError:
            self.logger.info("Воркер отменен (CancelledError)")
        except Exception as e:
            self.logger.error(f"Ошибка в воркере: {e}", exc_info=True)
        finally:
            await self.cleanup()


async def main():
    """Точка входа воркера"""
    if len(sys.argv) < 2:
        logging.error("Usage: python browser_worker.py <worker_id> [display]")
        sys.exit(1)

    worker_id = sys.argv[1]

    # Устанавливаем DISPLAY из аргумента (если передан)
    if len(sys.argv) >= 3:
        display = sys.argv[2]
        os.environ['DISPLAY'] = display
        logging.info(f"Worker#{worker_id}: DISPLAY установлен: {display}")

    worker = BrowserWorker(worker_id)
    await worker.run()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful shutdown - не показываем traceback
        logging.info("Воркер остановлен пользователем")
        sys.exit(0)
    except SystemExit:
        raise
    except Exception as e:
        logging.error(f"Критическая ошибка воркера: {e}", exc_info=True)
        sys.exit(1)
