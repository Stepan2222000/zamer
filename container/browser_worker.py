"""Browser Worker - воркер парсинга каталогов"""

import asyncio
import logging
import os
import sys
from typing import Optional

import asyncpg
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

from avito_library import (
    parse_card,
    CatalogParseStatus,
    CatalogParseResult,
    CardParseStatus,
    CardParseResult,
)

from config import (
    HEARTBEAT_UPDATE_INTERVAL,
    OBJECT_FIELDS,
    OBJECT_INCLUDE_HTML,
    SKIP_OBJECT_PARSING,
    REPARSE_MODE,
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
    increment_wrong_page_count,
)
from object_task_manager import (
    acquire_object_task,
    complete_object_task,
    fail_object_task,
    invalidate_object_task,
    return_object_task_to_queue,
    update_object_task_heartbeat,
    increment_wrong_page_count as increment_object_wrong_page_count,
)
from catalog_parser import parse_catalog_for_articulum, save_listings_to_db
from object_parser import save_object_data_to_db
from state_machine import transition_to_object_parsing, StateTransitionError

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

    async def update_catalog_checkpoint_if_needed(self, task_id: int, result: CatalogParseResult) -> None:
        """Сохраняет checkpoint страницы, если он доступен в результате парсинга."""
        if result.resume_page_number is None:
            return

        async with self.pool.acquire() as conn:
            await update_catalog_task_checkpoint(conn, task_id, result.resume_page_number)

    async def close_browser(self, reason: str) -> None:
        """Закрывает браузер и сбрасывает текущее состояние прокси."""
        try:
            if self.browser:
                await asyncio.wait_for(self.browser.close(), timeout=10)
        except Exception as e:
            self.logger.warning(f"Ошибка при закрытии браузера ({reason}): {e}")
        finally:
            self.browser = None
            self.context = None
            self.page = None
            self.current_proxy_id = None
            self.logger.info(f"Браузер закрыт ({reason})")


    async def handle_parse_result(self, task: dict, result: CatalogParseResult) -> bool:
        """Обработка результата парсинга. Возвращает True, если нужно закрыть браузер."""
        task_id = task['id']
        articulum_id = task['articulum_id']
        articulum = task['articulum']

        meta = result.meta
        status = result.status
        should_close_browser = False

        self.logger.info(
            f"Результат парсинга '{articulum}': status={status.value}, "
            f"pages={meta.processed_pages if meta else None}, "
            f"cards={meta.processed_cards if meta else None}"
        )

        async with self.pool.acquire() as conn:
            if status == CatalogParseStatus.SUCCESS:
                # Успех - сохраняем объявления и завершаем задачу атомарно
                try:
                    async with conn.transaction():
                        saved_count = await save_listings_to_db(conn, articulum_id, result.listings)
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
                    return should_close_browser

            elif status == CatalogParseStatus.EMPTY:
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
                    return should_close_browser

            elif status in {CatalogParseStatus.PROXY_BLOCKED, CatalogParseStatus.PROXY_AUTH_REQUIRED}:
                # Прокси заблокирован - блокируем его и возвращаем задачу
                self.logger.error(f"Прокси заблокирован: {status.value}")
                await block_proxy(conn, self.current_proxy_id, f"Catalog parsing: {status.value}")
                await return_catalog_task_to_queue(conn, task_id)
                should_close_browser = True

            elif status == CatalogParseStatus.CAPTCHA_FAILED:
                # Капча не решилась - возвращаем задачу и прокси в очередь
                self.logger.warning("Капча не решилась, возвращаем задачу")
                await return_catalog_task_to_queue(conn, task_id)
                await release_proxy(conn, self.current_proxy_id)
                should_close_browser = True

            elif status == CatalogParseStatus.PAGE_NOT_DETECTED:
                # Неопределенное состояние - помечаем как failed
                details = meta.details if meta else None
                self.logger.error(f"PAGE_NOT_DETECTED - помечаем как failed: {details}")
                await fail_catalog_task(conn, task_id, f"PAGE_NOT_DETECTED: {details}")

            elif status == CatalogParseStatus.WRONG_PAGE:
                # ВРЕМЕННОЕ РЕШЕНИЕ: retry вместо fail, счетчик для диагностики
                # TODO: после стабилизации проанализировать wrong_page_count
                details = meta.details if meta else None
                new_count = await increment_wrong_page_count(conn, task_id)
                self.logger.warning(f"WRONG_PAGE #{new_count} - освобождаем прокси, возвращаем в очередь: {details}")
                await release_proxy(conn, self.current_proxy_id)
                await return_catalog_task_to_queue(conn, task_id)
                should_close_browser = True

            elif status == CatalogParseStatus.LOAD_TIMEOUT:
                # Таймаут загрузки - возвращаем задачу и увеличиваем счетчик ошибок прокси
                self.logger.warning("LOAD_TIMEOUT - возвращаем задачу и увеличиваем счетчик ошибок прокси")
                await return_catalog_task_to_queue(conn, task_id)
                await increment_proxy_error(conn, self.current_proxy_id, "Catalog LOAD_TIMEOUT")
                should_close_browser = True

            elif status == CatalogParseStatus.SERVER_UNAVAILABLE:
                # Сервер недоступен - возвращаем задачу, прокси не блокируем
                self.logger.warning("SERVER_UNAVAILABLE - возвращаем задачу без блокировки прокси")
                await return_catalog_task_to_queue(conn, task_id)
                await release_proxy(conn, self.current_proxy_id)
                should_close_browser = True

            else:
                # Прочие ошибки - возвращаем в очередь
                self.logger.warning(f"Неожиданная ошибка {status.value}, возвращаем в очередь")
                await return_catalog_task_to_queue(conn, task_id)

        return should_close_browser

    async def process_catalog_task(self, task: dict):
        """Обработка одной catalog_task"""
        task_id = task['id']
        articulum = task['articulum']
        checkpoint_page = task['checkpoint_page']

        self.logger.info(f"Обработка задачи #{task_id}: артикул='{articulum}', checkpoint={checkpoint_page}")

        # Запускаем фоновые задачи
        heartbeat_task = asyncio.create_task(self.update_heartbeat_loop(task_id, 'catalog'))

        # Флаг успешной обработки (для race condition protection)
        task_completed = False
        max_proxy_rotations = 10
        proxy_rotations = 0

        try:
            # Парсим каталог
            result = await parse_catalog_for_articulum(
                self.page,
                articulum,
                start_page=checkpoint_page
            )

            # Сохраняем checkpoint если есть
            await self.update_catalog_checkpoint_if_needed(task_id, result)

            # При блокировке прокси — меняем и продолжаем
            while result.status in {CatalogParseStatus.PROXY_BLOCKED, CatalogParseStatus.PROXY_AUTH_REQUIRED}:
                proxy_rotations += 1

                async with self.pool.acquire() as conn:
                    await block_proxy(conn, self.current_proxy_id, f"Catalog parsing: {result.status.value}")

                if proxy_rotations >= max_proxy_rotations:
                    self.logger.error(
                        f"Слишком много смен прокси ({proxy_rotations}), "
                        f"возвращаем задачу #{task_id} в очередь"
                    )
                    async with self.pool.acquire() as conn:
                        await return_catalog_task_to_queue(conn, task_id)
                    await self.close_browser("catalog proxy rotations exceeded")
                    task_completed = True
                    break

                self.logger.warning("Прокси заблокирован, меняем...")
                await self.recreate_page_with_new_proxy()

                try:
                    result = await result.continue_from(self.page)
                except ValueError as e:
                    self.logger.error(f"Ошибка continue_from: {e}")
                    async with self.pool.acquire() as conn:
                        await fail_catalog_task(conn, task_id, f"continue_from error: {e}")
                    task_completed = True
                    break

                await self.update_catalog_checkpoint_if_needed(task_id, result)

            if not task_completed:
                # Обрабатываем финальный результат
                should_close_browser = await self.handle_parse_result(task, result)
                if should_close_browser:
                    await self.close_browser("catalog task result")

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
                await self.close_browser("catalog permanent proxy error")
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
                await self.close_browser("catalog transient proxy error")
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

            heartbeat_task.cancel()

            # 2. Ждем завершения фоновых задач с timeout
            try:
                await asyncio.wait_for(heartbeat_task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

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
        should_close_browser = False

        try:
            # Переходим на страницу объявления
            url = f"https://www.avito.ru/{avito_item_id}"
            response = await self.page.goto(url, wait_until="domcontentloaded", timeout=150000)

            # Парсим карточку (библиотека сама обрабатывает капчу, блокировки и retry)
            result: CardParseResult = await parse_card(
                self.page,
                response,
                fields=OBJECT_FIELDS,
                include_html=OBJECT_INCLUDE_HTML,
            )

            async with self.pool.acquire() as conn:
                if result.status == CardParseStatus.SUCCESS and result.data:
                    card_data = result.data

                    # Проверяем состояние "б/у" в характеристиках
                    if self._is_used_condition(card_data.characteristics):
                        rejection_reason = 'Найдено состояние "б/у" в характеристиках'
                        await invalidate_object_task(conn, task_id, rejection_reason)
                        self.logger.info(f"Объявление {avito_item_id} отклонено: {rejection_reason}")
                        task_completed = True
                    else:
                        raw_html = card_data.raw_html if OBJECT_INCLUDE_HTML else None

                        # Сохраняем данные в БД
                        await save_object_data_to_db(conn, articulum_id, avito_item_id, card_data, raw_html)

                        # Завершаем задачу
                        await complete_object_task(conn, task_id)

                        # Сбрасываем счетчик ошибок прокси после успешного выполнения
                        await reset_proxy_error_counter(conn, self.current_proxy_id)

                        self.logger.info(f"Объявление {avito_item_id} успешно спарсено")
                        task_completed = True

                elif result.status == CardParseStatus.PROXY_BLOCKED:
                    await block_proxy(conn, self.current_proxy_id, f"Card parsing: {result.status.value}")
                    await return_object_task_to_queue(conn, task_id)
                    self.logger.warning("Прокси заблокирован при парсинге карточки")
                    task_completed = True
                    should_close_browser = True

                elif result.status == CardParseStatus.CAPTCHA_FAILED:
                    await return_object_task_to_queue(conn, task_id)
                    await release_proxy(conn, self.current_proxy_id)
                    self.logger.warning("Капча не решена, возвращаем задачу и прокси")
                    task_completed = True
                    should_close_browser = True

                elif result.status == CardParseStatus.NOT_FOUND:
                    await invalidate_object_task(conn, task_id, "Item removed or not found")
                    if REPARSE_MODE:
                        self.logger.info(f"Объявление {avito_item_id} удалено, пропускаем (REPARSE_MODE)")
                    else:
                        self.logger.info(f"Объявление {avito_item_id} помечено как invalid: Item removed or not found")
                    task_completed = True

                elif result.status == CardParseStatus.PAGE_NOT_DETECTED:
                    await fail_object_task(conn, task_id, "PAGE_NOT_DETECTED")
                    self.logger.error("PAGE_NOT_DETECTED - задача помечена как failed")
                    task_completed = True

                elif result.status == CardParseStatus.WRONG_PAGE:
                    # ВРЕМЕННОЕ РЕШЕНИЕ: retry вместо fail, счетчик для диагностики
                    # TODO: после стабилизации проанализировать wrong_page_count
                    new_count = await increment_object_wrong_page_count(conn, task_id)
                    self.logger.warning(f"WRONG_PAGE #{new_count} - освобождаем прокси, возвращаем в очередь")
                    await release_proxy(conn, self.current_proxy_id)
                    await return_object_task_to_queue(conn, task_id)
                    task_completed = True
                    should_close_browser = True

                elif result.status == CardParseStatus.SERVER_UNAVAILABLE:
                    await return_object_task_to_queue(conn, task_id)
                    await release_proxy(conn, self.current_proxy_id)
                    self.logger.warning("SERVER_UNAVAILABLE - возвращаем задачу без блокировки прокси")
                    task_completed = True
                    should_close_browser = True

                else:
                    self.logger.warning(f"Неожиданный статус {result.status.value}, возвращаем задачу")
                    await return_object_task_to_queue(conn, task_id)
                    task_completed = True

            if should_close_browser:
                await self.close_browser("object task result")

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
                await self.close_browser("object permanent proxy error")
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
                await self.close_browser("object transient proxy error")
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
