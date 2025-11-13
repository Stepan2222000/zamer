"""
Универсальная обработка состояний детекторов.

Этот модуль содержит единую логику обработки всех типов детекторов
для catalog и object задач (без дублирования кода).

Функция handle_detector_state() анализирует состояние страницы и возвращает
рекомендацию о дальнейших действиях. Воркер выполняет действия самостоятельно.
"""

import logging
import asyncio
from typing import TypedDict, Literal
from playwright.async_api import Page

from detectors import (
    # Константы детекторов
    CATALOG_DETECTOR_ID,
    CARD_FOUND_DETECTOR_ID,
    SELLER_PROFILE_DETECTOR_ID,
    PROXY_BLOCK_403_DETECTOR_ID,
    PROXY_AUTH_DETECTOR_ID,
    CAPTCHA_DETECTOR_ID,
    PROXY_BLOCK_429_DETECTOR_ID,
    CONTINUE_BUTTON_DETECTOR_ID,
    REMOVED_DETECTOR_ID,
    NOT_DETECTED_STATE_ID,

    # Утилиты
    resolve_captcha_flow,
    detect_page_state,

    # Вспомогательные функции
    is_success_state,
    is_proxy_block,
    is_captcha_state,
    is_final_state,
    get_detector_description,
)

# ВРЕМЕННОЕ РЕШЕНИЕ: Локальный детектор для server errors
# TODO: В будущем перенести в avito-library
from server_error_detector import (
    detect_server_error,
    is_server_error,
    get_server_error_description,
    SERVER_ERROR_502_DETECTOR_ID,
    SERVER_ERROR_503_DETECTOR_ID,
    SERVER_ERROR_504_DETECTOR_ID,
)

logger = logging.getLogger(__name__)


# Типы для результата обработки

class DetectorContext(TypedDict, total=False):
    """Контекст для обработки детектора."""
    page: Page              # Playwright Page object
    proxy_id: int           # ID текущего прокси
    task_id: int            # ID текущей задачи
    worker_id: int          # ID воркера
    task_type: str          # 'catalog' или 'object'


ActionType = Literal[
    'continue',                 # Продолжить парсинг (успешное состояние)
    'block_proxy',             # Заблокировать прокси и взять новый
    'return_task_and_proxy',   # Вернуть задачу и прокси в пулы
    'mark_invalid',            # Пометить задачу как invalid
    'mark_failed',             # Пометить задачу как failed
    'change_proxy_and_retry',  # ВРЕМЕННО: Сменить прокси и повторить (для 502/503/504)
]


class DetectorResult(TypedDict):
    """Результат обработки детектора."""
    action: ActionType      # Рекомендуемое действие
    reason: str            # Причина принятия решения
    data: dict             # Дополнительные данные


# Обертка над detect_page_state для интеграции локального детектора server errors

async def enhanced_detect_page_state(page: Page, last_response=None) -> str:
    """
    Обертка над detect_page_state с приоритетной проверкой server errors.

    ВРЕМЕННОЕ РЕШЕНИЕ: сначала проверяет server errors (502/503/504),
    затем вызывает стандартный detect_page_state из avito-library.

    TODO: В будущем эта логика должна быть внутри avito-library.

    Args:
        page: Playwright Page object
        last_response: Последний HTTP response

    Returns:
        ID обнаруженного состояния
    """
    # ПРИОРИТЕТ 0: Проверяем server errors
    server_error_state = await detect_server_error(page, last_response)
    if server_error_state:
        return server_error_state

    # Вызываем стандартный detect_page_state из avito-library
    return await detect_page_state(page, last_response=last_response)


# Главная функция обработки

async def handle_detector_state(
    state: str,
    context: DetectorContext,
) -> DetectorResult:
    """
    Универсальная обработка результата детекции.

    Анализирует состояние страницы и возвращает рекомендацию о дальнейших действиях.
    Воркер самостоятельно выполняет рекомендованное действие.

    Args:
        state: ID обнаруженного состояния (из detect_page_state)
        context: Контекст выполнения (page, proxy_id, task_id, worker_id, task_type)

    Returns:
        DetectorResult с полями:
            - action: рекомендуемое действие
            - reason: причина принятия решения
            - data: дополнительные данные для выполнения действия

    Приоритет обработки:
        0. Server errors (502/503/504) → change_proxy_and_retry (ВРЕМЕННО)
        1. Блокировки прокси (403/AUTH) → block_proxy
        2. Капчи (CAPTCHA/429/CONTINUE) → resolve или return
        3. Финальные состояния (REMOVED/NOT_DETECTED) → mark_invalid/failed
        4. Успешные состояния (CATALOG/CARD_FOUND) → continue
    """

    page = context['page']
    task_type = context.get('task_type', 'unknown')

    logger.info(
        f"Обработка детектора: state={state}, task_type={task_type}, "
        f"task_id={context.get('task_id')}, proxy_id={context.get('proxy_id')}"
    )

    # ПРИОРИТЕТ 0: Server errors (502/503/504) - ВРЕМЕННОЕ РЕШЕНИЕ
    # TODO: В будущем эта логика должна быть в avito-library
    if is_server_error(state):
        logger.warning(
            f"Server error обнаружен: {state} ({get_server_error_description(state)}), "
            f"proxy_id={context['proxy_id']}"
        )
        # ВРЕМЕННО: меняем прокси и повторяем задачу
        # Прокси НЕ блокируем, так как это проблема сервера Avito, а не прокси
        return DetectorResult(
            action='change_proxy_and_retry',
            reason=f'Server error (temporary): {get_server_error_description(state)}',
            data={
                'proxy_id': context['proxy_id'],
                'task_id': context['task_id'],
                'detector_state': state,
                'keep_task': True,  # Задачу НЕ возвращаем в очередь, воркер сам повторит
            }
        )

    # ПРИОРИТЕТ 1: Блокировки прокси (постоянные)
    if is_proxy_block(state):
        logger.warning(
            f"Прокси заблокирован: {state} ({get_detector_description(state)}), "
            f"proxy_id={context['proxy_id']}"
        )
        return DetectorResult(
            action='block_proxy',
            reason=f'Proxy permanently blocked: {state}',
            data={
                'proxy_id': context['proxy_id'],
                'detector_state': state,
                'keep_task': True,  # Задачу НЕ возвращать в очередь
            }
        )

    # ПРИОРИТЕТ 2: Капчи, 429 и кнопка "Продолжить" (все решаются через resolve_captcha_flow)
    if state in {CAPTCHA_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID}:
        logger.info(
            f"Обнаружено состояние, требующее решения капчи: {state} "
            f"({get_detector_description(state)})"
        )
        return await _handle_captcha(page, context, state)

    # ПРИОРИТЕТ 3: Финальные состояния
    if state == REMOVED_DETECTOR_ID:
        logger.info(
            f"Объявление удалено с Авито, task_id={context['task_id']}"
        )
        return DetectorResult(
            action='mark_invalid',
            reason='Item removed from Avito',
            data={
                'task_id': context['task_id'],
                'detector_state': state,
            }
        )

    if state == NOT_DETECTED_STATE_ID:
        logger.error(
            f"Неизвестное состояние страницы (ни один детектор не сработал), "
            f"task_id={context['task_id']}"
        )
        return DetectorResult(
            action='mark_failed',
            reason='Unknown page state - no detector matched',
            data={
                'task_id': context['task_id'],
                'detector_state': state,
            }
        )

    # ПРИОРИТЕТ 4: Успешные состояния
    if is_success_state(state):
        logger.info(
            f"Успешное состояние: {state} ({get_detector_description(state)})"
        )
        return DetectorResult(
            action='continue',
            reason=f'Success state detected: {state}',
            data={
                'detected_state': state,
                'description': get_detector_description(state),
            }
        )

    # Неожиданное состояние (не должно происходить)
    logger.error(f"Неожиданный детектор: {state}")
    return DetectorResult(
        action='mark_failed',
        reason=f'Unexpected detector state: {state}',
        data={
            'unexpected_state': state,
            'task_id': context.get('task_id'),
        }
    )


# Вспомогательные функции для обработки специфичных состояний

async def _handle_captcha(
    page: Page,
    context: DetectorContext,
    original_state: str,
) -> DetectorResult:
    """
    Обработка капчи и связанных состояний.

    Применяется для:
    - CAPTCHA_DETECTOR_ID (обнаружена Geetest капча)
    - PROXY_BLOCK_429_DETECTOR_ID (rate limit, решается через капчу)
    - CONTINUE_BUTTON_DETECTOR_ID (кнопка "Продолжить", решается через капчу)

    resolve_captcha_flow сам нажимает кнопку "Продолжить" и решает капчу.

    Пытается решить капчу через resolve_captcha_flow (до 3 попыток).
    Если решена - делает повторный детект и рекурсивно обрабатывает новое состояние.
    Если не решена - возвращает рекомендацию вернуть прокси и задачу в пулы.
    """

    try:
        # Решаем капчу (до 3 попыток по умолчанию)
        # resolve_captcha_flow сам нажимает "Продолжить", решает слайдер и проверяет результат
        html, solved = await resolve_captcha_flow(page, max_attempts=3)

        if solved:
            logger.info("Капча успешно решена, выполняем повторный детект...")

            # Повторный детект после решения капчи
            new_state = await detect_page_state(page, last_response=page.response)
            logger.info(f"Новое состояние после решения капчи: {new_state}")

            # Рекурсивный вызов с новым состоянием
            return await handle_detector_state(new_state, context)

        else:
            logger.warning(
                f"Капча НЕ решена после 3 попыток, "
                f"task_id={context['task_id']}, proxy_id={context['proxy_id']}"
            )
            return DetectorResult(
                action='return_task_and_proxy',
                reason='Captcha not solved after 3 attempts',
                data={
                    'proxy_id': context['proxy_id'],
                    'task_id': context['task_id'],
                    'original_state': original_state,
                }
            )

    except Exception as e:
        logger.error(f"Ошибка при решении капчи: {e}", exc_info=True)
        # При ошибке решения капчи возвращаем прокси и задачу
        return DetectorResult(
            action='return_task_and_proxy',
            reason=f'Captcha solving error: {e}',
            data={
                'proxy_id': context['proxy_id'],
                'task_id': context['task_id'],
                'error': str(e),
            }
        )


