"""
Обработка network errors для классификации transient vs permanent.

ERR_CONNECTION_CLOSED и подобные ошибки - это transient network errors,
которые требуют retry с backoff, а не постоянной блокировки прокси.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def is_transient_network_error(exception: Exception) -> bool:
    """
    Проверяет, является ли ошибка временной network error.

    Transient errors - это сетевые ошибки, которые могут быть временными
    и требуют retry с backoff, а не постоянной блокировки прокси.

    Args:
        exception: Исключение от Playwright

    Returns:
        True если ошибка временная (retry), False если нет
    """
    error_str = str(exception).lower()

    # Паттерны временных сетевых ошибок
    transient_patterns = [
        'err_connection_closed',       # TCP FIN - корректное закрытие
        'err_connection_reset',        # TCP RST - принудительный разрыв
        'err_network_changed',         # Сеть изменилась
        'err_connection_timed_out',    # Timeout на уровне TCP
        'err_timed_out',               # Общий timeout (net::ERR_TIMED_OUT)
        'err_empty_response',          # Сервер закрыл без данных
        'connection closed',           # Общий паттерн
        'connection reset',            # Общий паттерн
        'net::err_aborted',           # Connection aborted
    ]

    return any(pattern in error_str for pattern in transient_patterns)


def is_permanent_proxy_error(exception: Exception) -> bool:
    """
    Проверяет, является ли ошибка постоянной проблемой прокси.

    Permanent errors - это ошибки прокси, которые НЕ исправятся при retry
    и требуют постоянной блокировки прокси.

    Args:
        exception: Исключение от Playwright

    Returns:
        True если прокси нужно блокировать навсегда, False если нет
    """
    error_str = str(exception).lower()

    # Паттерны постоянных проблем прокси
    permanent_patterns = [
        'err_proxy_connection_failed',      # Прокси недоступен
        'err_tunnel_connection_failed',     # CONNECT туннель не удался
        'proxy authentication required',    # 407 ошибка
        'err_proxy_auth',                   # Ошибка аутентификации
        '407 proxy authentication',         # Явный 407
    ]

    return any(pattern in error_str for pattern in permanent_patterns)


def get_error_description(exception: Exception) -> str:
    """
    Возвращает краткое описание ошибки для логирования.

    Args:
        exception: Исключение

    Returns:
        Строка с описанием типа ошибки
    """
    error_str = str(exception).lower()

    if 'err_connection_closed' in error_str:
        return 'ERR_CONNECTION_CLOSED (TCP FIN)'
    elif 'err_connection_reset' in error_str:
        return 'ERR_CONNECTION_RESET (TCP RST)'
    elif 'err_proxy_connection_failed' in error_str:
        return 'ERR_PROXY_CONNECTION_FAILED (proxy unavailable)'
    elif 'err_connection_timed_out' in error_str:
        return 'ERR_CONNECTION_TIMED_OUT (TCP timeout)'
    elif 'timeout' in error_str:
        return 'Timeout error'
    else:
        # Берем первые 100 символов
        return str(exception)[:100]
