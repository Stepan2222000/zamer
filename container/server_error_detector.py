"""
Локальный детектор для server-side ошибок (502, 503, 504).

ВАЖНО: Это ВРЕМЕННОЕ решение.
В будущем эти детекторы должны быть добавлены в avito-library.

Детектирует серверные ошибки Avito по:
1. HTTP статус-коду (если доступен через last_response)
2. Текст на странице ("502 Error", "503 Service", "504 Gateway")
"""

import logging
from typing import Optional
from playwright.async_api import Page, Response

logger = logging.getLogger(__name__)

# Константы детекторов (совместимо с avito-library)
SERVER_ERROR_502_DETECTOR_ID = "server_error_502_detector"
SERVER_ERROR_503_DETECTOR_ID = "server_error_503_detector"
SERVER_ERROR_504_DETECTOR_ID = "server_error_504_detector"


async def detect_server_error(page: Page, last_response: Optional[Response] = None) -> Optional[str]:
    """
    Детектирует server-side ошибки (502/503/504).

    ВРЕМЕННОЕ РЕШЕНИЕ: возможно в будущем эта логика будет перенесена в avito-library.

    Args:
        page: Playwright Page object
        last_response: Последний HTTP response (для проверки статус-кода)

    Returns:
        ID детектора (SERVER_ERROR_XXX_DETECTOR_ID) или None
    """

    # Проверка 1: HTTP статус-код (если доступен)
    if last_response:
        status = last_response.status

        if status == 502:
            logger.warning("Обнаружен HTTP 502 Bad Gateway")
            return SERVER_ERROR_502_DETECTOR_ID
        elif status == 503:
            logger.warning("Обнаружен HTTP 503 Service Unavailable")
            return SERVER_ERROR_503_DETECTOR_ID
        elif status == 504:
            logger.warning("Обнаружен HTTP 504 Gateway Timeout")
            return SERVER_ERROR_504_DETECTOR_ID

    # Проверка 2: Текст на странице (fallback, если response недоступен)
    try:
        html = await page.content()
        html_lower = html.lower()

        # Проверяем наличие характерных текстов
        if "502 error" in html_lower or "bad gateway" in html_lower:
            logger.warning("Обнаружен 502 Bad Gateway (по содержимому страницы)")
            return SERVER_ERROR_502_DETECTOR_ID

        if "503" in html_lower and ("service unavailable" in html_lower or "temporarily unavailable" in html_lower):
            logger.warning("Обнаружен 503 Service Unavailable (по содержимому страницы)")
            return SERVER_ERROR_503_DETECTOR_ID

        if "504" in html_lower and ("gateway timeout" in html_lower or "gateway time-out" in html_lower):
            logger.warning("Обнаружен 504 Gateway Timeout (по содержимому страницы)")
            return SERVER_ERROR_504_DETECTOR_ID

    except Exception as e:
        logger.debug(f"Ошибка при проверке содержимого страницы на server errors: {e}")

    return None


def is_server_error(detector_id: str) -> bool:
    """Проверяет, является ли детектор server error."""
    return detector_id in {
        SERVER_ERROR_502_DETECTOR_ID,
        SERVER_ERROR_503_DETECTOR_ID,
        SERVER_ERROR_504_DETECTOR_ID,
    }


def get_server_error_description(detector_id: str) -> str:
    """Возвращает описание server error детектора."""
    descriptions = {
        SERVER_ERROR_502_DETECTOR_ID: "HTTP 502 Bad Gateway (server error)",
        SERVER_ERROR_503_DETECTOR_ID: "HTTP 503 Service Unavailable (server error)",
        SERVER_ERROR_504_DETECTOR_ID: "HTTP 504 Gateway Timeout (server error)",
    }
    return descriptions.get(detector_id, f"Unknown server error: {detector_id}")
