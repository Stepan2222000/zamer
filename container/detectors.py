"""
Обертка над avito-library детекторами.

Этот модуль реэкспортирует детекторы состояний страницы и утилиты
для работы с капчей из библиотеки avito-library.

Все взаимодействие с Авито происходит через Playwright + avito-library.
"""

from playwright.async_api import Page, Response

# Импорт функции детекции состояния
try:
    from avito_library import detect_page_state
except ImportError:
    raise ImportError(
        "avito-library не установлена. Установите: "
        "pip install git+https://github.com/Stepan2222000/avito-library.git@v0.1.0#egg=avito-library"
    )

# Импорт констант детекторов
try:
    from avito_library.detectors import (
        # Успешные состояния
        CATALOG_DETECTOR_ID,          # Страница каталога с объявлениями
        CARD_FOUND_DETECTOR_ID,       # Карточка объявления загружена
        SELLER_PROFILE_DETECTOR_ID,   # Профиль продавца

        # Блокировки прокси (постоянные)
        PROXY_BLOCK_403_DETECTOR_ID,  # HTTP 403 - прокси заблокирован
        PROXY_AUTH_DETECTOR_ID,       # Ошибка аутентификации прокси

        # Капчи и временные блокировки
        CAPTCHA_DETECTOR_ID,          # Обнаружена Geetest капча
        PROXY_BLOCK_429_DETECTOR_ID,  # HTTP 429 - rate limit (решается через капчу)
        CONTINUE_BUTTON_DETECTOR_ID,  # Кнопка "Продолжить" (обычно после капчи)

        # Проблемы с объявлением
        REMOVED_DETECTOR_ID,          # Объявление снято с публикации

        # Неопределенное состояние
        NOT_DETECTED_STATE_ID,        # Ни один детектор не распознал страницу

        # Исключения
        DetectionError,               # Ошибка конфигурации детектора
    )
except ImportError as e:
    raise ImportError(f"Не удалось импортировать детекторы из avito-library: {e}")

# Импорт утилиты для решения капчи (только resolve_captcha_flow для прода)
try:
    from avito_library import resolve_captcha_flow
except ImportError as e:
    raise ImportError(f"Не удалось импортировать resolve_captcha_flow из avito-library: {e}")



# Реэкспорт для удобства
__all__ = [
    # Функция детекции
    "detect_page_state",

    # Константы детекторов
    "CATALOG_DETECTOR_ID",
    "CARD_FOUND_DETECTOR_ID",
    "SELLER_PROFILE_DETECTOR_ID",
    "PROXY_BLOCK_403_DETECTOR_ID",
    "PROXY_AUTH_DETECTOR_ID",
    "CAPTCHA_DETECTOR_ID",
    "PROXY_BLOCK_429_DETECTOR_ID",
    "CONTINUE_BUTTON_DETECTOR_ID",
    "REMOVED_DETECTOR_ID",
    "NOT_DETECTED_STATE_ID",

    # Исключения
    "DetectionError",

    # Утилиты
    "resolve_captcha_flow",
]


# Вспомогательные функции

def get_detector_description(detector_id: str) -> str:
    """Возвращает описание детектора по его ID."""
    descriptions = {
        CATALOG_DETECTOR_ID: "Страница каталога с объявлениями",
        CARD_FOUND_DETECTOR_ID: "Карточка объявления загружена",
        SELLER_PROFILE_DETECTOR_ID: "Профиль продавца",
        PROXY_BLOCK_403_DETECTOR_ID: "HTTP 403 - прокси заблокирован",
        PROXY_AUTH_DETECTOR_ID: "Ошибка аутентификации прокси",
        CAPTCHA_DETECTOR_ID: "Обнаружена Geetest капча",
        PROXY_BLOCK_429_DETECTOR_ID: "HTTP 429 - rate limit",
        CONTINUE_BUTTON_DETECTOR_ID: "Кнопка 'Продолжить'",
        REMOVED_DETECTOR_ID: "Объявление снято с публикации",
        NOT_DETECTED_STATE_ID: "Неизвестное состояние страницы",
    }
    return descriptions.get(detector_id, f"Неизвестный детектор: {detector_id}")


def is_success_state(detector_id: str) -> bool:
    """Проверяет, является ли состояние успешным (можно продолжать парсинг)."""
    return detector_id in {
        CATALOG_DETECTOR_ID,
        CARD_FOUND_DETECTOR_ID,
        SELLER_PROFILE_DETECTOR_ID,
    }


def is_proxy_block(detector_id: str) -> bool:
    """Проверяет, является ли состояние блокировкой прокси."""
    return detector_id in {
        PROXY_BLOCK_403_DETECTOR_ID,
        PROXY_AUTH_DETECTOR_ID,
    }


def is_captcha_state(detector_id: str) -> bool:
    """Проверяет, требуется ли решение капчи."""
    return detector_id in {
        CAPTCHA_DETECTOR_ID,
        PROXY_BLOCK_429_DETECTOR_ID,
    }


def is_final_state(detector_id: str) -> bool:
    """Проверяет, является ли состояние финальным (задачу нужно завершить)."""
    return detector_id in {
        REMOVED_DETECTOR_ID,
        NOT_DETECTED_STATE_ID,
    }
