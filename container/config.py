"""Конфигурация системы парсинга"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Загружаем .env файл из корня проекта
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

# ========== ПОДКЛЮЧЕНИЕ К БД ==========

DB_CONFIG = {
    'host': os.getenv('DB_HOST', '81.30.105.134'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'zamer_sys'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD', 'Password123'),
}

# ========== ВОРКЕРЫ ==========

# Общее количество Browser Workers
TOTAL_BROWSER_WORKERS = int(os.getenv('TOTAL_BROWSER_WORKERS', '10'))

# Количество Validation Workers
TOTAL_VALIDATION_WORKERS = int(os.getenv('TOTAL_VALIDATION_WORKERS', '2'))

# Размер буфера каталогов (минимум артикулов со спарсенными каталогами, готовых к парсингу объявлений)
# Если buffer < CATALOG_BUFFER_SIZE → воркеры берут catalog задачи (приоритет)
# Если buffer >= CATALOG_BUFFER_SIZE → воркеры берут object задачи (приоритет)
CATALOG_BUFFER_SIZE = int(os.getenv('CATALOG_BUFFER_SIZE', '5'))

# ========== HEARTBEAT ==========

# Таймаут heartbeat в секундах (если воркер не обновлял heartbeat дольше - задача возвращается в очередь)
HEARTBEAT_TIMEOUT_SECONDS = int(os.getenv('HEARTBEAT_TIMEOUT_SECONDS', '1800'))

# Интервал обновления heartbeat воркером в секундах
HEARTBEAT_UPDATE_INTERVAL = int(os.getenv('HEARTBEAT_UPDATE_INTERVAL', '30'))

# Интервал проверки зависших задач в секундах
HEARTBEAT_CHECK_INTERVAL = int(os.getenv('HEARTBEAT_CHECK_INTERVAL', '60'))

# ========== ПАРСИНГ КАТАЛОГОВ ==========

# Максимальное количество страниц каталога для парсинга (1 страница ≈ 50 объявлений)
CATALOG_MAX_PAGES = int(os.getenv('CATALOG_MAX_PAGES', '10'))

# Сохранять ли raw HTML объявлений из каталога
CATALOG_INCLUDE_HTML = os.getenv('CATALOG_INCLUDE_HTML', 'false').lower() == 'true'

# Базовые поля для извлечения из карточек каталога
# ВАЖНО: библиотека avito-library использует 'snippet_text'
_CATALOG_BASE_FIELDS = [
    'item_id',
    'title',
    'price',
    'snippet_text',
    'seller_name',
    'seller_id',
    'seller_rating',
    'seller_reviews',
]

# ========== ИЗОБРАЖЕНИЯ ==========

# Собирать ли изображения при парсинге каталога
# Если включено, добавляет поле "images" в запрос к avito-library
COLLECT_IMAGES = os.getenv('COLLECT_IMAGES', 'true').lower() == 'true'

# Сохранять ли байты изображений в БД
# Если выключено — сохраняются только URLs и количество
# Для AI валидации с изображениями должно быть включено
SAVE_IMAGES_BYTES = os.getenv('SAVE_IMAGES_BYTES', 'true').lower() == 'true'

# Максимальное количество изображений для сохранения на одно объявление (1-5)
# avito-library возвращает до 5 изображений
MAX_IMAGES_PER_LISTING = int(os.getenv('MAX_IMAGES_PER_LISTING', '5'))

# Требовать наличие изображений при валидации
# Если включено — объявления без фото отклоняются на этапе mechanical validation
REQUIRE_IMAGES = os.getenv('REQUIRE_IMAGES', 'true').lower() == 'true'

# Отправлять ли изображения в AI валидацию
# Работает только если SAVE_IMAGES_BYTES=true
AI_USE_IMAGES = os.getenv('AI_USE_IMAGES', 'true').lower() == 'true'

# Сколько изображений отправлять в AI на одно объявление (1-5)
# Рекомендуется 1-2 для экономии токенов
AI_MAX_IMAGES_PER_LISTING = int(os.getenv('AI_MAX_IMAGES_PER_LISTING', '2'))


def get_catalog_fields() -> list:
    """
    Возвращает список полей для парсинга каталога.
    Динамически добавляет 'images' если COLLECT_IMAGES=true.
    """
    fields = _CATALOG_BASE_FIELDS.copy()
    if COLLECT_IMAGES:
        fields.append('images')
    return fields


# Поля для извлечения из карточек каталога (для обратной совместимости)
CATALOG_FIELDS = get_catalog_fields()

# ========== ПАРСИНГ ОБЪЯВЛЕНИЙ ==========

# Пропустить парсинг объявлений (только валидация и сохранение в БД)
SKIP_OBJECT_PARSING = os.getenv('SKIP_OBJECT_PARSING', 'false').lower() == 'true'

# Сохранять ли raw HTML карточек объявлений
OBJECT_INCLUDE_HTML = os.getenv('OBJECT_INCLUDE_HTML', 'false').lower() == 'true'

# Поля для извлечения из карточек объявлений
OBJECT_FIELDS = [
    'title',
    'price',
    'seller',
    'item_id',
    'published_at',
    'description',
    'location',
    'characteristics',
    'views_total',
]

# ========== ОБРАБОТКА ОШИБОК ==========

# Количество попыток перезагрузки страницы при server errors (502/503/504)
SERVER_ERROR_RETRY_ATTEMPTS = int(os.getenv('SERVER_ERROR_RETRY_ATTEMPTS', '3'))

# Задержка между попытками перезагрузки страницы при server errors (секунды)
SERVER_ERROR_RETRY_DELAY = float(os.getenv('SERVER_ERROR_RETRY_DELAY', '4.0'))

# ========== ВАЛИДАЦИЯ ==========

# Минимальная цена объявления (объявления дешевле игнорируются)
MIN_PRICE = float(os.getenv('MIN_PRICE', '1000.0'))

# Минимальное количество валидных объявлений для артикула
MIN_VALIDATED_ITEMS = int(os.getenv('MIN_VALIDATED_ITEMS', '3'))

# Минимальное количество отзывов продавца (продавцы с меньшим количеством фильтруются)
MIN_SELLER_REVIEWS = int(os.getenv('MIN_SELLER_REVIEWS', '0'))

# Включить валидацию по цене (IQR метод для выбросов + проверка дешевых относительно медианы топ-40%)
ENABLE_PRICE_VALIDATION = os.getenv('ENABLE_PRICE_VALIDATION', 'true').lower() == 'true'

# Требовать обязательное наличие артикула в названии или описании объявления
REQUIRE_ARTICULUM_IN_TEXT = os.getenv('REQUIRE_ARTICULUM_IN_TEXT', 'false').lower() == 'true'

# ИИ-валидация (управляется через переменную окружения)
ENABLE_AI_VALIDATION = os.getenv('ENABLE_AI_VALIDATION', 'true').lower() == 'true'

# ========== AI ПРОВАЙДЕР (FIREWORKS AI) ==========

# Тип AI провайдера (только 'fireworks' поддерживается)
AI_PROVIDER = 'fireworks'

# Fireworks AI API ключ
FIREWORKS_API_KEY = 'fw_DJ9zDiaEjb1L3dPqxhXcdi'

# Модель для валидации (мультимодальная VLM)
FIREWORKS_MODEL = 'accounts/fireworks/models/qwen3-vl-30b-a3b-thinking'

# Таймаут запроса к AI API (секунды)
AI_REQUEST_TIMEOUT = 120

# Максимальное количество retry при transient errors (429, 503)
AI_MAX_RETRIES = 3

# Базовая задержка между retry (секунды, увеличивается экспоненциально)
AI_RETRY_BASE_DELAY = 2.0

# Стоп-слова для механической валидации
VALIDATION_STOPWORDS = [
    # Неоригинальность
    'копия', 'реплика', 'подделка', 'фейк', 'fake',
    'replica', 'copy', 'имитация', 'аналог',
    'не оригинал', 'неоригинал', 'китай', 'china',
    'подобие', 'как оригинал',
    'копи', 'копию', 'дубликат', 'дубль',

    # Б/У и состояние
    'б/у', 'бу', 'б у', 'использованный', 'использованная',
    'ношенный', 'ношеный', 'поношенный',
    'second hand', 'second-hand', 'secondhand', 'used',
    'worn', 'pre-owned', 'preowned', 'pre owned',
    'после носки', 'поноска', 'с дефектами', 'дефект',
    'потертости', 'потёртости', 'царапины', 'следы носки',
    'требует ремонта', 'на запчасти', 'не новый', 'не новая',
]

# ========== ПОВТОРНЫЙ ПАРСИНГ ==========

# Режим повторного парсинга (только ранее спарсенные объявления)
REPARSE_MODE = os.getenv('REPARSE_MODE', 'true').lower() == 'true'

# Минимальный интервал между парсингами одного объявления (в часах)
MIN_REPARSE_INTERVAL_HOURS = int(os.getenv('MIN_REPARSE_INTERVAL_HOURS', '24'))

if MIN_REPARSE_INTERVAL_HOURS < 0:
    raise ValueError("MIN_REPARSE_INTERVAL_HOURS не может быть отрицательным")

# ========== XVFB (ВИРТУАЛЬНЫЕ ДИСПЛЕИ) ==========

# Стартовый номер DISPLAY для Xvfb
XVFB_DISPLAY_START = int(os.getenv('XVFB_DISPLAY_START', '99'))

# Разрешение виртуального дисплея
XVFB_RESOLUTION = os.getenv('XVFB_RESOLUTION', '1920x1080x24')

# ========== STATE MACHINE ==========

# Возможные состояния артикула
class ArticulumState:
    NEW = 'NEW'
    CATALOG_PARSING = 'CATALOG_PARSING'
    CATALOG_PARSED = 'CATALOG_PARSED'
    VALIDATING = 'VALIDATING'
    VALIDATED = 'VALIDATED'
    OBJECT_PARSING = 'OBJECT_PARSING'
    REJECTED_BY_MIN_COUNT = 'REJECTED_BY_MIN_COUNT'

# Все состояния (для валидации)
ALL_STATES = [
    ArticulumState.NEW,
    ArticulumState.CATALOG_PARSING,
    ArticulumState.CATALOG_PARSED,
    ArticulumState.VALIDATING,
    ArticulumState.VALIDATED,
    ArticulumState.OBJECT_PARSING,
    ArticulumState.REJECTED_BY_MIN_COUNT,
]

# Финальные состояния (дальше не переходят)
FINAL_STATES = [
    ArticulumState.OBJECT_PARSING,
    ArticulumState.REJECTED_BY_MIN_COUNT,
]

# ========== ЗАДАЧИ ==========

# Возможные статусы задач
class TaskStatus:
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    INVALID = 'invalid'

# ========== ПРОКСИ ==========

# Режим без прокси (для локального тестирования)
DISABLE_PROXY = os.getenv('DISABLE_PROXY', 'false').lower() == 'true'

# Таймаут ожидания свободного прокси (секунды)
PROXY_WAIT_TIMEOUT = int(os.getenv('PROXY_WAIT_TIMEOUT', '10'))

# ========== ВАЛИДАЦИЯ КОНФИГУРАЦИИ ==========

# Проверка параметров изображений
if MAX_IMAGES_PER_LISTING < 1 or MAX_IMAGES_PER_LISTING > 5:
    raise ValueError("MAX_IMAGES_PER_LISTING должен быть от 1 до 5")

if AI_MAX_IMAGES_PER_LISTING < 1 or AI_MAX_IMAGES_PER_LISTING > 5:
    raise ValueError("AI_MAX_IMAGES_PER_LISTING должен быть от 1 до 5")

if AI_MAX_IMAGES_PER_LISTING > MAX_IMAGES_PER_LISTING:
    raise ValueError("AI_MAX_IMAGES_PER_LISTING не может быть больше MAX_IMAGES_PER_LISTING")

# Автоматическое отключение AI_USE_IMAGES если байты не сохраняются
if AI_USE_IMAGES and not SAVE_IMAGES_BYTES:
    import logging
    logging.warning("AI_USE_IMAGES автоматически отключено: SAVE_IMAGES_BYTES=false")
    AI_USE_IMAGES = False

# Предупреждение если REQUIRE_IMAGES включено, но изображения не собираются
if REQUIRE_IMAGES and not COLLECT_IMAGES:
    import logging
    logging.warning("REQUIRE_IMAGES игнорируется: COLLECT_IMAGES=false")

# Проверка AI провайдера (только fireworks поддерживается)
if AI_PROVIDER != 'fireworks':
    raise ValueError(f"Неподдерживаемый AI_PROVIDER: '{AI_PROVIDER}'. Поддерживается только: fireworks")

# Проверка API ключа
if ENABLE_AI_VALIDATION and not FIREWORKS_API_KEY:
    raise ValueError("FIREWORKS_API_KEY обязателен при ENABLE_AI_VALIDATION=true")
