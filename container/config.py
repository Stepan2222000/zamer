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
    'port': int(os.getenv('DB_PORT', '5419')),
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

# Поля для извлечения из карточек каталога
# ВАЖНО: библиотека avito-library ищет 'snippet', а не 'snippet_text'
CATALOG_FIELDS = [
    'item_id',
    'title',
    'price',
    'snippet',  # не snippet_text!
    'seller_name',
    'seller_id',
    'seller_rating',
    'seller_reviews',
]

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
ENABLE_AI_VALIDATION = os.getenv('ENABLE_AI_VALIDATION', 'false').lower() == 'true'

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

# ========== VERTEX AI (ИИ-ВАЛИДАЦИЯ) ==========

# Google Cloud Project ID
VERTEX_AI_PROJECT_ID = os.getenv('VERTEX_AI_PROJECT_ID', 'gen-lang-client-0026618973')

# Регион Vertex AI
VERTEX_AI_LOCATION = os.getenv('VERTEX_AI_LOCATION', 'us-central1')

# Модель Gemini для валидации
VERTEX_AI_MODEL = os.getenv('VERTEX_AI_MODEL', 'google/gemini-2.5-flash')

# Путь к Service Account JSON для аутентификации
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    'GOOGLE_APPLICATION_CREDENTIALS',
    '/app/gen-lang-client-0026618973-4dbdd3b53fdc.json'
)

# ========== ПОВТОРНЫЙ ПАРСИНГ ==========

# Режим повторного парсинга (только ранее спарсенные объявления)
REPARSE_MODE = os.getenv('REPARSE_MODE', 'false').lower() == 'true'

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

# Таймаут ожидания свободного прокси (секунды)
PROXY_WAIT_TIMEOUT = int(os.getenv('PROXY_WAIT_TIMEOUT', '10'))
