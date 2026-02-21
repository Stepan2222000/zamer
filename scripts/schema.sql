-- Таблица артикулов
-- state: NEW → CATALOG_PARSING → CATALOG_PARSED → VALIDATING → VALIDATED → OBJECT_PARSING
--        или VALIDATING → REJECTED_BY_MIN_COUNT (финальное)
CREATE TABLE IF NOT EXISTS articulums (
    id SERIAL PRIMARY KEY,
    articulum VARCHAR(255) UNIQUE NOT NULL,
    state VARCHAR(50) NOT NULL DEFAULT 'NEW',
    state_updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индекс для быстрого поиска по состоянию
CREATE INDEX IF NOT EXISTS idx_articulums_state ON articulums(state);

-- Таблица прокси
CREATE TABLE IF NOT EXISTS proxies (
    id SERIAL PRIMARY KEY,
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    username VARCHAR(255),
    password VARCHAR(255),
    is_blocked BOOLEAN DEFAULT FALSE,
    is_in_use BOOLEAN DEFAULT FALSE,
    worker_id VARCHAR(50),
    consecutive_errors INTEGER DEFAULT 0,
    last_error_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(host, port, username)
);

-- Индекс для атомарной выдачи прокси (по доступности)
CREATE INDEX IF NOT EXISTS idx_proxies_availability ON proxies(is_blocked, is_in_use);

-- Индекс для поиска прокси по воркеру
CREATE INDEX IF NOT EXISTS idx_proxies_worker ON proxies(worker_id);

-- Таблица очереди задач парсинга каталогов
-- status: pending → processing → completed/failed/invalid
CREATE TABLE IF NOT EXISTS catalog_tasks (
    id SERIAL PRIMARY KEY,
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    checkpoint_page INTEGER DEFAULT 1,
    worker_id VARCHAR(50),
    heartbeat_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для catalog_tasks
CREATE INDEX IF NOT EXISTS idx_catalog_tasks_status ON catalog_tasks(status);
CREATE INDEX IF NOT EXISTS idx_catalog_tasks_heartbeat ON catalog_tasks(heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_catalog_tasks_articulum ON catalog_tasks(articulum_id);

-- Таблица объявлений из каталогов
CREATE TABLE IF NOT EXISTS catalog_listings (
    id SERIAL PRIMARY KEY,
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    avito_item_id VARCHAR(255) UNIQUE NOT NULL,
    title TEXT,
    price NUMERIC,
    snippet_text TEXT,
    seller_name VARCHAR(500),
    seller_id VARCHAR(255),
    seller_rating NUMERIC,
    seller_reviews INTEGER,
    -- Колонки изображений
    images_urls JSONB,              -- JSON-массив URL изображений
    s3_keys TEXT[],                 -- Массив S3-ключей изображений (до 5 шт)
    images_count SMALLINT,          -- Количество изображений (0-5), NULL если не запрашивалось
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для catalog_listings
CREATE INDEX IF NOT EXISTS idx_catalog_listings_articulum ON catalog_listings(articulum_id);
CREATE INDEX IF NOT EXISTS idx_catalog_listings_avito_item_id ON catalog_listings(avito_item_id);
CREATE INDEX IF NOT EXISTS idx_catalog_listings_images_count ON catalog_listings(images_count);

-- Таблица очереди задач парсинга объявлений
-- status: pending → processing → completed/failed/invalid
CREATE TABLE IF NOT EXISTS object_tasks (
    id SERIAL PRIMARY KEY,
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    avito_item_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    worker_id VARCHAR(50),
    heartbeat_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для object_tasks
CREATE INDEX IF NOT EXISTS idx_object_tasks_status ON object_tasks(status);
CREATE INDEX IF NOT EXISTS idx_object_tasks_heartbeat ON object_tasks(heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_object_tasks_articulum ON object_tasks(articulum_id);
CREATE INDEX IF NOT EXISTS idx_object_tasks_avito_item_id ON object_tasks(avito_item_id);

-- Таблица детальных данных объявлений
-- Каждый парсинг создает новую запись (для анализа динамики)
CREATE TABLE IF NOT EXISTS object_data (
    id SERIAL PRIMARY KEY,
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    avito_item_id VARCHAR(255) NOT NULL,
    title TEXT,
    price NUMERIC,
    seller_name VARCHAR(500),
    seller_id VARCHAR(255),
    seller_rating NUMERIC,
    published_at TIMESTAMP,
    description TEXT,
    location_name VARCHAR(500),
    location_coords VARCHAR(100),
    characteristics JSONB,
    views_total INTEGER,
    raw_html TEXT,
    parsed_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для object_data
CREATE INDEX IF NOT EXISTS idx_object_data_articulum ON object_data(articulum_id);
CREATE INDEX IF NOT EXISTS idx_object_data_avito_item_id ON object_data(avito_item_id);
CREATE INDEX IF NOT EXISTS idx_object_data_parsed_at ON object_data(parsed_at);

-- Таблица результатов валидации объявлений
-- Хранит результаты трех этапов валидации: price_filter, mechanical, ai
CREATE TABLE IF NOT EXISTS validation_results (
    id SERIAL PRIMARY KEY,
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    avito_item_id VARCHAR(255) NOT NULL,
    validation_type VARCHAR(20) NOT NULL, -- 'price_filter', 'mechanical', 'ai'
    passed BOOLEAN NOT NULL,
    rejection_reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для validation_results
CREATE INDEX IF NOT EXISTS idx_validation_results_articulum ON validation_results(articulum_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_item ON validation_results(avito_item_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_type ON validation_results(validation_type);
CREATE INDEX IF NOT EXISTS idx_validation_results_passed ON validation_results(passed);

-- Таблица фильтра объявлений для повторного парсинга
-- Содержит список avito_item_id для фильтрации в режиме REPARSE_MODE
CREATE TABLE IF NOT EXISTS reparse_filter_items (
    id SERIAL PRIMARY KEY,
    avito_item_id VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индекс для reparse_filter_items
CREATE INDEX IF NOT EXISTS idx_reparse_filter_items_avito_item_id ON reparse_filter_items(avito_item_id);

-- Таблица фильтра артикулов для повторного парсинга
-- Содержит список артикулов для фильтрации в режиме REPARSE_MODE
CREATE TABLE IF NOT EXISTS reparse_filter_articulums (
    id SERIAL PRIMARY KEY,
    articulum VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индекс для reparse_filter_articulums
CREATE INDEX IF NOT EXISTS idx_reparse_filter_articulums_articulum ON reparse_filter_articulums(articulum);

-- Составной индекс для оптимизации запроса повторного парсинга (проверка MIN_REPARSE_INTERVAL_HOURS)
CREATE INDEX IF NOT EXISTS idx_object_data_avito_item_id_parsed_at ON object_data(avito_item_id, parsed_at DESC);

-- Таблица аналитики просмотров объявлений
-- Расчет динамики изменения просмотров между замерами
CREATE TABLE IF NOT EXISTS analytics_views (
    id SERIAL PRIMARY KEY,
    avito_item_id VARCHAR(255) NOT NULL,
    articulums TEXT,  -- список артикулов через запятую
    title TEXT,
    description TEXT,
    characteristics JSONB,
    price NUMERIC,
    first_views INTEGER,
    last_views INTEGER,
    views_diff INTEGER,
    time_diff NUMERIC,  -- в часах
    efficiency_coefficient NUMERIC,  -- просмотры за час (views_diff / time_diff)
    first_parsed_at TIMESTAMP,
    last_parsed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для analytics_views
CREATE INDEX IF NOT EXISTS idx_analytics_views_avito_item_id ON analytics_views(avito_item_id);
CREATE INDEX IF NOT EXISTS idx_analytics_views_efficiency ON analytics_views(efficiency_coefficient DESC);

-- Таблица аналитики отклонений объявлений по артикулам
-- Детальный отчет по каждому объявлению с причинами отклонения на каждом этапе валидации
CREATE TABLE IF NOT EXISTS analytics_articulum_report (
    id SERIAL PRIMARY KEY,
    rejection_reason TEXT,                   -- Финальная причина отклонения (из соответствующего этапа)
    articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
    articulum VARCHAR(255) NOT NULL,
    avito_item_id VARCHAR(255) NOT NULL,

    -- Данные объявления
    title TEXT,
    price NUMERIC,
    seller_name VARCHAR(500),

    -- Результаты валидации: Price Filter
    price_filter_passed BOOLEAN,
    price_filter_reason TEXT,

    -- Результаты валидации: Mechanical
    mechanical_passed BOOLEAN,
    mechanical_reason TEXT,

    -- Результаты валидации: AI
    ai_passed BOOLEAN,
    ai_reason TEXT,

    -- Итоговый результат
    final_passed BOOLEAN NOT NULL,           -- Прошло все этапы валидации
    rejection_stage VARCHAR(50),             -- На каком этапе отклонено (price_filter/mechanical/ai)

    created_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для analytics_articulum_report
CREATE INDEX IF NOT EXISTS idx_analytics_report_articulum ON analytics_articulum_report(articulum_id, avito_item_id);
CREATE INDEX IF NOT EXISTS idx_analytics_report_item ON analytics_articulum_report(avito_item_id);
CREATE INDEX IF NOT EXISTS idx_analytics_report_passed ON analytics_articulum_report(final_passed);
CREATE INDEX IF NOT EXISTS idx_analytics_report_stage ON analytics_articulum_report(rejection_stage);

-- ВРЕМЕННОЕ РЕШЕНИЕ: счетчик WRONG_PAGE для диагностики
-- TODO: удалить или переосмыслить после анализа проблем
ALTER TABLE catalog_tasks ADD COLUMN IF NOT EXISTS wrong_page_count INTEGER DEFAULT 0;
ALTER TABLE object_tasks ADD COLUMN IF NOT EXISTS wrong_page_count INTEGER DEFAULT 0;
