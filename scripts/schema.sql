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
    worker_id INTEGER,
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
    worker_id INTEGER,
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
    created_at TIMESTAMP DEFAULT NOW()
);

-- Индексы для catalog_listings
CREATE INDEX IF NOT EXISTS idx_catalog_listings_articulum ON catalog_listings(articulum_id);
CREATE INDEX IF NOT EXISTS idx_catalog_listings_avito_item_id ON catalog_listings(avito_item_id);
