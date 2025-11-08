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
