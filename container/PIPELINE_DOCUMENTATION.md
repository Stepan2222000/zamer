# Zamer Container — Объяснение пайплайна парсинга Avito

## Обзор системы

**Назначение:** Распределённая система парсинга объявлений Avito по артикулам автозапчастей с многоэтапной валидацией.

**Технологии:**
- **Python 3.12** — основной язык
- **Playwright** — браузерная автоматизация
- **asyncpg** — асинхронный PostgreSQL драйвер
- **avito-library** — специализированная библиотека для парсинга Avito
- **Xvfb** — виртуальные дисплеи для headless-режима на Linux
- **Docker** — контейнеризация

**Компоненты:**
1. **MainProcess** — оркестратор, управляет воркерами и создаёт задачи
2. **BrowserWorker** — парсит каталоги и карточки объявлений через браузер
3. **ValidationWorker** — валидирует объявления без браузера (механика + AI)
4. **HeartbeatManager** — восстанавливает зависшие задачи
5. **ProxyManager** — управляет пулом прокси с политикой блокировки
6. **StateMachine** — контролирует жизненный цикл артикулов

**Схема взаимодействия:**
```
┌─────────────────────────────────────────────────────────────────────┐
│                         MainProcess                                  │
│  ┌──────────────┐ ┌──────────────┐ ┌───────────────────────────┐   │
│  │ spawn_workers│ │ heartbeat    │ │ create_tasks (catalog/obj)│   │
│  └──────────────┘ └──────────────┘ └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
         │                   │                        │
         ▼                   ▼                        ▼
┌────────────────┐  ┌────────────────┐      ┌─────────────────┐
│ BrowserWorker  │  │ ValidationWorker│      │   PostgreSQL    │
│ (1..N процессы)│  │ (1..M процессы) │      │   (articulums,  │
│                │  │                 │      │   tasks, data)  │
│ ┌────────────┐ │  │ ┌─────────────┐ │      └─────────────────┘
│ │ Playwright │ │  │ │ price_filter│ │               ▲
│ │ + Proxy    │ │  │ │ mechanical  │ │               │
│ │            │ │  │ │ AI (Gemini) │ │               │
│ └────────────┘ │  │ └─────────────┘ │               │
└────────────────┘  └────────────────┘                │
         │                   │                        │
         └───────────────────┴────────────────────────┘
```

## Структура проекта

```
container/
├── main.py                  # Точка входа, оркестратор системы
├── config.py                # Все параметры конфигурации
├── database.py              # Пул подключений к PostgreSQL
├── state_machine.py         # State machine артикулов
├── browser_worker.py        # Браузерный воркер (каталоги + объявления)
├── validation_worker.py     # Воркер валидации (без браузера)
├── catalog_parser.py        # Парсинг каталогов через avito-library
├── object_parser.py         # Сохранение данных объявлений
├── catalog_task_manager.py  # CRUD для catalog_tasks
├── object_task_manager.py   # CRUD для object_tasks
├── proxy_manager.py         # Управление пулом прокси
├── heartbeat_manager.py     # Восстановление зависших задач
├── network_error_handler.py # Классификация сетевых ошибок
├── detectors.py             # Обёртка над avito-library детекторами
├── xvfb_manager.py          # Управление виртуальными дисплеями
├── docker-compose.yml       # Конфигурация Docker
├── Dockerfile               # Сборка образа
└── requirements.txt         # Python зависимости
```

## Конфигурация

| Параметр | Описание | Default |
|----------|----------|---------|
| `TOTAL_BROWSER_WORKERS` | Количество браузерных воркеров | 10 |
| `TOTAL_VALIDATION_WORKERS` | Количество воркеров валидации | 2 |
| `CATALOG_BUFFER_SIZE` | Мин. артикулов в буфере для переключения на объявления | 5 |
| `CATALOG_MAX_PAGES` | Макс. страниц каталога (1 стр ≈ 50 объявлений) | 10 |
| `HEARTBEAT_TIMEOUT_SECONDS` | Таймаут зависшей задачи | 1800 |
| `HEARTBEAT_UPDATE_INTERVAL` | Интервал обновления heartbeat | 30 |
| `MIN_PRICE` | Минимальная цена объявления | 1000.0 |
| `MIN_VALIDATED_ITEMS` | Мин. валидных объявлений для артикула | 3 |
| `MIN_SELLER_REVIEWS` | Мин. отзывов продавца | 0 |
| `ENABLE_PRICE_VALIDATION` | Ценовая валидация (IQR) | true |
| `ENABLE_AI_VALIDATION` | AI-валидация (Gemini) | false |
| `REQUIRE_ARTICULUM_IN_TEXT` | Требовать артикул в тексте | false |
| `SKIP_OBJECT_PARSING` | Пропустить парсинг карточек | false |
| `REPARSE_MODE` | Режим повторного парсинга | false |
| `MIN_REPARSE_INTERVAL_HOURS` | Мин. интервал между парсингами | 24 |
| `PROXY_WAIT_TIMEOUT` | Таймаут ожидания прокси | 10 |

## Пайплайн

### Этап 0: Инициализация системы

При запуске `main.py` создаётся `MainProcess`, который выполняет трёхэтапную инициализацию.

**Ключевые функции:**
- `init_xvfb_displays()` (`xvfb_manager.py:135-157`) — создаёт виртуальные дисплеи для каждого браузерного воркера
- `create_pool()` (`database.py:12-14`) — создаёт пул подключений к PostgreSQL
- `spawn_browser_workers()` (`main.py:229-253`) — запускает browser workers как subprocess
- `spawn_validation_workers()` (`main.py:255-279`) — запускает validation workers как subprocess
- `heartbeat_check_loop()` (`heartbeat_manager.py:163-197`) — фоновая проверка зависших задач

**Логика инициализации:**
```python
# main.py:436-489
async def run(self):
    # Этап 1: Xvfb дисплеи (только на Linux)
    init_xvfb_displays()

    # Этап 2: Подключение к БД
    self.pool = await create_pool()

    # Этап 3: Запуск воркеров
    self.heartbeat_task = asyncio.create_task(heartbeat_check_loop(self.pool))
    await self.spawn_browser_workers()
    await self.spawn_validation_workers()

    # Создание задач в зависимости от режима
    if REPARSE_MODE:
        await self.create_object_tasks_for_reparse()
    else:
        asyncio.create_task(self.create_catalog_tasks_from_new_articulums())
        asyncio.create_task(self.create_object_tasks_from_validated_articulums())
```

**Режимы работы:**
1. **Обычный режим** — артикулы берутся из `articulums` в состоянии `NEW`
2. **REPARSE_MODE** — повторный парсинг ранее спарсенных объявлений из `object_data`

### Этап 1: Создание catalog_tasks

В обычном режиме MainProcess создаёт задачи для парсинга каталогов.

**Ключевые функции:**
- `create_catalog_tasks_from_new_articulums()` (`main.py:52-79`) — батчевое создание задач

**SQL-запрос создания задач:**
```sql
-- main.py:74-77
INSERT INTO catalog_tasks (articulum_id, status, checkpoint_page)
VALUES ($1, 'pending', 1)
```

**Важно:** Артикулы остаются в состоянии `NEW` до момента, когда воркер возьмёт задачу. Переход в `CATALOG_PARSING` происходит атомарно при захвате задачи.

### Этап 2: Парсинг каталогов (BrowserWorker)

BrowserWorker динамически выбирает тип задачи на основе "буфера каталогов".

**Логика приоритезации задач:**
```python
# browser_worker.py:674-731
async def main_loop(self):
    while True:
        buffer_size = await self.get_catalog_buffer_size(conn)

        if buffer_size < CATALOG_BUFFER_SIZE:
            # Буфер мал → приоритет каталогам (пополнение)
            task = await acquire_catalog_task(conn, self.worker_id)
            if task:
                await self.process_catalog_task(task)
                continue
            # Fallback на object_tasks
            task = await acquire_object_task(conn, self.worker_id)
        else:
            # Буфер полон → приоритет объявлениям
            task = await acquire_object_task(conn, self.worker_id)
            if task:
                await self.process_object_task(task)
                continue
            # Fallback на catalog_tasks
            task = await acquire_catalog_task(conn, self.worker_id)
```

**Буфер каталогов** (`browser_worker.py:653-672`):
```sql
-- Количество VALIDATED артикулов с pending object_tasks
SELECT COUNT(DISTINCT a.id)
FROM articulums a
WHERE a.state = 'VALIDATED'
  AND EXISTS (
      SELECT 1 FROM object_tasks ot
      WHERE ot.articulum_id = a.id AND ot.status = 'pending'
  )
```

**Захват catalog_task** (`catalog_task_manager.py:37-76`):
```sql
-- Атомарный захват с блокировкой
SELECT ct.*, a.articulum
FROM catalog_tasks ct
JOIN articulums a ON a.id = ct.articulum_id
WHERE ct.status = 'pending' AND a.state = 'NEW'
ORDER BY ct.created_at ASC
LIMIT 1
FOR UPDATE OF ct SKIP LOCKED
```

После захвата задачи:
1. Артикул переводится в `CATALOG_PARSING` (state_machine)
2. Задача переводится в `processing`
3. Запускается heartbeat loop

**Парсинг каталога** (`catalog_parser.py:113-140`):
```python
async def parse_catalog_for_articulum(page, articulum, start_page=1):
    catalog_url = f"https://www.avito.ru/rossiya?q={quote(articulum)}"

    result = await parse_catalog(
        page,
        catalog_url,
        fields=CATALOG_FIELDS,
        max_pages=CATALOG_MAX_PAGES,
        sort="date",
        include_html=CATALOG_INCLUDE_HTML,
        start_page=start_page,
    )
    return result
```

**Обработка результатов** (`browser_worker.py:255-353`):

| Статус | Действие |
|--------|----------|
| `SUCCESS` | Сохраняем listings → CATALOG_PARSED |
| `EMPTY` | 0 объявлений → CATALOG_PARSED |
| `PROXY_BLOCKED` | Блокируем прокси → возврат задачи |
| `CAPTCHA_FAILED` | Освобождаем прокси → возврат задачи |
| `PAGE_NOT_DETECTED` | Помечаем как failed |
| `LOAD_TIMEOUT` | increment_proxy_error → возврат задачи |
| `SERVER_UNAVAILABLE` | Возврат задачи без блокировки прокси |

**Ротация прокси при блокировке:**
```python
# browser_worker.py:383-412
while result.status in {PROXY_BLOCKED, PROXY_AUTH_REQUIRED}:
    proxy_rotations += 1
    await block_proxy(conn, self.current_proxy_id, reason)

    if proxy_rotations >= 10:
        await return_catalog_task_to_queue(conn, task_id)
        break

    await self.recreate_page_with_new_proxy()
    result = await result.continue_from(self.page)  # Resume с checkpoint
```

**Сохранение listings** (`catalog_parser.py:54-110`):
```sql
INSERT INTO catalog_listings (
    articulum_id, avito_item_id, title, price, snippet_text,
    seller_name, seller_id, seller_rating, seller_reviews
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (avito_item_id) DO NOTHING
```

### Этап 3: Валидация (ValidationWorker)

ValidationWorker работает **без браузера** и выполняет 3-этапную фильтрацию объявлений.

**Захват артикула для валидации** (`validation_worker.py:132-158`):
```sql
-- Атомарный захват через UPDATE ... RETURNING
UPDATE articulums
SET state = 'VALIDATING',
    state_updated_at = NOW()
WHERE id = (
    SELECT id FROM articulums
    WHERE state = 'CATALOG_PARSED'
    ORDER BY state_updated_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, articulum, state
```

**Получение объявлений** (`validation_worker.py:160-179`):
```sql
SELECT avito_item_id, title, price, snippet_text,
       seller_name, seller_id, seller_rating, seller_reviews
FROM catalog_listings
WHERE articulum_id = $1
```

#### Проверка #1: Price Filter

**Функция:** `price_filter_validation()` (`validation_worker.py:198-235`)

Фильтрует объявления с ценой ниже `MIN_PRICE`.

```python
if price is None or price < MIN_PRICE:
    await self.save_validation_result(
        articulum_id, avito_item_id,
        'price_filter', False,
        f'Цена {price} < MIN_PRICE {MIN_PRICE}'
    )
```

#### Проверка #2: Mechanical Validation

**Функция:** `mechanical_validation()` (`validation_worker.py:237-377`)

Многоуровневая проверка:

1. **Проверка артикула в тексте** (если `REQUIRE_ARTICULUM_IN_TEXT`):
   ```python
   # Нормализация: русские буквы → английские, удаление спецсимволов
   articulum_normalized = normalize_text_for_articulum_search(articulum)
   if articulum_normalized not in title_normalized and \
      articulum_normalized not in snippet_normalized:
       rejection_reason = f'Артикул "{articulum}" не найден'
   ```

2. **Проверка стоп-слов**:
   ```python
   VALIDATION_STOPWORDS = [
       'копия', 'реплика', 'подделка', 'фейк',
       'б/у', 'бу', 'б у', 'использованный', ...
   ]
   for stopword in VALIDATION_STOPWORDS:
       if stopword.lower() in text_combined:
           rejection_reason = f'Найдено стоп-слово: "{stopword}"'
   ```

3. **Проверка отзывов продавца**:
   ```python
   if MIN_SELLER_REVIEWS > 0:
       if seller_reviews < MIN_SELLER_REVIEWS:
           rejection_reason = f'Отзывов: {seller_reviews} < {MIN_SELLER_REVIEWS}'
   ```

4. **Ценовая валидация (IQR метод)**:
   ```python
   # IQR метод для выбросов
   q1, q3 = statistics.quantiles(prices_sorted, n=4)[0], [2]
   iqr = q3 - q1
   lower_bound = q1 - 1.0 * iqr
   upper_bound = q3 + 1.0 * iqr

   # Фильтрация выбросов
   prices_clean = [p for p in prices if lower_bound <= p <= upper_bound]
   median_clean = statistics.median(prices_clean)

   # Топ-40% для проверки подозрительно дешёвых
   top40_prices = sorted(prices_clean, reverse=True)[:len*2//5]
   median_top40 = statistics.median(top40_prices)

   # Отклонение дешёвых
   if price < median_top40 * 0.5:
       rejection_reason = f'Подозрительно низкая цена'
   ```

#### Проверка #3: AI Validation (опционально)

**Функция:** `ai_validation()` (`validation_worker.py:379-590`)

Если `ENABLE_AI_VALIDATION=true`, объявления проверяются через Gemini/HuggingFace.

**Промпт для AI:**
```
АРТИКУЛ: "{articulum}"

КРИТЕРИИ ОТКЛОНЕНИЯ:
1. Неоригинальные запчасти (аналог, копия, реплика)
2. Подделки (низкая цена, отсутствие упаковки)
3. Несоответствие артикулу

ФОРМАТ ОТВЕТА:
{
  "passed_ids": ["123456", "789012"],
  "rejected": [
    {"id": "345678", "reason": "Аналог, не оригинал"}
  ]
}
```

**Обработка ошибок AI API:**
```python
# validation_worker.py:567-590
except Exception as e:
    self.ai_error_count += 1

    if self.ai_error_count >= 3:
        self.should_shutdown = True
        self.exit_code = 2

    # Артикул возвращается в CATALOG_PARSED для повторной валидации
    raise AIAPIError(f"Ошибка AI API: {e}")
```

**Откат при ошибке API** (`state_machine.py:204-241`):
```sql
-- Атомарный откат в транзакции
UPDATE articulums
SET state = 'CATALOG_PARSED'
WHERE id = $1 AND state = 'VALIDATING';

DELETE FROM validation_results WHERE articulum_id = $1;
```

#### Финализация валидации

После всех проверок:

```python
# validation_worker.py:668-683
async with conn.transaction():
    # VALIDATING → VALIDATED
    await transition_to_validated(conn, articulum_id)

    if not SKIP_OBJECT_PARSING:
        # Создаём object_tasks для прошедших валидацию
        tasks_created = await create_object_tasks_for_articulum(conn, articulum_id)
```

**Создание object_tasks** (`object_task_manager.py:8-66`):
```sql
-- Только для объявлений, прошедших ВСЕ этапы
WITH validated_items AS (
    SELECT DISTINCT avito_item_id
    FROM validation_results
    WHERE articulum_id = $1 AND passed = true
    GROUP BY avito_item_id
    HAVING COUNT(DISTINCT validation_type) = $3
      AND ARRAY_AGG(DISTINCT validation_type) = $4
)
INSERT INTO object_tasks (articulum_id, avito_item_id, status)
SELECT $1, vi.avito_item_id, 'pending'
FROM validated_items vi
WHERE NOT EXISTS (SELECT 1 FROM object_tasks ot WHERE ot.avito_item_id = vi.avito_item_id)
```

### Этап 4: Парсинг карточек объявлений (BrowserWorker)

BrowserWorker также обрабатывает object_tasks для парсинга полных карточек.

**Захват object_task** (`object_task_manager.py:69-101`):
```sql
SELECT ot.*, a.articulum
FROM object_tasks ot
JOIN articulums a ON a.id = ot.articulum_id
WHERE ot.status = 'pending'
ORDER BY ot.created_at ASC
LIMIT 1
FOR UPDATE OF ot SKIP LOCKED
```

**Парсинг карточки** (`browser_worker.py:487-591`):
```python
async def process_object_task(self, task):
    url = f"https://www.avito.ru/{avito_item_id}"
    response = await self.page.goto(url, timeout=150000)

    result = await parse_card(
        self.page, response,
        fields=OBJECT_FIELDS,
        include_html=OBJECT_INCLUDE_HTML,
    )

    if result.status == CardParseStatus.SUCCESS:
        # Проверка б/у в характеристиках
        if self._is_used_condition(card_data.characteristics):
            await invalidate_object_task(conn, task_id, 'б/у')
        else:
            await save_object_data_to_db(conn, articulum_id, avito_item_id, card_data)
            await complete_object_task(conn, task_id)
```

**Проверка б/у** (`browser_worker.py:93-127`):
```python
def _is_used_condition(self, characteristics: dict) -> bool:
    used_variants = ['б/у', 'бу', 'б у', 'б.у.', ...]
    condition_keys = ['состояние', 'condition', 'статус']

    for key, value in characteristics.items():
        if any(cond_key in key.lower() for cond_key in condition_keys):
            if any(variant in value.lower() for variant in used_variants):
                return True
    return False
```

**Сохранение данных** (`object_parser.py:13-67`):
```sql
INSERT INTO object_data (
    articulum_id, avito_item_id, title, price,
    seller_name, published_at, description,
    location_name, characteristics, views_total, raw_html
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING id
```

**Переход состояния:** При первой object_task артикул переводится `VALIDATED → OBJECT_PARSING` (финальное состояние).

## Описание модулей

### main.py

**Назначение:** Оркестратор системы, точка входа.

**Ключевые компоненты:**

#### class MainProcess
Управляет жизненным циклом системы.

**Основные методы:**
- `run()` — главный async entry point с 3-этапной инициализацией
- `spawn_browser_workers()` — запуск N browser workers как subprocess
- `spawn_validation_workers()` — запуск M validation workers
- `monitor_workers()` — мониторинг и перезапуск упавших воркеров
- `create_catalog_tasks_from_new_articulums()` — батчевое создание catalog_tasks
- `create_object_tasks_from_validated_articulums()` — создание object_tasks
- `create_object_tasks_for_reparse()` — создание задач в REPARSE_MODE
- `shutdown()` — graceful shutdown с освобождением ресурсов

**Особенности:**
- Уникальный `CONTAINER_ID` из MD5 хеша hostname для multi-container deployments
- Глобально уникальные `worker_id`: `{CONTAINER_ID}_{local_id}`
- При падении воркера освобождаются его прокси и задачи возвращаются в очередь

### config.py

**Назначение:** Централизованная конфигурация из environment variables.

**Ключевые компоненты:**

#### class ArticulumState
Состояния артикула в state machine:
```
NEW → CATALOG_PARSING → CATALOG_PARSED → VALIDATING → VALIDATED → OBJECT_PARSING
                                                   ↓
                                         REJECTED_BY_MIN_COUNT
```

#### class TaskStatus
Статусы задач: `pending`, `processing`, `completed`, `failed`, `invalid`

### state_machine.py

**Назначение:** Управление жизненным циклом артикулов.

**Ключевые компоненты:**

#### transition_state()
Атомарный переход состояния с защитой от race condition:
```sql
UPDATE articulums
SET state = $2, state_updated_at = NOW()
WHERE id = $1 AND state = $3
```

**Функции переходов:**
- `transition_to_catalog_parsing()` — NEW → CATALOG_PARSING
- `transition_to_catalog_parsed()` — CATALOG_PARSING → CATALOG_PARSED
- `transition_to_validating()` — CATALOG_PARSED → VALIDATING
- `transition_to_validated()` — VALIDATING → VALIDATED
- `transition_to_object_parsing()` — VALIDATED → OBJECT_PARSING
- `reject_articulum()` — VALIDATING → REJECTED_BY_MIN_COUNT
- `rollback_to_catalog_parsed()` — VALIDATING → CATALOG_PARSED (откат при ошибке AI)

### browser_worker.py

**Назначение:** Браузерный парсинг каталогов и карточек.

**Ключевые компоненты:**

#### class BrowserWorker
Воркер с Playwright и прокси.

**Основные методы:**
- `init()` — инициализация pool и Playwright
- `create_browser_with_proxy()` — создание браузера с прокси из пула
- `recreate_page_with_new_proxy()` — ротация прокси при блокировке
- `main_loop()` — главный цикл с динамическим выбором задач
- `process_catalog_task()` — обработка catalog_task
- `process_object_task()` — обработка object_task
- `handle_parse_result()` — обработка результатов парсинга каталога
- `update_heartbeat_loop()` — фоновое обновление heartbeat

**Обработка ошибок:**
- `is_transient_network_error()` — временные ошибки (retry с backoff)
- `is_permanent_proxy_error()` — постоянные проблемы прокси (блокировка)

### validation_worker.py

**Назначение:** Многоэтапная валидация без браузера.

**Ключевые компоненты:**

#### class ValidationWorker

**Основные методы:**
- `run()` — главный цикл с проверкой should_shutdown
- `get_next_articulum()` — атомарный захват артикула
- `validate_articulum()` — оркестратор 3-этапной валидации
- `price_filter_validation()` — фильтр по MIN_PRICE
- `mechanical_validation()` — стоп-слова + IQR ценовая проверка
- `ai_validation()` — AI-валидация через Gemini/HuggingFace

**Нормализация текста:**
```python
def normalize_text_for_articulum_search(text):
    # Замена русских букв на английские
    replacements = {'а': 'a', 'в': 'b', 'е': 'e', 'о': 'o', ...}
    # Удаление спецсимволов
    text = ''.join(char for char in text if char.isalnum())
```

### proxy_manager.py

**Назначение:** Управление пулом прокси с политикой блокировки.

**Ключевые функции:**
- `acquire_proxy()` — атомарный захват свободного прокси
- `acquire_proxy_with_wait()` — захват с ожиданием
- `release_proxy()` — возврат в пул без блокировки
- `block_proxy()` — постоянная блокировка (нет механизма разблокировки)
- `increment_proxy_error()` — три страйка → блокировка
- `reset_proxy_error_counter()` — сброс после успешной задачи

**Политика "три страйка":**
```python
if new_errors >= 3:
    # Блокируем навсегда
    await conn.execute("UPDATE proxies SET is_blocked = TRUE ...")
else:
    # Увеличиваем счётчик и освобождаем
    await conn.execute("UPDATE proxies SET consecutive_errors = $2, is_in_use = FALSE ...")
```

### heartbeat_manager.py

**Назначение:** Восстановление зависших задач.

**Ключевые функции:**
- `heartbeat_check_loop()` — бесконечный цикл проверки
- `check_expired_catalog_tasks()` — возврат зависших catalog_tasks
- `check_expired_object_tasks()` — возврат зависших object_tasks
- `fix_orphaned_catalog_tasks()` — исправление orphaned состояний

**Логика восстановления:**
```sql
-- Находим зависшие задачи
SELECT id, worker_id FROM catalog_tasks
WHERE status = 'processing'
  AND heartbeat_at < NOW() - INTERVAL '1800 seconds'

-- Освобождаем прокси
UPDATE proxies SET is_in_use = FALSE WHERE worker_id = $1

-- Возвращаем артикул в NEW
UPDATE articulums SET state = 'NEW' WHERE id = $1 AND state = 'CATALOG_PARSING'

-- Возвращаем задачу в очередь
UPDATE catalog_tasks SET status = 'pending' WHERE id = $1
```

### network_error_handler.py

**Назначение:** Классификация сетевых ошибок.

**Transient errors (retry):**
- `ERR_CONNECTION_CLOSED` — TCP FIN
- `ERR_CONNECTION_RESET` — TCP RST
- `ERR_TIMED_OUT` — таймаут
- `ERR_EMPTY_RESPONSE` — сервер закрыл без данных

**Permanent errors (блокировка прокси):**
- `ERR_PROXY_CONNECTION_FAILED` — прокси недоступен
- `ERR_TUNNEL_CONNECTION_FAILED` — CONNECT туннель не удался
- `407 Proxy Authentication Required`

### detectors.py

**Назначение:** Обёртка над avito-library детекторами.

**Константы детекторов:**
- `CATALOG_DETECTOR_ID` — страница каталога
- `CARD_FOUND_DETECTOR_ID` — карточка загружена
- `PROXY_BLOCK_403_DETECTOR_ID` — HTTP 403
- `CAPTCHA_DETECTOR_ID` — Geetest капча
- `REMOVED_DETECTOR_ID` — объявление снято

**Вспомогательные функции:**
- `is_success_state()` — можно продолжать парсинг
- `is_proxy_block()` — нужна блокировка прокси
- `is_captcha_state()` — требуется решение капчи

### xvfb_manager.py

**Назначение:** Виртуальные X11 дисплеи для Linux.

**Ключевые функции:**
- `should_use_xvfb()` — auto-detection (только Linux)
- `init_xvfb_displays()` — создание N дисплеев при старте
- `create_xvfb_display()` — запуск одного Xvfb процесса
- `get_display_env()` — строка DISPLAY для воркера
- `cleanup_displays()` — graceful shutdown всех Xvfb

**Маппинг worker_id → DISPLAY:**
```
worker_id=1 → DISPLAY=:100 (XVFB_DISPLAY_START=99)
worker_id=2 → DISPLAY=:101
...
```

## State Machine

```
┌─────┐     acquire_catalog_task()     ┌─────────────────┐
│ NEW │ ─────────────────────────────► │ CATALOG_PARSING │
└─────┘                                └────────┬────────┘
                                                │
                          complete_catalog_task()
                                                ▼
                                       ┌────────────────┐
                                       │ CATALOG_PARSED │
                                       └───────┬────────┘
                                               │
                              get_next_articulum() (ValidationWorker)
                                               ▼
                                       ┌────────────┐
                              ┌────────│ VALIDATING │────────┐
                              │        └────────────┘        │
                              │                              │
              validation passed                    < MIN_VALIDATED_ITEMS
              (transition_to_validated)            (reject_articulum)
                              ▼                              ▼
                       ┌───────────┐              ┌──────────────────────┐
                       │ VALIDATED │              │ REJECTED_BY_MIN_COUNT│
                       └─────┬─────┘              └──────────────────────┘
                             │                         (финальное)
         acquire_object_task() + transition_to_object_parsing()
                             ▼
                    ┌────────────────┐
                    │ OBJECT_PARSING │
                    └────────────────┘
                         (финальное)
```

**Особый переход — откат при ошибке AI:**
```
VALIDATING → CATALOG_PARSED (rollback_to_catalog_parsed)
+ DELETE FROM validation_results WHERE articulum_id = $1
```

## Обработка ошибок

### Сетевые ошибки

| Тип | Примеры | Действие |
|-----|---------|----------|
| Transient | ERR_CONNECTION_CLOSED, TIMED_OUT | increment_proxy_error() (3 страйка → блокировка) |
| Permanent | ERR_PROXY_CONNECTION_FAILED, 407 | block_proxy() сразу |

### Ошибки парсинга

| Статус | Действие |
|--------|----------|
| SUCCESS | complete_task(), reset_proxy_error_counter() |
| PROXY_BLOCKED | block_proxy(), return_task_to_queue() |
| CAPTCHA_FAILED | release_proxy(), return_task_to_queue() |
| PAGE_NOT_DETECTED | fail_task() |
| SERVER_UNAVAILABLE | return_task_to_queue() (без блокировки прокси) |

### Ошибки валидации

| Ситуация | Действие |
|----------|----------|
| AI API Error | rollback_to_catalog_parsed() + удаление validation_results |
| 3+ AI ошибок подряд | shutdown воркера с exit_code=2 |
| < MIN_VALIDATED_ITEMS | reject_articulum() |

## Диаграмма потока данных

```
                    ┌──────────────┐
                    │  articulums  │
                    │   (state)    │
                    └──────┬───────┘
                           │ NEW
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    MainProcess                               │
│  create_catalog_tasks_from_new_articulums()                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
                   ┌───────────────┐
                   │ catalog_tasks │
                   │   (pending)   │
                   └───────┬───────┘
                           │
           ┌───────────────┴───────────────┐
           │        BrowserWorker          │
           │   acquire_catalog_task()      │
           │   parse_catalog()             │
           └───────────────┬───────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ catalog_listings│
                  │ (avito_item_id) │
                  └────────┬────────┘
                           │
                           ▼
             ┌──────────────────────────┐
             │    ValidationWorker      │
             │  price_filter_validation │
             │  mechanical_validation   │
             │  ai_validation           │
             └────────────┬─────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│  VALIDATED    │ │   REJECTED    │ │validation_    │
│ + object_tasks│ │               │ │   results     │
└───────┬───────┘ └───────────────┘ └───────────────┘
        │
        │  BrowserWorker
        │  acquire_object_task()
        │  parse_card()
        ▼
┌───────────────┐
│  object_data  │
│ (full details)│
└───────────────┘
```

## Чек-лист

- [x] Описаны все этапы пайплайна от запуска до завершения
- [x] Описаны все модули в scope
- [x] Для каждого модуля перечислены ключевые функции/классы
- [x] Описана логика ветвления для критичных мест
- [x] Описаны все состояния state machine
- [x] Описана обработка основных ошибок
- [x] Есть диаграмма потока данных
- [x] Есть таблица конфигурации
