# План миграции container/ на актуальный avito-library API

Источник истины: `docs/avito-library-docs.md`.

## Ключевые изменения API (что поменялось в библиотеке)

- Каталог:
  - Было: `avito_library.parsers.catalog_parser.parse_catalog_until_complete(...)` + `wait_for_page_request()/supply_page()`
  - Стало: `from avito_library import parse_catalog` -> `CatalogParseResult`:
    - `status: CatalogParseStatus`
    - `listings`, `meta`
    - `resume_url`, `resume_page_number`
    - `continue_from(new_page)` для продолжения после смены прокси

- Карточка:
  - Было: синхронный HTML-парсер `parse_card(html, ensure_card=True, ...)`
  - Стало: `await parse_card(page, last_response, fields=..., include_html=...)` -> `CardParseResult(status, data)`

- Поля:
  - В каталоге: `snippet_text` (не `snippet`)
  - В карточке: структура `seller` и `location` изменилась (см. доку)

- Статусы каталога (актуальная дока):
  - `SUCCESS`, `PROXY_BLOCKED`, `PROXY_AUTH_REQUIRED`, `PAGE_NOT_DETECTED`, `LOAD_TIMEOUT`, `CAPTCHA_FAILED`, `WRONG_PAGE`
  - Важно: в доке нет `EMPTY` — пустой каталог нужно трактовать по фактическому поведению (`SUCCESS` + 0 listings?) и зафиксировать правила.

## Где сейчас используется старый API (что менять)

- `container/config.py`
  - `CATALOG_FIELDS` содержит `snippet` (устарело) -> заменить на `snippet_text`.

- `container/catalog_parser.py`
  - заменить `parse_catalog_until_complete` на `parse_catalog`
  - заменить `sort_by_date=True` на `sort="date"`
  - решить, возвращать ли `CatalogParseResult` целиком или по-старому `(listings, meta)`.

- `container/browser_worker.py`
  - удалить механику `page_provider_loop` и импорты `wait_for_page_request/supply_page`
  - перестроить `process_catalog_task` под `CatalogParseResult` + `continue_from(new_page)`
  - обновить `handle_parse_result` под актуальные `CatalogParseStatus`
  - переписать object-flow на `await parse_card(page, response, ...)` и ветвление по `CardParseStatus`

- `container/object_parser.py`
  - перестать использовать `CardData` из внутреннего модуля
  - обновить маппинг `seller/location` под актуальную структуру
  - решить вопрос БД:
    - либо писать отсутствующие поля как `NULL`
    - либо мигрировать схему (добавить `seller_profile_url`, `location_address/region/metro` и т.д.)

- `container/detectors.py`, `container/detector_handler.py`
  - после перехода на новый API минимизировать/убрать ручную обработку капчи (во избежание дублей)
  - если `detect_page_state` остаётся, всегда прокидывать корректный `last_response`
  - убрать/починить `press_continue_and_detect` (сейчас в `__all__` есть, но функции нет)

- `container/requirements.txt`
  - зафиксировать версию avito-library (тег/commit), совместимую с `docs/avito-library-docs.md` (вместо `@main`).

## Рекомендуемая целевая логика (высокоуровнево)

### Catalog task

1) `result = await parse_catalog(page, url=..., fields=..., max_pages=..., start_page=..., sort="date", include_html=..., condition="Новый")`
2) Если `result.status in {PROXY_BLOCKED, PROXY_AUTH_REQUIRED}`:
   - block proxy в БД
   - создать новый браузер/контекст/страницу с другим прокси
   - `result = await result.continue_from(new_page)`
   - продолжать пока не получим финальный статус или лимит смен прокси
3) Иначе:
   - `SUCCESS`: сохранить `result.listings`, завершить задачу
   - `CAPTCHA_FAILED`: вернуть задачу и прокси в пулы (или ваша политика)
   - `LOAD_TIMEOUT`: transient -> инкремент ошибок прокси + вернуть задачу
   - `PAGE_NOT_DETECTED` / `WRONG_PAGE`: fail (или вернуть, если решите что transient)

### Object task

1) `response = await page.goto(url)`
2) (опционально) ваш server-error детектор 502/503/504 (если нужен)
3) `card_result = await parse_card(page, response, fields=..., include_html=..., max_captcha_attempts=...)`
4) По `card_result.status`:
   - `SUCCESS`: сохранить данные, complete task
   - `PROXY_BLOCKED`: block proxy + вернуть задачу
   - `CAPTCHA_FAILED`: вернуть задачу + прокси в пул
   - `NOT_FOUND`: invalidate task
   - `PAGE_NOT_DETECTED` / `WRONG_PAGE`: fail task

## Обновить внутреннюю документацию проекта

Файлы `dont-know/*.md`, где упоминается `parse_catalog_until_complete` и `page_provider`, обновить на `parse_catalog` + `continue_from`.

## Чек-лист при приемке

- Каталог:
  - корректно обновляется checkpoint (если вы продолжаете хранить page_number)
  - корректно отрабатывает смена прокси и продолжение с `continue_from`
  - корректно сохраняется `snippet_text` в БД
- Карточка:
  - не происходит двойного решения капчи (ваша логика + библиотека)
  - корректно маппятся `seller/location/characteristics/views_total`
- Статусы:
  - нет упоминаний `CAPTCHA_UNSOLVED`, `NOT_DETECTED`, `EMPTY` в коде (или они осмысленно замаплены)

## Обязательный фильтр состояния товара (condition="Новый")

В актуальной документации avito-library добавлен фильтр `condition` для `parse_catalog`:

- допустимые значения: `"С пробегом"` или `"Новый"`
- соответствующие URL-сегменты: `/s_probegom`, `/novyy`

Требование для нашей системы: при парсинге каталога всегда выставлять `condition="Новый"` (т.е. искать только новые товары), чтобы не собирать б/у и не тратить ресурсы на последующую фильтрацию.
