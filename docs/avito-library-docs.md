# avito-library

Асинхронная Playwright-библиотека для парсинга Avito с автоматическим решением капчи.

## Установка

```bash
pip install git+https://github.com/Stepan2222000/avito-library.git
```

После установки необходимо установить браузер Chromium:

```bash
avito-install-chromium
```

Или программно:

```python
from avito_library import install_playwright_chromium
install_playwright_chromium()
```

## Требования

- Python >= 3.11
- playwright
- beautifulsoup4, lxml
- numpy, opencv-python
- httpx >= 0.25.0
- asyncpg

---

## Быстрый старт

### Простой парсинг каталога

```python
import asyncio
from playwright.async_api import async_playwright
from avito_library import parse_catalog, CatalogParseStatus

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch()
        page = await browser.new_page()

        # Парсим каталог телефонов
        result = await parse_catalog(
            page,
            category="telefony",
            city="moskva",
            fields=["item_id", "title", "price"],
            max_pages=3,
        )

        if result.status in {CatalogParseStatus.SUCCESS, CatalogParseStatus.EMPTY}:
            if not result.listings:
                print("Каталог пуст")
            else:
                for listing in result.listings:
                    print(f"{listing.item_id}: {listing.title} - {listing.price} руб.")

        await browser.close()

asyncio.run(main())
```

### Парсинг автомобилей с фильтрами

```python
import asyncio
from playwright.async_api import async_playwright
from avito_library import parse_catalog, CatalogParseStatus

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)  # headless=False для отладки
        page = await browser.new_page()

        # Парсим BMW седаны 2018+ года с полным приводом
        result = await parse_catalog(
            page,
            category="avtomobili",
            city="moskva",
            brand="bmw",
            body_type="Седан",
            year_from=2018,
            drive=["Полный"],
            price_min=1_000_000,
            price_max=3_000_000,
            sort="date",
            fields=["item_id", "title", "price", "location_city"],
            max_pages=5,
        )

        if result.status in {CatalogParseStatus.SUCCESS, CatalogParseStatus.EMPTY}:
            if not result.listings:
                print("Каталог пуст")
            else:
                print(f"Найдено {len(result.listings)} объявлений")
                for listing in result.listings:
                    price_str = f"{listing.price:,} руб." if listing.price else "Цена не указана"
                    print(f"{listing.title} - {price_str}")
        else:
            print(f"Ошибка: {result.status}")

        await browser.close()

asyncio.run(main())
```

### Продолжение после блокировки прокси

```python
# При блокировке прокси — создаём новый browser context с другим прокси
if result.status == CatalogParseStatus.PROXY_BLOCKED:
    # Важно: прокси задаётся на уровне browser context, не page
    new_context = await browser.new_context(proxy={"server": "http://new-proxy:8080"})
    new_page = await new_context.new_page()
    result = await result.continue_from(new_page)
```

---

## Детекторы состояния страницы

Детекторы определяют текущее состояние страницы Avito и позволяют правильно обработать капчу, блокировки и другие состояния.

### Функция detect_page_state

Главная функция детектирования. Проверяет детекторы по приоритету и возвращает ID первого сработавшего.

```python
async def detect_page_state(
    page: Page,
    *,
    skip: Iterable[str] | None = None,
    priority: Sequence[str] | None = None,
    detector_kwargs: Mapping[str, Mapping[str, object]] | None = None,
    last_response: Response | None = None,
) -> str
```

**Параметры:**

| Параметр | Тип | Описание |
|----------|-----|----------|
| `page` | `Page` | Playwright-страница |
| `skip` | `Iterable[str]` | Какие детекторы пропустить |
| `priority` | `Sequence[str]` | Свой порядок приоритетов (вместо стандартного) |
| `detector_kwargs` | `Mapping` | Параметры для конкретных детекторов |
| `last_response` | `Response` | HTTP-ответ (для детекторов блокировки прокси) |

**Возвращает:** `str` — ID сработавшего детектора или `NOT_DETECTED_STATE_ID`

**Поведение:**
- Проверяет детекторы в порядке приоритета
- Если ничего не сработало — ждёт 20 секунд и повторяет (до 3 повторов)
- При неудаче возвращает `NOT_DETECTED_STATE_ID`

**Пример:**

```python
from avito_library import detect_page_state, CATALOG_DETECTOR_ID

response = await page.goto("https://www.avito.ru/moskva/telefony")
state = await detect_page_state(page, last_response=response)

if state == CATALOG_DETECTOR_ID:
    print("Страница каталога загружена")
```

### Константы ID детекторов

| Константа | Значение | Описание |
|-----------|----------|----------|
| `PROXY_BLOCK_403_DETECTOR_ID` | `"proxy_block_403_detector"` | **Блокировка прокси.** HTTP 403 или блокировка IP. Необходимо сменить прокси. |
| `SERVER_ERROR_5XX_DETECTOR_ID` | `"server_error_5xx_detector"` | Серверная ошибка (HTTP 502/503/504). Парсеры автоматически делают retry. |
| `PROXY_BLOCK_429_DETECTOR_ID` | `"proxy_block_429_detector"` | Rate limit (HTTP 429). Часто сопровождается капчей. |
| `PROXY_AUTH_DETECTOR_ID` | `"proxy_auth_407_detector"` | **Блокировка прокси.** HTTP 407 — прокси требует авторизацию. Необходимо сменить прокси или исправить авторизацию. |
| `CAPTCHA_DETECTOR_ID` | `"captcha_geetest_detector"` | Geetest-капча |
| `REMOVED_DETECTOR_ID` | `"removed_or_not_found_detector"` | HTTP 404/410 или удалённое объявление |
| `SELLER_PROFILE_DETECTOR_ID` | `"seller_profile_detector"` | Профиль продавца |
| `CATALOG_DETECTOR_ID` | `"catalog_page_detector"` | Страница каталога |
| `CARD_FOUND_DETECTOR_ID` | `"card_found_detector"` | Карточка объявления |
| `CONTINUE_BUTTON_DETECTOR_ID` | `"continue_button_detector"` | Кнопка "Продолжить" |
| `UNKNOWN_PAGE_DETECTOR_ID` | `"unknown_page_detector"` | Известный edge case (журнал, редакционная страница) |
| `NOT_DETECTED_STATE_ID` | `"not_detected"` | Ничего не определено |

### Обработка блокировки прокси

При срабатывании `PROXY_BLOCK_403_DETECTOR_ID` или `PROXY_AUTH_DETECTOR_ID` необходимо сменить прокси:

```python
from avito_library import (
    detect_page_state,
    PROXY_BLOCK_403_DETECTOR_ID,
    PROXY_AUTH_DETECTOR_ID,
)

PROXY_BLOCKED_STATES = {PROXY_BLOCK_403_DETECTOR_ID, PROXY_AUTH_DETECTOR_ID}

state = await detect_page_state(page, last_response=response)

if state in PROXY_BLOCKED_STATES:
    # Прокси заблокирован — создаём новый context с другим прокси
    await page.close()
    new_context = await browser.new_context(proxy={"server": "http://new-proxy:8080"})
    page = await new_context.new_page()
```

### Порядок приоритетов по умолчанию

```python
DETECTOR_DEFAULT_ORDER = (
    "proxy_block_403_detector",      # 1. Блокировка прокси (403)
    "server_error_5xx_detector",     # 2. Серверные ошибки (502/503/504)
    "proxy_block_429_detector",      # 3. Rate limit (429)
    "proxy_auth_407_detector",       # 4. Блокировка прокси (407)
    "captcha_geetest_detector",      # 5. Geetest-капча
    "removed_or_not_found_detector", # 6. Удалённое объявление
    "seller_profile_detector",       # 7. Профиль продавца
    "catalog_page_detector",         # 8. Каталог
    "card_found_detector",           # 9. Карточка
    "continue_button_detector",      # 10. Кнопка "Продолжить"
    "unknown_page_detector",         # 11. Известные edge cases (журнал и т.д.)
)
```

### Когда вызывать resolve_captcha_flow

При срабатывании следующих детекторов необходимо вызвать `resolve_captcha_flow()`:

- `CONTINUE_BUTTON_DETECTOR_ID` — кнопка "Продолжить" перед капчей
- `CAPTCHA_DETECTOR_ID` — Geetest-капча
- `PROXY_BLOCK_429_DETECTOR_ID` — rate limit (часто сопровождается капчей)

```python
from avito_library import (
    detect_page_state,
    resolve_captcha_flow,
    CAPTCHA_DETECTOR_ID,
    CONTINUE_BUTTON_DETECTOR_ID,
    PROXY_BLOCK_429_DETECTOR_ID,
)

CAPTCHA_STATES = {CAPTCHA_DETECTOR_ID, CONTINUE_BUTTON_DETECTOR_ID, PROXY_BLOCK_429_DETECTOR_ID}

state = await detect_page_state(page, last_response=response)

if state in CAPTCHA_STATES:
    html, solved = await resolve_captcha_flow(page)
    if not solved:
        raise RuntimeError("Капча не решена")
```

---

## Парсинг каталога

### Функция parse_catalog

Парсит страницы каталога с автоматической пагинацией, фильтрацией, решением капчи и возможностью продолжения после ошибок.

```python
async def parse_catalog(
    page: Page,
    url: str | None = None,
    *,
    # Параметры для построения URL
    city: str | None = None,
    category: str | None = None,
    brand: str | None = None,
    model: str | None = None,
    body_type: str | None = None,
    fuel_type: str | None = None,
    transmission: list[str] | None = None,
    condition: str | None = None,
    # GET-параметры
    price_min: int | None = None,
    price_max: int | None = None,
    radius: int | None = None,
    sort: str | None = None,
    # Механические фильтры (применяются через UI)
    year_from: int | None = None,
    year_to: int | None = None,
    mileage_from: int | None = None,
    mileage_to: int | None = None,
    engine_volumes: list[float] | None = None,
    drive: list[str] | None = None,
    power_from: int | None = None,
    power_to: int | None = None,
    turbo: bool | None = None,
    seller_type: str | None = None,
    # Параметры парсинга
    fields: Iterable[str],
    max_pages: int | None = None,
    start_page: int = 1,
    single_page: bool = False,
    include_html: bool = False,
    max_captcha_attempts: int = 30,
    load_timeout: int = 180_000,
    load_retries: int = 5,
) -> CatalogParseResult
```

**Два способа вызова:**

1. **С параметрами фильтрации** (рекомендуется):
   ```python
   result = await parse_catalog(
       page,
       category="avtomobili",
       city="moskva",
       brand="bmw",
       year_from=2018,
       fields=["item_id", "title", "price"],
   )
   ```

2. **С готовым URL:**
   ```python
   result = await parse_catalog(
       page,
       url="https://www.avito.ru/moskva/avtomobili/bmw",
       fields=["item_id", "title", "price"],
   )
   ```

### Параметры фильтрации

#### Основные параметры

| Параметр | Тип | Описание |
|----------|-----|----------|
| `page` | `Page` | Playwright-страница |
| `url` | `str \| None` | Готовый URL каталога (опционально) |
| `category` | `str` | Slug категории. **Обязателен если url не передан!** |
| `city` | `str \| None` | Slug города. `None` = все регионы (`all`) |
| `fields` | `Iterable[str]` | Поля для извлечения: `item_id`, `title`, `price`, `snippet_text`, `location_city`, `location_area`, `location_extra`, `seller_name`, `seller_id`, `seller_rating`, `seller_reviews`, `promoted`, `published_ago`, `raw_html` |

#### URL-фильтры (ЧПУ-сегменты)

Эти фильтры добавляются в URL как человекопонятные сегменты.

| Параметр | Тип | Описание | Пример URL |
|----------|-----|----------|------------|
| `brand` | `str` | Slug марки | `/bmw`, `/toyota` |
| `model` | `str` | Slug модели | `/bmw/x5` |
| `body_type` | `str` | Тип кузова (русский) | `/sedan` |
| `fuel_type` | `str` | Тип топлива (русский) | `/benzin` |
| `transmission` | `list[str]` | Коробка (если 1 значение) | `/mekhanika` |
| `condition` | `str` | Состояние: `"С пробегом"` или `"Новый"` | `/s_probegom`, `/novyy` |

#### GET-параметры

| Параметр | Тип | Описание |
|----------|-----|----------|
| `price_min` | `int` | Минимальная цена (рубли) |
| `price_max` | `int` | Максимальная цена (рубли) |
| `radius` | `int` | Радиус поиска: 0 (только город), 50, 100, 200, 300, 500 км |
| `sort` | `str` | Сортировка (см. ниже) |

#### Механические фильтры

Эти фильтры применяются через взаимодействие с UI страницы (Playwright кликает на элементы).

| Параметр | Тип | Описание |
|----------|-----|----------|
| `year_from` | `int` | Год выпуска от |
| `year_to` | `int` | Год выпуска до |
| `mileage_from` | `int` | Пробег от (км) |
| `mileage_to` | `int` | Пробег до (км) |
| `engine_volumes` | `list[float]` | Объёмы двигателя: `[2.0, 2.5]` |
| `transmission` | `list[str]` | Коробка (если 2+ значений) |
| `drive` | `list[str]` | Тип привода |
| `power_from` | `int` | Мощность от (л.с.) |
| `power_to` | `int` | Мощность до (л.с.) |
| `turbo` | `bool` | Наличие турбины |
| `seller_type` | `str` | Тип продавца |

#### Параметры парсинга

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `max_pages` | `int \| None` | `None` | Лимит страниц (None = без лимита) |
| `start_page` | `int` | `1` | Начальная страница |
| `single_page` | `bool` | `False` | Режим одной страницы (см. ниже) |
| `include_html` | `bool` | `False` | Сохранять HTML карточек |
| `max_captcha_attempts` | `int` | `30` | Макс. попыток решения капчи |
| `load_timeout` | `int` | `180000` | Таймаут загрузки страницы (мс) |
| `load_retries` | `int` | `5` | Повторов при таймауте |

### Допустимые значения фильтров

#### body_type (тип кузова)

```
Седан, Хэтчбек, Универсал, Внедорожник, Кроссовер, Купе,
Кабриолет, Пикап, Минивэн, Лимузин, Фургон
```

#### fuel_type (тип топлива)

```
Бензин, Дизель, Электро, Гибрид, Газ
```

#### transmission (коробка передач)

```
Механика, Автомат, Робот, Вариатор
```

#### condition (состояние)

```
С пробегом, Новый
```

#### drive (тип привода)

```
Передний, Задний, Полный
```

#### seller_type (тип продавца)

```
Все, Дилеры, Частные
```

#### engine_volumes (объём двигателя)

```
0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9,
2.0, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2, 3.3,
3.4, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0, 6.5, 7.0
```

### Сортировка каталога

| Значение | Описание |
|----------|----------|
| `"date"` | По дате публикации (сначала новые) |
| `"price_asc"` | По цене (сначала дешёвые) |
| `"price_desc"` | По цене (сначала дорогие) |
| `"mileage_asc"` | По пробегу (для автомобилей) |

### Примеры использования

**Простой парсинг:**

```python
result = await parse_catalog(
    page,
    category="telefony",
    city="moskva",
    fields=["item_id", "title", "price"],
    max_pages=5,
)
```

**С фильтрами автомобилей:**

```python
result = await parse_catalog(
    page,
    category="avtomobili",
    city="moskva",
    brand="bmw",
    body_type="Седан",
    transmission=["Автомат", "Робот"],  # 2+ значений → механически
    year_from=2018,
    drive=["Полный"],
    price_min=1_000_000,
    sort="date",
    fields=["item_id", "title", "price", "location_city"],
    max_pages=10,
)
```

**С готовым URL + дополнительные фильтры:**

```python
result = await parse_catalog(
    page,
    url="https://www.avito.ru/moskva/avtomobili/bmw",
    body_type="Седан",
    year_from=2020,
    fields=["item_id", "title", "price"],
)
```

**Фильтр по состоянию (только новые объявления):**

```python
result = await parse_catalog(
    page,
    category="avtomobili",
    city="moskva",
    brand="bmw",
    condition="Новый",  # Только новые объявления (не б/у)
    fields=["item_id", "title", "price"],
    max_pages=10,
)
```

### Режим single_page

Режим `single_page=True` предназначен для упрощённого парсинга одной страницы каталога без инфраструктуры продолжения.

**Когда использовать:**
- Нужна только одна страница результатов
- Не нужна возможность автоматического продолжения при ошибках
- Хочется упрощённый API без лишних полей

**Поведение:**
- Парсится ровно одна страница (первая)
- Приватные поля результата остаются пустыми
- `resume_url` всегда `None`
- Метод `continue_from()` выбрасывает `ValueError`

**Пример:**

```python
result = await parse_catalog(
    page,
    url="https://www.avito.ru/moskva/telefony",
    fields=["item_id", "title", "price"],
    single_page=True,
)

if result.status in {CatalogParseStatus.SUCCESS, CatalogParseStatus.EMPTY}:
    print(f"Спарсено {len(result.listings)} карточек")
else:
    print(f"Ошибка: {result.status}")
```

**С фильтрами:**

```python
result = await parse_catalog(
    page,
    category="avtomobili",
    city="moskva",
    brand="bmw",
    year_from=2018,
    drive=["Полный"],
    fields=["item_id", "title", "price"],
    single_page=True,
)
```

**Ограничения:**

При `single_page=True` запрещены:
- `max_pages` — выбросит `ValueError("max_pages нельзя указывать при single_page=True")`
- `start_page > 1` — выбросит `ValueError("start_page нельзя указывать при single_page=True")`

**Сравнение single_page=True vs max_pages=1:**

| Аспект | `single_page=True` | `max_pages=1` |
|--------|-------------------|---------------|
| Количество страниц | 1 | 1 |
| `continue_from()` | Недоступен (ValueError) | Доступен |
| `resume_url` | Всегда `None` | Заполнен при ошибке |
| Использование памяти | Меньше | Больше |
| Сценарий | Быстро спарсить одну страницу | Парсить с возможностью продолжения |

### Модель CatalogParseResult

Результат парсинга каталога.

| Поле | Тип | Описание |
|------|-----|----------|
| `status` | `CatalogParseStatus` | Статус парсинга |
| `listings` | `list[CatalogListing]` | Собранные карточки |
| `meta` | `CatalogParseMeta` | Метаинформация |
| `error_state` | `str \| None` | ID детектора при ошибке |
| `error_url` | `str \| None` | URL, где произошла ошибка |
| `resume_url` | `str \| None` | URL для продолжения |
| `resume_page_number` | `int \| None` | Номер страницы для продолжения |

**Метод continue_from():**

Позволяет продолжить парсинг с новой страницей (например, с другим прокси после блокировки):

```python
async def continue_from(
    self,
    new_page: Page,
    skip_navigation: bool | None = None,
) -> CatalogParseResult
```

```python
# При блокировке прокси — см. пример в разделе "Быстрый старт"
if result.status == CatalogParseStatus.PROXY_BLOCKED:
    new_context = await browser.new_context(proxy={"server": "http://new-proxy:8080"})
    new_page = await new_context.new_page()
    result = await result.continue_from(new_page)
```

**Ограничение:** Метод недоступен при `single_page=True` — выбросит `ValueError("Невозможно продолжить парсинг: результат получен в режиме single_page")`.

### Модель CatalogParseMeta

Метаинформация по итогам парсинга каталога.

| Поле | Тип | Описание |
|------|-----|----------|
| `status` | `CatalogParseStatus` | Статус парсинга |
| `processed_pages` | `int` | Количество обработанных страниц |
| `processed_cards` | `int` | Количество обработанных карточек |
| `last_state` | `str \| None` | ID последнего детектированного состояния |
| `details` | `str \| None` | Дополнительная информация об ошибке |
| `last_url` | `str \| None` | URL последней обработанной страницы |

### Enum CatalogParseStatus

| Статус | Описание |
|--------|----------|
| `SUCCESS` | Успешно завершено |
| `EMPTY` | Каталог пуст (0 объявлений) |
| `PROXY_BLOCKED` | **Блокировка прокси** (HTTP 403). Необходимо сменить прокси. |
| `PROXY_AUTH_REQUIRED` | **Блокировка прокси** (HTTP 407). Необходимо сменить прокси. |
| `PAGE_NOT_DETECTED` | Состояние не определено |
| `LOAD_TIMEOUT` | Таймаут загрузки |
| `CAPTCHA_FAILED` | Капча не решена |
| `WRONG_PAGE` | Открыта не та страница |
| `SERVER_UNAVAILABLE` | Сервер недоступен (HTTP 502/503/504). Retry исчерпаны. |

### Модель CatalogListing

Карточка объявления из каталога.

| Поле | Тип | Описание |
|------|-----|----------|
| `item_id` | `str` | ID объявления |
| `title` | `str \| None` | Заголовок |
| `price` | `int \| None` | Цена в рублях |
| `snippet_text` | `str \| None` | Краткое описание |
| `location_city` | `str \| None` | Город |
| `location_area` | `str \| None` | Район |
| `location_extra` | `str \| None` | Доп. информация о локации |
| `seller_name` | `str \| None` | Имя продавца |
| `seller_id` | `str \| None` | ID продавца |
| `seller_rating` | `float \| None` | Рейтинг продавца |
| `seller_reviews` | `int \| None` | Количество отзывов |
| `promoted` | `bool` | Продвигаемое объявление |
| `published_ago` | `str \| None` | "2 дня назад" |
| `raw_html` | `str \| None` | HTML карточки |

### Функция navigate_to_catalog

Переход на страницу каталога с применением сортировки и пагинации.

```python
async def navigate_to_catalog(
    page: Page,
    catalog_url: str,
    *,
    sort: str | None = None,
    start_page: int = 1,
    timeout: int = 180_000,
    wait_until: str = "domcontentloaded",  # "load", "domcontentloaded", "networkidle", "commit"
) -> Response
```

**Пример:**

```python
from avito_library import navigate_to_catalog

response = await navigate_to_catalog(
    page,
    "https://www.avito.ru/moskva/telefony",
    sort="price_asc",  # Сначала дешёвые
    start_page=2,
)
```

### Функция parse_single_page

Низкоуровневая функция для парсинга одной страницы каталога.

```python
async def parse_single_page(
    page: Page,
    *,
    fields: Iterable[str],
    include_html: bool = False,
    max_captcha_attempts: int = 30,
) -> SinglePageResult
```

**Возвращает:** `SinglePageResult` с полями:
- `status` — статус парсинга
- `cards` — список `CatalogListing`
- `has_next` — есть ли следующая страница
- `next_url` — URL следующей страницы
- `error_state` — ID детектора при ошибке
- `error_url` — URL, где произошла ошибка

---

## Парсинг карточки объявления

### Функция parse_card

Парсит карточку объявления с автоматической обработкой состояний страницы (капча, блокировки, ошибки).

```python
async def parse_card(
    page: Page,
    last_response: Response,
    *,
    fields: Iterable[str],
    max_captcha_attempts: int = 30,
    include_html: bool = False,
) -> CardParseResult
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `page` | `Page` | — | Playwright Page с уже открытой страницей карточки |
| `last_response` | `Response` | — | Response от навигации (`page.goto()`) |
| `fields` | `Iterable[str]` | — | Поля для извлечения |
| `max_captcha_attempts` | `int` | `30` | Максимум попыток решения капчи |
| `include_html` | `bool` | `False` | Сохранять raw_html |

**Доступные поля:** `title`, `price`, `seller`, `item_id`, `published_at`, `description`, `location`, `characteristics`, `views_total`, `images`

**Возвращает:** `CardParseResult`

**Как работает:**

1. Воркер делает навигацию на страницу карточки (`page.goto()`)
2. Воркер передаёт `page` и `response` в `parse_card`
3. Функция автоматически определяет состояние страницы
4. Если капча — решает её (до `max_captcha_attempts` попыток)
5. Возвращает результат со статусом и данными

**Пример:**

```python
from avito_library import parse_card, CardParseStatus

# Воркер делает навигацию
response = await page.goto("https://www.avito.ru/moskva/telefony/iphone_15_123456")

# Вызывает parse_card
result = await parse_card(
    page,
    response,
    fields=["title", "price", "description", "images"],
)

# Обрабатывает результат
if result.status == CardParseStatus.SUCCESS:
    print(f"{result.data.title}: {result.data.price} руб.")
    print(f"Изображений: {len(result.data.images or [])}")

elif result.status == CardParseStatus.PROXY_BLOCKED:
    print("Прокси заблокирован — нужно сменить")

elif result.status == CardParseStatus.CAPTCHA_FAILED:
    print("Капча не решена")

elif result.status == CardParseStatus.NOT_FOUND:
    print("Объявление удалено или не найдено")

elif result.status == CardParseStatus.PAGE_NOT_DETECTED:
    print("Неизвестная страница")
```

**Пример с обработкой блокировки:**

```python
async def parse_with_retry(browser, url, fields, max_retries=3):
    for attempt in range(max_retries):
        page = await browser.new_page()
        response = await page.goto(url)

        result = await parse_card(page, response, fields=fields)

        if result.status == CardParseStatus.SUCCESS:
            return result.data

        if result.status == CardParseStatus.PROXY_BLOCKED:
            await page.close()
            # Создаём страницу с новым прокси
            continue

        if result.status == CardParseStatus.NOT_FOUND:
            return None  # Объявление удалено

        # Другие ошибки
        break

    return None
```

### Enum CardParseStatus

| Статус | Описание |
|--------|----------|
| `SUCCESS` | Карточка успешно спарсена. Данные в `result.data`. |
| `CAPTCHA_FAILED` | Капча не решена за `max_captcha_attempts` попыток. |
| `PROXY_BLOCKED` | Прокси заблокирован (HTTP 403 или 407). Нужно сменить прокси. |
| `NOT_FOUND` | Объявление удалено или не найдено (HTTP 404/410). |
| `PAGE_NOT_DETECTED` | Неизвестное состояние страницы. Ни один детектор не сработал. |
| `WRONG_PAGE` | Открыта не та страница (журнал, редакционная страница и т.д.). |
| `SERVER_UNAVAILABLE` | Сервер недоступен (HTTP 502/503/504). Retry исчерпаны. |

### Модель CardParseResult

| Поле | Тип | Описание |
|------|-----|----------|
| `status` | `CardParseStatus` | Статус парсинга |
| `data` | `CardData \| None` | Данные карточки (только при `SUCCESS`) |

### Модель CardData

| Поле | Тип | Описание |
|------|-----|----------|
| `title` | `str \| None` | Заголовок |
| `price` | `int \| None` | Цена |
| `seller` | `dict \| None` | `{"name": ..., "profile_url": ...}` |
| `item_id` | `int \| None` | ID объявления |
| `published_at` | `str \| None` | Дата публикации |
| `description` | `str \| None` | Описание |
| `location` | `dict \| None` | `{"address": ..., "metro": ..., "region": ...}` |
| `characteristics` | `dict \| None` | Характеристики товара |
| `views_total` | `int \| None` | Всего просмотров |
| `images` | `list[bytes] \| None` | Скачанные изображения |
| `images_urls` | `list[str] \| None` | URL изображений |
| `images_errors` | `list[str] \| None` | Ошибки при скачивании |
| `raw_html` | `str \| None` | Исходный HTML |

---

## Решение капчи

### Функция resolve_captcha_flow

Оркестратор решения Geetest-капчи. Автоматически нажимает кнопку "Продолжить" и решает слайдер-капчу.

```python
async def resolve_captcha_flow(
    page: Page,
    *,
    max_attempts: int = 30,
) -> tuple[str, bool]
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `page` | `Page` | — | Playwright-страница с капчей |
| `max_attempts` | `int` | `30` | Максимум попыток решения |

**Возвращает:** `(html: str, solved: bool)`
- `html` — текущий HTML страницы
- `solved` — удалось ли решить капчу

**Когда вызывать:**

При срабатывании одного из детекторов:
- `CONTINUE_BUTTON_DETECTOR_ID`
- `CAPTCHA_DETECTOR_ID`
- `PROXY_BLOCK_429_DETECTOR_ID`

**Алгоритм работы:**

1. Нажимает кнопку "Продолжить" (если есть)
2. Детектирует появление капчи
3. Извлекает изображения фона и пазла
4. Ищет смещение в кеше или вычисляет через OpenCV
5. Выполняет drag&drop слайдера
6. Проверяет результат
7. Повторяет при неудаче (до `max_attempts` раз)

**Пример:**

```python
from avito_library import resolve_captcha_flow

html, solved = await resolve_captcha_flow(page, max_attempts=30)

if solved:
    print("Капча решена успешно")
    # Продолжаем работу со страницей
else:
    print("Не удалось решить капчу — возможно, нужно сменить прокси")
```

---

## Парсинг профиля продавца

### Функция collect_seller_items

Собирает список товаров продавца через API Avito.

```python
async def collect_seller_items(
    page: Page,
    *,
    min_price: int | None = 8000,
    condition_titles: Sequence[str] | None = None,
    include_items: bool = False,
    item_fields: Sequence[str] | None = None,
    item_schema: dict[str, Any] | None = None,
) -> SellerProfileParsingResult
```

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| `page` | `Page` | — | Страница профиля продавца |
| `min_price` | `int \| None` | `8000` | Мин. цена для фильтрации |
| `condition_titles` | `Sequence[str]` | `None` | Фильтр по badge-заголовкам из API ("Новое", "Б/у" и др.) |
| `include_items` | `bool` | `False` | Включить детали товаров |
| `item_fields` | `Sequence[str]` | `None` | Поля товара для извлечения |
| `item_schema` | `dict` | `None` | Схема извлечения полей, например `{"price": "priceDetailed.value"}` |

**Возвращает:** `SellerProfileParsingResult` (dict) с полями:
- `state` — статус (см. возможные значения ниже)
- `seller_name` — имя продавца
- `item_ids` — список ID товаров
- `pages_collected` — обработано страниц API
- `is_complete` — полностью ли обработаны страницы
- `items` — детали товаров (если `include_items=True` или `item_fields`)
- `item_titles` — список заголовков товаров (если `include_items=True`)
- `items_by_id` — словарь товаров по ID (если передан `item_schema`)

**Возможные значения `state`:**
- `SELLER_PROFILE_DETECTOR_ID` — успех, данные собраны
- `"detection_error"` — ошибка детектора состояния
- `"seller_id_not_found"` — не удалось извлечь ID продавца из HTML
- `"server_unavailable"` — сервер недоступен (HTTP 502/503/504), retry исчерпаны
- `NOT_DETECTED_STATE_ID` — страница не распознана
- Любой другой ID детектора — страница распознана как другой тип (капча, блокировка и т.д.)

**Исключение:** `SellerIdNotFound` — seller_id не найден в HTML (устаревшее, теперь возвращается в `state`)

**Пример:**

```python
from avito_library import collect_seller_items, SELLER_PROFILE_DETECTOR_ID

await page.goto("https://www.avito.ru/user/abc123/profile")

result = await collect_seller_items(
    page,
    min_price=5000,
    include_items=True,
)

if result['state'] == SELLER_PROFILE_DETECTOR_ID:
    print(f"Продавец: {result['seller_name']}")
    print(f"Товаров: {len(result['item_ids'])}")
elif result['state'] == "seller_id_not_found":
    print("Не удалось найти ID продавца")
else:
    print(f"Ошибка: {result['state']}")
```

---

## Исключения

| Исключение | Модуль | Описание |
|------------|--------|----------|
| `DetectionError` | `detectors` | Критическая ошибка детектора |
| `CardParsingError` | `parsers` | HTML не соответствует карточке Avito (используется внутренне) |
| `SellerIdNotFound` | `parsers` | Не найден ID продавца *(deprecated: теперь возвращается в `state`)* |

**Примечание:** Функция `parse_card` не выбрасывает исключения — она возвращает `CardParseResult` со статусом. Функция `parse_catalog` также возвращает статус в `CatalogParseResult`.

---

## Конфигурация

### Глобальный лимит страниц

```python
import avito_library.config

# Установить глобальный лимит страниц (для всех вызовов parse_catalog и collect_seller_items)
avito_library.config.MAX_PAGE = 50

# По умолчанию None (без лимита)
```

---

## Лицензия

MIT
