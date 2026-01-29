# План: AI-валидация через Fireworks Batch API

## Концепция

Модификация `validation_worker.py` для поддержки двух режимов AI-валидации:

| Режим | Назначение | Когда использовать |
|-------|------------|-------------------|
| **BATCH** | Продакшен | Массовая обработка артикулов |
| **REALTIME** | Тестирование | Проверка работы модели |

Переключение через переменную окружения `AI_VALIDATION_MODE`.

---

## Архитектура

```
Артикул (CATALOG_PARSED)
        │
        ▼
┌─────────────────────────────────────┐
│       validation_worker.py          │
│                                     │
│  1. price_filter_validation()       │
│  2. mechanical_validation()         │
│  3. ai_validation() ◄───────────────┼──── Выбор режима
│         │                           │
│    ┌────┴────┐                      │
│    │         │                      │
│  BATCH    REALTIME                  │
│    │         │                      │
│    └────┬────┘                      │
│         ▼                           │
│  validation_results (type='ai')     │
└─────────────────────────────────────┘
        │
        ▼
Артикул (VALIDATED) → object_tasks
```

---

## Режим BATCH (продакшен)

### Логика запуска
- Запуск batch job при накоплении **M артикулов** (настраивается)
- Минимум: `BATCH_MIN_ARTICULUMS` (default: 10)
- Максимум в одном job: `BATCH_MAX_ARTICULUMS` (default: 1000)

### Workflow
```
1. Накопление артикулов в статусе VALIDATING (прошли mechanical)
2. При достижении M артикулов:
   a) Генерация JSONL файла
   b) Загрузка dataset на Fireworks
   c) Создание batch inference job
   d) Polling статуса (каждые 5 минут)
   e) Скачивание результатов
   f) Парсинг и сохранение в validation_results
   g) Перевод артикулов в VALIDATED / REJECTED
```

### Fireworks API endpoints
```
POST /v1/accounts/{account_id}/datasets                    # Создать dataset
POST /v1/accounts/{account_id}/datasets/{id}:upload        # Загрузить JSONL
POST /v1/accounts/{account_id}/batchInferenceJobs          # Создать job
GET  /v1/accounts/{account_id}/batchInferenceJobs/{id}     # Статус job
GET  /v1/accounts/{account_id}/datasets/{id}:getDownloadEndpoint  # Скачать результат
```

---

## Режим REALTIME (тестирование)

### Логика
- Каждый артикул обрабатывается сразу после mechanical validation
- Синхронный запрос к Fireworks Chat Completions API
- Аналогично текущей реализации с HuggingFace

### API endpoint
```
POST https://api.fireworks.ai/inference/v1/chat/completions
```

---

## Формат данных (оптимизированный)

### Входные данные (user message)
```json
{"a":"LR081595","items":[
  {"i":"1891948542","t":"Топливный насос 3.0/5.0 Бензин LR081595","s":"Насос Топливный Подкачивающий Land Rover...","p":42000.0},
  {"i":"3593721707","t":"LR081595 land rover насос топливный","s":"LR081595 Land Rover Насос Топливный...","p":18411.0}
]}
```

| Поле | Полное имя | Ограничение |
|------|------------|-------------|
| `a` | articulum | — |
| `i` | id (avito_item_id) | — |
| `t` | title | до 100 символов |
| `s` | snippet | до 200 символов |
| `p` | price | — |

**Убрано:** `seller_reviews` (не влияет на решение)

### Выходные данные (assistant message)
```json
{"p":["1891948542","3681460245"],"r":[["3593721707","2"],["4464060771","1"]]}
```

| Поле | Значение |
|------|----------|
| `p` | passed_ids — список ID прошедших валидацию |
| `r` | rejected — массив [id, код_причины] |

### Коды причин отклонения
| Код | Причина |
|-----|---------|
| 1 | Неоригинал / сторонний бренд |
| 2 | Подозрительная цена |
| 3 | Несоответствие артикулу |
| 4 | Б/у / восстановленный |
| 5 | Подделка |
| 6 | Другое |

---

## Промпты

### System Prompt (для fine-tuned модели)
```
Валидатор автозапчастей. JSON ответ: {"p":[ids],"r":[[id,код]]}. Коды: 1=неоригинал, 2=цена, 3=артикул, 4=б/у, 5=подделка, 6=другое
```

### User Prompt (сокращённый, ~950 символов)

**Расположение:** `container/validation_worker.py` → метод `ai_validation()`

```
Ты эксперт по валидации автозапчастей Авито. Отсеивай неоригиналы и подделки.

АРТИКУЛ: "{articulum}"
(может быть одним из нескольких артикулов запчасти)

ОБЪЯВЛЕНИЯ:
{items_json}

ОТКЛОНЯЙ (REJECT):
• Неоригинал: "аналог", "копия", "aftermarket", "заменитель", сторонние бренды (не OEM)
• Подделка: подозрительно низкая цена, "качество как оригинал"
• Несоответствие: артикул "{articulum}" отсутствует в объявлении
• Б/у или восстановленный товар

ПРИНИМАЙ (PASS):
• Указание на оригинальность (OEM, оригинал)
• Бренд оригинального производителя
• Адекватная цена для оригинала
• Артикул "{articulum}" присутствует

При сомнениях — ОТКЛОНЯЙ.

ОТВЕТ JSON:
{
  "passed_ids": ["id1", "id2"],
  "rejected": [{"id": "id3", "reason": "причина"}]
}

Используй РЕАЛЬНЫЕ ID из входных данных. Каждое объявление — либо в passed_ids, либо в rejected.
```

| Версия | Символов | Токенов | Экономия |
|--------|----------|---------|----------|
| Оригинальный | ~2100 | ~525 | — |
| Сокращённый | ~950 | ~240 | **55%** |

---

## Формат JSONL для Batch API

### Входной файл (batch_input.jsonl)
```jsonl
{"custom_id":"art_123","body":{"model":"accounts/fireworks/models/qwen3-8b","messages":[{"role":"system","content":"..."},{"role":"user","content":"{\"a\":\"...\",\"items\":[...]}"}],"max_tokens":500,"temperature":0.1}}
{"custom_id":"art_456","body":{"model":"accounts/fireworks/models/qwen3-8b","messages":[{"role":"system","content":"..."},{"role":"user","content":"{\"a\":\"...\",\"items\":[...]}"}],"max_tokens":500,"temperature":0.1}}
```

`custom_id` = `art_{articulum_id}` для сопоставления результатов.

### Выходной файл (results.jsonl)
```jsonl
{"custom_id":"art_123","response":{"choices":[{"message":{"content":"{\"p\":[...],\"r\":[...]}"}}]}}
{"custom_id":"art_456","response":{"choices":[{"message":{"content":"{\"p\":[...],\"r\":[...]}"}}]}}
```

---

## Конфигурация (env переменные)

```bash
# Fireworks API
FIREWORKS_API_KEY=fw_3ZahRSzrPtVVWnPmWXwMQoze
FIREWORKS_ACCOUNT_ID=your-account-id
FIREWORKS_MODEL=accounts/fireworks/models/qwen3-8b

# Режим AI валидации
AI_VALIDATION_MODE=batch  # batch | realtime

# Настройки Batch режима
BATCH_MIN_ARTICULUMS=10        # Мин. артикулов для запуска job
BATCH_MAX_ARTICULUMS=1000      # Макс. артикулов в одном job
BATCH_POLL_INTERVAL=300        # Интервал проверки статуса (секунды)
```

---

## Изменения в validation_worker.py

### Новый класс FireworksClient
```python
class FireworksClient:
    """Клиент для Fireworks AI API"""
    
    # BATCH режим
    async def create_dataset(self, dataset_id: str) -> None
    async def upload_jsonl(self, dataset_id: str, content: bytes) -> None
    async def create_batch_job(self, input_dataset: str, output_dataset: str) -> str
    async def get_job_status(self, job_id: str) -> str
    async def download_results(self, dataset_id: str) -> List[dict]
    
    # REALTIME режим
    async def chat_completion(self, messages: List[dict]) -> str
```

### Модификация ai_validation()
```python
async def ai_validation(self, articulum_id, articulum, listings):
    if AI_VALIDATION_MODE == 'realtime':
        return await self._ai_validation_realtime(articulum_id, articulum, listings)
    else:
        # В batch режиме артикул добавляется в очередь
        # Обработка происходит в отдельном цикле
        await self._add_to_batch_queue(articulum_id, articulum, listings)
        return None  # Результат будет позже
```

---

## Экономика

| Метрика | Значение |
|---------|----------|
| Цена Batch API | $0.10 / 1M токенов |
| Токенов на артикул | ~2,800 |
| Стоимость 1000 артикулов | ~$0.28 |
| Экономия vs HuggingFace | ~93% |

---

## Статусы Batch Job (Fireworks)

| Статус | Описание |
|--------|----------|
| VALIDATING | Проверка формата dataset |
| PENDING | В очереди |
| RUNNING | Обработка |
| COMPLETED | Завершено успешно |
| FAILED | Ошибка |
| EXPIRED | Превышен timeout (24 часа) |
