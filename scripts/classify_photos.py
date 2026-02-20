#!/usr/bin/env python3
"""
Классификация фотографий автозапчастей через Kimi K2.5 (OpenAI-совместимый API).
Категории: new (новое), used (б/у), catalog (каталожное/стоковое).
Перемещает б/у и каталожные фото в отдельные папки.
Поддерживает resume — при повторном запуске пропускает уже обработанные.
"""

import asyncio
import json
import re
import shutil
import base64
import time
from pathlib import Path
from typing import Dict, List

import aiohttp

# ═══════════════════ НАСТРОЙКИ ═══════════════════

INPUT_DIR = Path(__file__).parent.resolve() / 'data' / 'original_photos'
USED_DIR = Path(__file__).parent.resolve() / 'data' / 'used_photos'
CATALOG_DIR = Path(__file__).parent.resolve() / 'data' / 'catalog_photos'
RESULTS_FILE = Path(__file__).parent.resolve() / 'data' / 'classification_results.json'

API_URL = 'https://api.kimi.com/coding/v1/chat/completions'
API_KEY = 'sk-kimi-DFZkehxfFuw5y05kqhM2jRrzDKrqz2SqEVo6G1yjTZkEaam82aAn2XJHRzkoofgM'
API_MODEL = 'kimi-for-coding'
API_USER_AGENT = 'KimiCLI/1.12.0'

BATCH_SIZE = 10       # фото в одном запросе
MAX_CONCURRENT = 3    # параллельные запросы
TIMEOUT = 180         # секунды (10 изображений = тяжёлый запрос)
MAX_RETRIES = 3
RETRY_BASE_DELAY = 3.0


# ═══════════════════ ПРОМПТ ═══════════════════

SYSTEM_PROMPT = (
    "Ты эксперт по фотографиям автозапчастей. "
    "Отвечай ТОЛЬКО JSON объектом. Никакого текста вне JSON."
)


def build_prompt(filenames: List[str]) -> str:
    mapping = "\n".join(f"  Фото {i+1} = {fn}" for i, fn in enumerate(filenames))
    return f"""Классифицируй каждое фото автозапчасти.

Порядок фото:
{mapping}

Категории:
- "new" — НОВАЯ запчасть: чистая, без следов использования, может быть в упаковке или без. Нет царапин, потёртостей, грязи, ржавчины. Реальный снимок продавца.
- "used" — Б/У запчасть: видны царапины, потёртости, грязь, ржавчина, следы установки или эксплуатации, сколы, облезшая краска. Деталь была в использовании.
- "catalog" — Каталожное/стоковое фото: рендер, 3D-модель, фото из интернет-каталога, идеально белый или однотонный фон, профессиональное студийное фото. НЕ реальный снимок продавца.

Правила:
- Сомнения между new и used -> выбирай "used" (строже к б/у)
- Сомнения между new и catalog -> выбирай "new"
- Для "used" и "catalog" ОБЯЗАТЕЛЬНО укажи краткую причину (reason)
- Для "new" поле reason необязательно

Ответь JSON:
{{
  "results": [
    {{"photo": 1, "status": "new"}},
    {{"photo": 2, "status": "used", "reason": "краткая причина"}},
    {{"photo": 3, "status": "catalog", "reason": "краткая причина"}}
  ]
}}"""


# ═══════════════════ API ═══════════════════

def build_messages(prompt: str, image_paths: List[Path]) -> List[Dict]:
    """OpenAI-совместимые multimodal messages с base64 изображениями."""
    content = [{"type": "text", "text": prompt}]
    for img_path in image_paths:
        img_b64 = base64.b64encode(img_path.read_bytes()).decode('utf-8')
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
        })
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": content},
    ]


def parse_response(raw: str, filenames: List[str]) -> List[Dict]:
    """Парсинг JSON ответа AI в список {file, status, reason}."""
    # Убираем <think> блоки (thinking-модели)
    cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()

    data = None
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

    if not data:
        return [{"file": fn, "status": "error", "reason": "JSON parse error"} for fn in filenames]

    results = []
    processed = set()

    for item in data.get("results", []):
        photo_num = item.get("photo", 0)
        status = item.get("status", "")
        reason = item.get("reason", "")

        if status not in ("new", "used", "catalog"):
            status = "error"
            reason = f"Unknown status: {item.get('status')}"

        if 1 <= photo_num <= len(filenames):
            fn = filenames[photo_num - 1]
            if fn not in processed:
                results.append({"file": fn, "status": status, "reason": reason})
                processed.add(fn)

    # Фото, не упомянутые в ответе AI
    for fn in filenames:
        if fn not in processed:
            results.append({"file": fn, "status": "error", "reason": "Not in AI response"})

    return results


async def classify_batch(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    batch_paths: List[Path],
    batch_num: int,
    total_batches: int,
) -> List[Dict]:
    """Классифицировать батч фото через Kimi API."""
    filenames = [p.name for p in batch_paths]

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            async with semaphore:
                # Формируем запрос внутри семафора (чтобы не держать все изображения в памяти)
                prompt = build_prompt(filenames)
                messages = build_messages(prompt, batch_paths)
                payload = {
                    "model": API_MODEL,
                    "messages": messages,
                    "max_tokens": 4096,
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                    "thinking": {"type": "disabled"},
                }

                async with session.post(API_URL, json=payload) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw = data['choices'][0]['message']['content']
                        results = parse_response(raw, filenames)

                        counts = {}
                        for r in results:
                            counts[r["status"]] = counts.get(r["status"], 0) + 1
                        info = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
                        print(f"  [{batch_num}/{total_batches}] {info}")
                        return results

                    if resp.status in (429, 503, 504):
                        delay = RETRY_BASE_DELAY * (2 ** attempt)
                        print(f"  [{batch_num}] HTTP {resp.status}, retry {attempt+1}/{MAX_RETRIES}")
                        await asyncio.sleep(delay)
                        continue

                    text = await resp.text()
                    last_error = f"HTTP {resp.status}: {text[:200]}"

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_error = str(e)
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"  [{batch_num}] {type(e).__name__}, retry {attempt+1}/{MAX_RETRIES}")
                await asyncio.sleep(delay)

    print(f"  [{batch_num}] FAIL: {last_error}")
    return [{"file": fn, "status": "error", "reason": str(last_error)[:200]} for fn in filenames]


# ═══════════════════ СОХРАНЕНИЕ ═══════════════════

def save_results(results: Dict[str, Dict]):
    """Сохранить результаты в JSON файл."""
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(results.values()), f, ensure_ascii=False, indent=2)


# ═══════════════════ MAIN ═══════════════════

async def main():
    print(f"Входная папка: {INPUT_DIR}")
    print(f"API: {API_URL}")
    print(f"Модель: {API_MODEL}")
    print()

    USED_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)

    # Resume: загружаем ранее обработанные
    existing: Dict[str, Dict] = {}
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE) as f:
            for item in json.load(f):
                existing[item["file"]] = item
        print(f"Resume: {len(existing)} фото уже обработано")

    # Собираем фото для обработки
    all_photos = sorted(INPUT_DIR.glob("*.jpg"))
    to_process = [p for p in all_photos if p.name not in existing]

    print(f"Фото в папке: {len(all_photos)}")
    print(f"К обработке: {len(to_process)}")

    if to_process:
        batches = [to_process[i:i + BATCH_SIZE] for i in range(0, len(to_process), BATCH_SIZE)]
        total = len(batches)
        print(f"Батчей: {total} (по {BATCH_SIZE} фото, {MAX_CONCURRENT} параллельных)")
        print()

        start_time = time.time()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)

        resolver = aiohttp.resolver.ThreadedResolver()
        connector = aiohttp.TCPConnector(resolver=resolver)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
                "User-Agent": API_USER_AGENT,
            },
            timeout=aiohttp.ClientTimeout(total=TIMEOUT),
        ) as session:
            tasks = [
                classify_batch(session, semaphore, batch, i + 1, total)
                for i, batch in enumerate(batches)
            ]

            completed = 0
            for coro in asyncio.as_completed(tasks):
                result = await coro
                for item in result:
                    existing[item["file"]] = item
                completed += 1

                # Промежуточное сохранение каждые 5 батчей
                if completed % 5 == 0 or completed == total:
                    save_results(existing)

        elapsed = time.time() - start_time
        print(f"\nОбработка: {elapsed:.0f}с ({elapsed/60:.1f} мин)")

    # Финальное сохранение
    save_results(existing)
    print(f"Результаты: {RESULTS_FILE}")

    # Перемещение файлов
    counts = {"new": 0, "used": 0, "catalog": 0, "error": 0}
    moved = 0

    for item in existing.values():
        status = item["status"]
        counts[status] = counts.get(status, 0) + 1

        src = INPUT_DIR / item["file"]
        if not src.exists():
            continue

        if status == "used":
            shutil.move(str(src), str(USED_DIR / item["file"]))
            moved += 1
        elif status == "catalog":
            shutil.move(str(src), str(CATALOG_DIR / item["file"]))
            moved += 1

    print(f"\n{'='*50}")
    print(f"ИТОГО ({len(existing)} фото):")
    print(f"  Новые (original_photos/):    {counts.get('new', 0)}")
    print(f"  Б/У (used_photos/):          {counts.get('used', 0)}")
    print(f"  Каталожные (catalog_photos/): {counts.get('catalog', 0)}")
    if counts.get('error', 0):
        print(f"  Ошибки:                      {counts['error']}")
    print(f"  Перемещено файлов:           {moved}")
    print(f"{'='*50}")


if __name__ == '__main__':
    asyncio.run(main())
