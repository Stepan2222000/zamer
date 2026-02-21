#!/usr/bin/env python3
"""
Тест модели Kimi K2.5 на реальных данных из БД zamer_sys.

Измеряет:
- Input/Output токены для расчёта стоимости
- Качество валидации (сохраняет решения AI в JSON)

Использование:
    python3 test_kimi.py
"""

import asyncio
import asyncpg
import aiohttp
import json
import base64
import re
import sys
import time
from datetime import datetime
from pathlib import Path

# Добавляем container в sys.path для импорта s3_client
sys.path.insert(0, str(Path(__file__).parent.parent / 'container'))

# ═══════════════════════════════════════════════
#  КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════

DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5419,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}

FIREWORKS_API_KEY = 'fw_DJ9zDiaEjb1L3dPqxhXcdi'
FIREWORKS_API_URL = 'https://api.fireworks.ai/inference/v1/chat/completions'

MODEL_ID = 'accounts/fireworks/models/kimi-k2p5'
MODEL_NAME = 'Kimi K2.5'

# Pricing ($ per 1M tokens)
PRICING = {'input': 0.60, 'output': 3.00}

# Параметры валидации (как в проде)
MIN_PRICE = 8000.0
MIN_VALIDATED_ITEMS = 5  # Минимум объявлений для прохождения (как в config.py)
AI_MAX_IMAGES_PER_LISTING = 1
MAX_LISTINGS_FOR_AI = 30
TEST_COUNT = 10

# Стоп-слова (из config.py)
STOPWORDS = [
    'копия', 'копии', 'копий', 'копию', 'копией',
    'реплика', 'реплики', 'реплику',
    'подделка', 'подделки', 'подделок', 'подделку',
    'фейк', 'fake', 'replica', 'copy',
    'имитация', 'имитации', 'имитацию',
    'аналог', 'аналоги', 'аналогов', 'аналогу',
    'не оригинал', 'неоригинал', 'неоригинальный', 'неоригинальная',
    'китай', 'китайский', 'китайская', 'китайские', 'china',
    'подобие', 'как оригинал',
    'дубликат', 'дубликаты', 'дубль', 'дубли',
    'б/у', 'бу', 'б у', 'использованный', 'использованная', 'использованные',
    'ношенный', 'ношеный', 'поношенный',
    'second hand', 'second-hand', 'secondhand', 'used',
    'worn', 'pre-owned', 'preowned', 'pre owned',
    'после носки', 'поноска', 'с дефектами', 'дефект', 'дефекты', 'дефектов',
    'потертости', 'потёртости', 'потертость', 'потёртость',
    'царапины', 'царапина', 'царапин',
    'следы носки',
    'требует ремонта', 'на запчасти', 'не новый', 'не новая',
]


# ═══════════════════════════════════════════════
#  РАБОТА С БД
# ═══════════════════════════════════════════════

async def get_random_articulums(pool, count):
    """Получить СЛУЧАЙНЫЕ артикулы в CATALOG_PARSED (минимум MIN_VALIDATED_ITEMS объявлений)."""
    return await pool.fetch("""
        SELECT a.id, a.articulum
        FROM articulums a
        JOIN catalog_listings cl ON cl.articulum_id = a.id
        WHERE a.state = 'CATALOG_PARSED'
        GROUP BY a.id, a.articulum
        HAVING COUNT(*) >= $2
        ORDER BY RANDOM()
        LIMIT $1
    """, count, MIN_VALIDATED_ITEMS)


async def get_listings(pool, articulum_id):
    """Получить объявления для артикула (изображения из S3)."""
    rows = await pool.fetch("""
        SELECT avito_item_id, title, price, snippet_text,
               seller_name, seller_id, seller_rating, seller_reviews,
               images_count, s3_keys
        FROM catalog_listings
        WHERE articulum_id = $1
    """, articulum_id)

    listings = [dict(r) for r in rows]

    # Скачиваем изображения из S3
    from s3_client import get_s3_async_client
    s3 = get_s3_async_client()

    all_keys = []
    for listing in listings:
        keys = listing.get('s3_keys') or []
        all_keys.extend(keys)

    downloaded = await s3.download_many(all_keys) if all_keys else {}

    for listing in listings:
        keys = listing.pop('s3_keys', None) or []
        listing['images_bytes'] = [downloaded[k] for k in keys if k in downloaded]

    return listings


# ═══════════════════════════════════════════════
#  ФИЛЬТРАЦИЯ (как в проде)
# ═══════════════════════════════════════════════

def apply_filters(listings):
    """Применить продакшн-фильтры: цена + изображения + стоп-слова."""
    result = []
    stats = {'price': 0, 'no_images': 0, 'no_bytes': 0, 'stopword': 0}

    for l in listings:
        # Фильтр по цене
        price = l.get('price')
        if price is None or float(price) < MIN_PRICE:
            stats['price'] += 1
            continue

        # Фильтр по наличию изображений
        images_count = l.get('images_count')
        if images_count is None or images_count == 0:
            stats['no_images'] += 1
            continue

        # Проверка что images_bytes реально есть
        images_bytes = l.get('images_bytes') or []
        if not images_bytes or len(images_bytes) == 0:
            stats['no_bytes'] += 1
            continue

        # Стоп-слова
        title = (l.get('title') or '').lower()
        snippet = (l.get('snippet_text') or '').lower()
        seller = (l.get('seller_name') or '').lower()
        text = f"{title} {snippet} {seller}"

        found_stopword = None
        for sw in STOPWORDS:
            pattern = r'\b' + re.escape(sw.lower()) + r'\b'
            if re.search(pattern, text):
                found_stopword = sw
                break

        if found_stopword:
            stats['stopword'] += 1
            continue

        result.append(l)

    return result, stats


# ═══════════════════════════════════════════════
#  ПОСТРОЕНИЕ ПРОМПТА (идентично ai_provider.py)
# ═══════════════════════════════════════════════

def build_prompt(articulum, listings):
    """Построить текстовый промпт — точная копия из ai_provider.py."""
    items = []
    for l in listings:
        items.append({
            'id': l['avito_item_id'],
            'title': l.get('title', ''),
            'price': float(l['price']) if l.get('price') else None,
            'snippet': l.get('snippet_text'),
            'seller': l.get('seller_name'),
        })

    real_ids = [i['id'] for i in items[:4]]

    image_criteria = """
САМЫЙ ВАЖНЫЙ КРИТЕРИЙ — ФОТО:
На фото ОБЯЗАТЕЛЬНО должна быть видна сама запчасть или её упаковка.
Если на фото что угодно кроме запчасти (автомобиль, рекламный баннер, логотип магазина, заглушка, каталожная картинка) — ОТКЛОНЯЙ.

Дополнительные критерии отклонения по фото:
   - На фото видны признаки использования (царапины, потёртости, грязь)
   - Состояние товара НЕ НОВОЕ (следы эксплуатации)
   - На фото видна неоригинальная упаковка

Критерии принятия по фото:
✓ На фото видна реальная запчасть или коробка от запчасти
✓ Если видна оригинальная упаковка/маркировка производителя — это сильный плюс, даже при низкой цене
✓ Состояние товара — НОВОЕ
"""

    return f"""Ты эксперт по валидации автозапчастей с Авито. Твоя задача - отсеивать неоригинальные запчасти и подделки.

АРТИКУЛ ДЛЯ ПРОВЕРКИ: "{articulum}"
Этот артикул соответствует определённому типу запчасти для определённого автомобиля.
Ты должен определить по названию, описанию и фото — продаётся ли в объявлении запчасть того же типа для того же автомобиля.
НЕ проверяй наличие точного номера артикула в тексте — у запчасти может быть много совместимых артикулов, и в описании может быть указан другой номер.

ОБЪЯВЛЕНИЯ:
{json.dumps(items, ensure_ascii=False)}

СТРОГИЕ КРИТЕРИИ ОТКЛОНЕНИЯ (REJECT):

1. НЕОРИГИНАЛЬНЫЕ ЗАПЧАСТИ:
   - Явное указание на аналог, копию, реплику, имитацию
   - Фразы: "неоригинальный", "аналог оригинала", "китайская копия", "aftermarket", "заменитель"
   - Указание на сторонние бренды-производители (не OEM)
   - Фразы: "качество как оригинал", "не уступает оригиналу" (это признак подделки)

2. ПОДДЕЛКИ И ПАЛЬ:
   - Признаки подделки в описании
   - Отсутствие оригинальной упаковки/маркировки (если об этом упоминается)
   - Подозрительно низкая цена — НО ТОЛЬКО если на фото НЕТ подтверждения оригинальности. Если на фото видна оригинальная упаковка, маркировка производителя или QR-код — цена НЕ является основанием для отклонения.

3. НЕСООТВЕТСТВИЕ ТИПУ ЗАПЧАСТИ:
   - Запчасть явно ДРУГОГО типа (например, ищем тормозной диск, а продаётся колодка)
   - Запчасть для ДРУГОГО автомобиля или модели
   - Запчасть не имеет отношения к артикулу "{articulum}" по типу и назначению
{image_criteria}
КРИТЕРИИ ПРИНЯТИЯ (PASS):

✓ Запчасть того же типа и для того же автомобиля, что ожидается для артикула "{articulum}"
✓ Явное указание на оригинальность (OEM, оригинальный артикул)
✓ Бренд известного оригинального производителя
✓ Цена соответствует оригинальной запчасти
✓ Отсутствие признаков подделки в описании

ВАЖНО: При малейших сомнениях в оригинальности - ОТКЛОНЯЙ объявление.

ФОРМАТ ОТВЕТА - СТРОГО JSON:
- Верни ОДИН JSON объект (не повторяй его!)
- КАЖДОЕ объявление из входных данных ОБЯЗАТЕЛЬНО должно быть либо в passed_ids, либо в rejected
- Используй РЕАЛЬНЫЕ ID объявлений (например: "{real_ids[0] if real_ids else ''}", "{real_ids[1] if len(real_ids) > 1 else ''}")
- НЕ используй шаблонные id1, id2 - только настоящие числовые ID!
- Для каждого отклонённого объявления ОБЯЗАТЕЛЬНО укажи причину в поле reason

{{
  "passed_ids": ["ID принятых объявлений"],
  "rejected": [
    {{"id": "ID отклонённого", "reason": "Краткая причина отклонения"}}
  ]
}}

ПРИМЕР для {len(items)} объявлений - все ID должны быть распределены:
{{
  "passed_ids": ["{real_ids[0] if real_ids else ''}"],
  "rejected": [
    {{"id": "{real_ids[1] if len(real_ids) > 1 else ''}", "reason": "Аналог, не оригинал"}},
    {{"id": "{real_ids[2] if len(real_ids) > 2 else ''}", "reason": "Подозрительно низкая цена"}}
  ]
}}"""


def build_messages(prompt, listings):
    """Построить messages с изображениями (мультимодальный режим)."""
    content = [{"type": "text", "text": prompt}]

    for listing in listings:
        images_bytes_raw = listing.get('images_bytes') or []
        for img_data in images_bytes_raw[:AI_MAX_IMAGES_PER_LISTING]:
            if img_data:
                # asyncpg может вернуть memoryview — конвертируем в bytes
                if isinstance(img_data, memoryview):
                    img_data = bytes(img_data)
                img_b64 = base64.b64encode(img_data).decode('utf-8')
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
                })

    return [
        {
            "role": "system",
            "content": "Ты валидатор автозапчастей. Отвечай ТОЛЬКО одним JSON объектом с полями passed_ids (массив строк) и rejected (массив объектов с id и reason). НЕ копируй входные данные объявлений в ответ. Верни только своё решение."
        },
        {"role": "user", "content": content}
    ]


# ═══════════════════════════════════════════════
#  ВЫЗОВ FIREWORKS API
# ═══════════════════════════════════════════════

async def call_fireworks(session, messages):
    """Отправить запрос и вернуть (content, usage, elapsed, error)."""
    payload = {
        "model": MODEL_ID,
        "messages": messages,
        "temperature": 0.1,
        "response_format": {"type": "json_object"},
    }

    start = time.time()
    try:
        async with session.post(FIREWORKS_API_URL, json=payload) as resp:
            elapsed = time.time() - start

            if resp.status != 200:
                text = await resp.text()
                # Если json_object не поддерживается — пробуем без него
                if resp.status == 400 and 'response_format' in text:
                    print("    (json_object не поддерживается, пробую без)")
                    del payload['response_format']
                    start2 = time.time()
                    async with session.post(FIREWORKS_API_URL, json=payload) as resp2:
                        elapsed = time.time() - start2
                        if resp2.status != 200:
                            text2 = await resp2.text()
                            return None, None, elapsed, f"HTTP {resp2.status}: {text2[:500]}"
                        data = await resp2.json()
                        content = data['choices'][0]['message']['content']
                        usage = data.get('usage', {})
                        return content, usage, elapsed, None
                return None, None, elapsed, f"HTTP {resp.status}: {text[:500]}"

            data = await resp.json()
            content = data['choices'][0]['message']['content']
            usage = data.get('usage', {})
            return content, usage, elapsed, None

    except asyncio.TimeoutError:
        elapsed = time.time() - start
        return None, None, elapsed, f"Timeout после {elapsed:.0f}с"
    except Exception as e:
        elapsed = time.time() - start
        return None, None, elapsed, f"Ошибка: {e}"


# ═══════════════════════════════════════════════
#  ПАРСИНГ ОТВЕТА AI
# ═══════════════════════════════════════════════

def parse_ai_response(raw, listings):
    """Парсинг JSON ответа (идентично ai_provider.py)."""
    all_ids = {l['avito_item_id'] for l in listings}

    # Убираем <think> теги если есть
    cleaned = re.sub(r'<think>.*?</think>', '', raw, flags=re.DOTALL).strip()

    try:
        data = json.loads(cleaned)
        passed_ids = set(str(pid) for pid in data.get('passed_ids', []))
        rejected_dict = {
            str(r['id']): r.get('reason', 'Причина не указана')
            for r in data.get('rejected', [])
        }
    except (json.JSONDecodeError, KeyError, TypeError):
        # Fallback regex
        match = re.search(r'"passed_ids"\s*:\s*\[(.*?)\]', raw, re.DOTALL)
        passed_ids = set(re.findall(r'"(\d+)"', match.group(1))) if match else set()
        rejected_dict = dict(
            re.findall(r'\{"id"\s*:\s*"(\d+)"\s*,\s*"reason"\s*:\s*"([^"]*)"', raw)
        )

    missing = all_ids - passed_ids - set(rejected_dict.keys())

    return {
        'passed_ids': list(passed_ids),
        'rejected': [{'id': k, 'reason': v} for k, v in rejected_dict.items()],
        'missing_ids': list(missing),
        'json_parsed_ok': True,
    }


# ═══════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════

async def main():
    print("=" * 70)
    print(f"  ТЕСТ {MODEL_NAME} — ЗАМЕР ТОКЕНОВ И КАЧЕСТВА")
    print("=" * 70)
    print(f"  Модель:  {MODEL_ID}")
    print(f"  Цены:    input=${PRICING['input']}/M, output=${PRICING['output']}/M")
    print(f"  Дата:    {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Выборка: {TEST_COUNT} СЛУЧАЙНЫХ артикулов")
    print("=" * 70)
    print()

    # --- Подключение к БД ---
    print("[DB] Подключение...")
    pool = await asyncpg.create_pool(**DB_CONFIG, min_size=1, max_size=3)
    print("[DB] OK\n")

    # --- Получение тестовых артикулов ---
    articulums = await get_random_articulums(pool, TEST_COUNT)
    print(f"[DB] Выбрано {len(articulums)} случайных артикулов\n")

    if not articulums:
        print(f"Нет артикулов в CATALOG_PARSED с >= {MIN_VALIDATED_ITEMS} объявлениями!")
        await pool.close()
        return

    # --- HTTP сессия ---
    resolver = aiohttp.resolver.ThreadedResolver()
    connector = aiohttp.TCPConnector(resolver=resolver)
    session = aiohttp.ClientSession(
        connector=connector,
        headers={
            "Authorization": f"Bearer {FIREWORKS_API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=aiohttp.ClientTimeout(total=180)
    )

    results = []
    total_input_tokens = 0
    total_output_tokens = 0
    total_cost = 0.0

    try:
        for i, art in enumerate(articulums):
            art_id = art['id']
            art_name = art['articulum']

            print(f"{'─' * 60}")
            print(f"[{i+1}/{len(articulums)}] Артикул: {art_name} (id={art_id})")

            # --- Объявления ---
            listings = await get_listings(pool, art_id)
            print(f"  Всего объявлений:   {len(listings)}")

            # --- Фильтрация ---
            filtered, filter_stats = apply_filters(listings)
            rejected_parts = []
            if filter_stats['price'] > 0:
                rejected_parts.append(f"цена: {filter_stats['price']}")
            if filter_stats['no_images'] > 0:
                rejected_parts.append(f"нет фото: {filter_stats['no_images']}")
            if filter_stats['no_bytes'] > 0:
                rejected_parts.append(f"нет bytes: {filter_stats['no_bytes']}")
            if filter_stats['stopword'] > 0:
                rejected_parts.append(f"стоп-слова: {filter_stats['stopword']}")
            rejected_str = f" (отсеяно: {', '.join(rejected_parts)})" if rejected_parts else ""
            print(f"  После фильтров:    {len(filtered)}{rejected_str}")

            if len(filtered) < MIN_VALIDATED_ITEMS:
                print(f"  ПРОПУСК — мало объявлений после фильтров (нужно минимум {MIN_VALIDATED_ITEMS})\n")
                continue

            # --- Ограничение на MAX_LISTINGS_FOR_AI ---
            ai_listings = filtered[:MAX_LISTINGS_FOR_AI]
            if len(filtered) > MAX_LISTINGS_FOR_AI:
                print(f"  Обрезано:           {len(filtered)} -> {MAX_LISTINGS_FOR_AI}")

            # --- Статистика по изображениям ---
            img_count = 0
            img_total_bytes = 0
            for l in ai_listings:
                ibs = l.get('images_bytes') or []
                if ibs and len(ibs) > 0:
                    img_data = ibs[0]
                    if isinstance(img_data, memoryview):
                        img_data = bytes(img_data)
                    if img_data:
                        img_count += 1
                        img_total_bytes += len(img_data)

            avg_img_kb = (img_total_bytes / img_count / 1024) if img_count > 0 else 0
            print(f"  Изображений для AI: {img_count} (avg {avg_img_kb:.1f} KB)")

            # --- Промпт ---
            prompt = build_prompt(art_name, ai_listings)
            messages = build_messages(prompt, ai_listings)
            prompt_chars = len(prompt)
            print(f"  Размер промпта:     {prompt_chars:,} символов")

            # --- API запрос ---
            print(f"  Отправка в {MODEL_NAME}...", end='', flush=True)
            content, usage, elapsed, error = await call_fireworks(session, messages)

            if error:
                print(f"\n  ОШИБКА: {error}\n")
                results.append({
                    'articulum': art_name,
                    'articulum_id': art_id,
                    'total_listings': len(listings),
                    'after_filters': len(filtered),
                    'error': error,
                })
                continue

            print(f" {elapsed:.1f}с")

            # --- Токены ---
            input_tok = usage.get('prompt_tokens', 0)
            output_tok = usage.get('completion_tokens', 0)
            total_tok = usage.get('total_tokens', input_tok + output_tok)

            cost_in = input_tok * PRICING['input'] / 1_000_000
            cost_out = output_tok * PRICING['output'] / 1_000_000
            cost = cost_in + cost_out

            total_input_tokens += input_tok
            total_output_tokens += output_tok
            total_cost += cost

            # Оценка токенов на изображение
            # Грубо: промпт ~4 символа = ~1 токен для мультиязычного
            text_tokens_est = prompt_chars // 3
            image_tokens_est = max(0, input_tok - text_tokens_est)
            per_image_tokens = image_tokens_est // img_count if img_count > 0 else 0

            print(f"  ┌─── ТОКЕНЫ ───────────────────────────")
            print(f"  │ Input:      {input_tok:>8,} токенов  (${cost_in:.4f})")
            print(f"  │ Output:     {output_tok:>8,} токенов  (${cost_out:.4f})")
            print(f"  │ Total:      {total_tok:>8,} токенов  (${cost:.4f})")
            print(f"  │ ~На 1 фото: {per_image_tokens:>8,} токенов")
            print(f"  └────────────────────────────────────────")

            # --- Парсинг ответа ---
            has_thinking = '<think>' in (content or '')
            ai_result = parse_ai_response(content, ai_listings)

            passed_n = len(ai_result['passed_ids'])
            rejected_n = len(ai_result['rejected'])
            missing_n = len(ai_result['missing_ids'])

            print(f"  Результат AI: passed={passed_n}, rejected={rejected_n}, missing={missing_n}")
            if has_thinking:
                print(f"  ВНИМАНИЕ: Обнаружены thinking токены!")
            print()

            # --- Детализация по объявлениям ---
            passed_set = set(ai_result['passed_ids'])
            rejected_map = {r['id']: r['reason'] for r in ai_result['rejected']}

            listing_details = []
            for l in ai_listings:
                lid = l['avito_item_id']
                detail = {
                    'avito_item_id': lid,
                    'title': l.get('title', ''),
                    'price': float(l['price']) if l.get('price') else None,
                    'snippet_text': (l.get('snippet_text') or '')[:200],
                    'seller_name': l.get('seller_name', ''),
                    'images_count': l.get('images_count', 0),
                }
                if lid in passed_set:
                    detail['ai_decision'] = 'passed'
                    detail['ai_reason'] = None
                elif lid in rejected_map:
                    detail['ai_decision'] = 'rejected'
                    detail['ai_reason'] = rejected_map[lid]
                else:
                    detail['ai_decision'] = 'missing'
                    detail['ai_reason'] = 'Не упомянут AI'
                listing_details.append(detail)

            results.append({
                'articulum': art_name,
                'articulum_id': art_id,
                'total_listings': len(listings),
                'after_filters': len(filtered),
                'sent_to_ai': len(ai_listings),
                'images_sent': img_count,
                'avg_image_kb': round(avg_img_kb, 1),
                'prompt_chars': prompt_chars,
                'usage': {
                    'input_tokens': input_tok,
                    'output_tokens': output_tok,
                    'total_tokens': total_tok,
                },
                'cost': {
                    'input': round(cost_in, 6),
                    'output': round(cost_out, 6),
                    'total': round(cost, 6),
                },
                'tokens_per_image_est': per_image_tokens,
                'elapsed_seconds': round(elapsed, 2),
                'has_thinking_tokens': has_thinking,
                'ai_result': ai_result,
                'raw_response_preview': (content or '')[:1000],
                'listings': listing_details,
                'filter_stats': filter_stats,
            })

    finally:
        await session.close()
        await pool.close()

    # ═══════════════════════════════════════════════
    #  ИТОГОВАЯ СВОДКА
    # ═══════════════════════════════════════════════

    successful = [r for r in results if 'usage' in r]
    n = len(successful)

    if n == 0:
        print("Нет успешных результатов!")
        return

    avg_input = total_input_tokens / n
    avg_output = total_output_tokens / n
    avg_cost = total_cost / n
    avg_listings_sent = sum(r['sent_to_ai'] for r in successful) / n
    avg_images = sum(r['images_sent'] for r in successful) / n
    avg_per_image = sum(r['tokens_per_image_est'] for r in successful) / n

    # Прогноз на 30K артикулов (80% доходят до AI)
    ai_calls_30k = 30_000 * 0.80
    est_input_cost = ai_calls_30k * avg_input * PRICING['input'] / 1_000_000
    est_output_cost = ai_calls_30k * avg_output * PRICING['output'] / 1_000_000
    est_total_cost = est_input_cost + est_output_cost

    print()
    print("=" * 70)
    print("  ИТОГОВАЯ СВОДКА")
    print("=" * 70)
    print()
    print(f"  Успешных тестов:         {n}")
    print(f"  Среднее объявл. → AI:    {avg_listings_sent:.1f}")
    print(f"  Среднее изображ. → AI:   {avg_images:.1f}")
    print()
    print(f"  ┌─── СРЕДНИЕ ТОКЕНЫ НА 1 ЗАПРОС ─────────")
    print(f"  │ Input:       {avg_input:>10,.0f} токенов")
    print(f"  │ Output:      {avg_output:>10,.0f} токенов")
    print(f"  │ ~На 1 фото:  {avg_per_image:>10,.0f} токенов")
    print(f"  └─────────────────────────────────────────")
    print()
    print(f"  Средняя стоимость 1 артикула: ${avg_cost:.4f}")
    print(f"  Thinking токены: {'ДА' if any(r['has_thinking_tokens'] for r in successful) else 'НЕТ'}")
    print()
    print(f"  ┌─── ПРОГНОЗ: 30,000 АРТИКУЛОВ ──────────")
    print(f"  │ AI-запросов:      {ai_calls_30k:>10,.0f}")
    print(f"  │ Input стоимость:  ${est_input_cost:>10,.2f}")
    print(f"  │ Output стоимость: ${est_output_cost:>10,.2f}")
    print(f"  │ ИТОГО:            ${est_total_cost:>10,.2f}")
    print(f"  └─────────────────────────────────────────")
    print()

    # --- Таблица по артикулам ---
    print("  Детали по артикулам:")
    print(f"  {'Артикул':<12} {'Листинг':>7} {'→AI':>4} {'Фото':>4} {'InTok':>8} {'OutTok':>8} {'$':>8} {'Sec':>5}")
    print(f"  {'─'*12} {'─'*7} {'─'*4} {'─'*4} {'─'*8} {'─'*8} {'─'*8} {'─'*5}")
    for r in successful:
        print(f"  {r['articulum']:<12} {r['total_listings']:>7} {r['sent_to_ai']:>4} {r['images_sent']:>4} "
              f"{r['usage']['input_tokens']:>8,} {r['usage']['output_tokens']:>8,} "
              f"${r['cost']['total']:>7.4f} {r['elapsed_seconds']:>5.1f}")
    print()

    # --- Сохранение JSON ---
    summary = {
        'successful_tests': n,
        'avg_listings_sent_to_ai': round(avg_listings_sent, 1),
        'avg_images_sent': round(avg_images, 1),
        'avg_input_tokens': round(avg_input),
        'avg_output_tokens': round(avg_output),
        'avg_tokens_per_image': round(avg_per_image),
        'avg_cost_per_articulum': round(avg_cost, 5),
        'thinking_tokens_detected': any(r['has_thinking_tokens'] for r in successful),
        'totals': {
            'input_tokens': total_input_tokens,
            'output_tokens': total_output_tokens,
            'cost': round(total_cost, 4),
        },
        'estimate_30k_articulums': {
            'ai_calls': int(ai_calls_30k),
            'input_tokens': round(ai_calls_30k * avg_input),
            'output_tokens': round(ai_calls_30k * avg_output),
            'cost_input': round(est_input_cost, 2),
            'cost_output': round(est_output_cost, 2),
            'cost_total': round(est_total_cost, 2),
        },
        'pricing_per_1m': PRICING,
    }

    output = {
        'test_info': {
            'model': MODEL_ID,
            'model_name': MODEL_NAME,
            'pricing_per_1m_tokens': PRICING,
            'date': datetime.now().isoformat(),
            'settings': {
                'min_price': MIN_PRICE,
                'min_validated_items': MIN_VALIDATED_ITEMS,
                'max_images_per_listing': AI_MAX_IMAGES_PER_LISTING,
                'max_listings_for_ai': MAX_LISTINGS_FOR_AI,
                'test_count': TEST_COUNT,
                'selection': 'RANDOM',
            },
        },
        'summary': summary,
        'results': results,
    }

    output_path = '/Users/stepanorlov/Desktop/DONE/zamer/scripts/data/test_kimi_results.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"  Результаты сохранены: {output_path}")
    print()


if __name__ == '__main__':
    asyncio.run(main())
