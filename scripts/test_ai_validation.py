#!/usr/bin/env python3
"""
Тест AI-валидации: берёт объявления с изображениями из БД,
отправляет на Fireworks AI и сохраняет результаты в JSON.

Интерактивный режим — скрипт спрашивает путь к файлу с артикулами,
показывает доступные данные и предлагает выбор.

Использование:
    python scripts/test_ai_validation.py
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# Пути
SCRIPT_DIR = Path(__file__).parent.resolve()
PROJECT_ROOT = SCRIPT_DIR.parent
CONTAINER_DIR = PROJECT_ROOT / 'container'

# Добавляем container/ в sys.path для импортов
sys.path.insert(0, str(CONTAINER_DIR))

from ai_provider import (
    FireworksProvider,
    convert_listing_dict_to_validation,
)
from config import (
    DB_CONFIG,
    FIREWORKS_API_KEY,
    FIREWORKS_MODEL,
    AI_REQUEST_TIMEOUT,
    AI_MAX_RETRIES,
    AI_RETRY_BASE_DELAY,
    AI_MAX_IMAGES_PER_LISTING,
)

import aiohttp
import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [TEST-AI] %(levelname)s: %(message)s',
)
logger = logging.getLogger(__name__)

DEFAULT_FILE = SCRIPT_DIR / 'data' / 'articulums.txt'


# ============================================================
#  Утилиты вывода
# ============================================================

def print_header(text: str):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}")


def print_listings_table(listings: list):
    print(f"\n  {'ID':<15} {'Цена':>8} {'Фото':>5} {'Размер':>8} | Название")
    print(f"  {'-' * 75}")
    for l in listings:
        imgs = l.get('images_bytes') or []
        img_count = len(imgs)
        total_size = sum(len(b) for b in imgs if b) / 1024
        print(f"  {l['avito_item_id']:<15} {str(l['price']):>8} {img_count:>5} {total_size:>6.0f}KB | {l['title'][:42]}")


def print_results(result, listings: list, prev_results: dict):
    passed_set = set(result.passed_ids)
    listing_map = {l['avito_item_id']: l for l in listings}

    print(f"\n  ПРИНЯТО ({result.passed_count}):")
    print(f"  {'ID':<15} {'Цена':>8} | Название")
    print(f"  {'-' * 60}")
    for pid in result.passed_ids:
        l = listing_map.get(pid, {})
        print(f"  {pid:<15} {str(l.get('price', '?')):>8} | {l.get('title', '?')[:42]}")

    print(f"\n  ОТКЛОНЕНО ({result.rejected_count}):")
    print(f"  {'ID':<15} {'Цена':>8} | Причина")
    print(f"  {'-' * 60}")
    for r in result.rejected:
        l = listing_map.get(r.avito_item_id, {})
        print(f"  {r.avito_item_id:<15} {str(l.get('price', '?')):>8} | {r.reason[:50]}")

    if prev_results:
        print(f"\n  СРАВНЕНИЕ С ПРЕДЫДУЩЕЙ AI-ВАЛИДАЦИЕЙ:")
        print(f"  {'ID':<15} {'Было':>10} {'Стало':>10} | Совпадение")
        print(f"  {'-' * 55}")
        matches = compared = 0
        for lid in listing_map:
            if lid in prev_results:
                compared += 1
                was_passed = prev_results[lid]['passed']
                now_passed = lid in passed_set
                if was_passed == now_passed:
                    matches += 1
                marker = "OK" if was_passed == now_passed else "РАЗНИЦА"
                print(f"  {lid:<15} {'PASS' if was_passed else 'REJECT':>10} {'PASS' if now_passed else 'REJECT':>10} | {marker}")
        if compared > 0:
            print(f"\n  Совпадение: {matches}/{compared} ({matches/compared*100:.0f}%)")


# ============================================================
#  Работа с БД
# ============================================================

async def get_listings_with_images(conn, articulum_id: int) -> list:
    rows = await conn.fetch('''
        SELECT avito_item_id, title, price, snippet_text,
               seller_name, seller_id, seller_rating, seller_reviews,
               images_count, images_bytes
        FROM catalog_listings
        WHERE articulum_id = $1
          AND images_bytes IS NOT NULL
          AND array_length(images_bytes, 1) > 0
        ORDER BY price
    ''', articulum_id)
    return [dict(r) for r in rows]


async def get_previous_ai_results(conn, articulum_id: int) -> dict:
    rows = await conn.fetch('''
        SELECT avito_item_id, passed, rejection_reason
        FROM validation_results
        WHERE articulum_id = $1 AND validation_type = 'ai'
    ''', articulum_id)
    return {r['avito_item_id']: {'passed': r['passed'], 'reason': r['rejection_reason']} for r in rows}


async def find_articulums_with_images(conn, articulum_names: list) -> list:
    """Найти артикулы из списка, у которых есть объявления с изображениями."""
    rows = await conn.fetch('''
        SELECT a.id, a.articulum, a.state,
               COUNT(cl.id) as total_listings,
               COUNT(CASE WHEN cl.images_bytes IS NOT NULL
                          AND array_length(cl.images_bytes, 1) > 0
                     THEN 1 END) as with_images
        FROM articulums a
        LEFT JOIN catalog_listings cl ON cl.articulum_id = a.id
        WHERE a.articulum = ANY($1)
        GROUP BY a.id, a.articulum, a.state
        ORDER BY a.articulum
    ''', articulum_names)
    return [dict(r) for r in rows]


# ============================================================
#  Построение JSON-отчёта
# ============================================================

def build_json_report(articulum_info, listings, result, raw_response, prev_results, duration_sec):
    passed_set = set(result.passed_ids)
    listings_info = []
    for l in listings:
        imgs = l.get('images_bytes') or []
        lid = l['avito_item_id']
        entry = {
            'avito_item_id': lid,
            'title': l.get('title'),
            'price': float(l['price']) if l.get('price') else None,
            'snippet_text': l.get('snippet_text'),
            'seller_name': l.get('seller_name'),
            'images_count': len(imgs),
            'images_total_size_kb': round(sum(len(b) for b in imgs if b) / 1024, 1),
            'ai_passed': lid in passed_set,
            'ai_rejection_reason': next(
                (r.reason for r in result.rejected if r.avito_item_id == lid), None
            ),
        }
        if lid in prev_results:
            entry['prev_ai_passed'] = prev_results[lid]['passed']
            entry['prev_ai_reason'] = prev_results[lid]['reason']
            entry['result_changed'] = prev_results[lid]['passed'] != (lid in passed_set)
        listings_info.append(entry)

    return {
        'test_info': {
            'timestamp': datetime.now().isoformat(),
            'articulum': articulum_info['articulum'],
            'articulum_id': articulum_info['id'],
            'articulum_state': articulum_info['state'],
            'total_listings_sent': len(listings),
            'model': FIREWORKS_MODEL,
            'max_images_per_listing': AI_MAX_IMAGES_PER_LISTING,
            'duration_seconds': round(duration_sec, 2),
        },
        'summary': {
            'passed': result.passed_count,
            'rejected': result.rejected_count,
            'pass_rate': round(result.passed_count / len(listings) * 100, 1) if listings else 0,
        },
        'listings': listings_info,
        'ai_response_raw': raw_response,
    }


# ============================================================
#  Создание провайдера с патчем DNS для macOS
# ============================================================

def create_provider():
    provider = FireworksProvider(
        api_key=FIREWORKS_API_KEY,
        model=FIREWORKS_MODEL,
        timeout=AI_REQUEST_TIMEOUT,
        max_retries=AI_MAX_RETRIES,
        retry_base_delay=AI_RETRY_BASE_DELAY,
        max_images_per_listing=AI_MAX_IMAGES_PER_LISTING,
    )

    # Патч: на macOS aiodns не резолвит DNS, используем ThreadedResolver
    async def _get_session_patched(self_provider=provider):
        if self_provider.session is None or self_provider.session.closed:
            resolver = aiohttp.resolver.ThreadedResolver()
            connector = aiohttp.TCPConnector(resolver=resolver)
            self_provider.session = aiohttp.ClientSession(
                headers={
                    "Authorization": f"Bearer {self_provider.api_key}",
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=self_provider.timeout),
                connector=connector,
            )
        return self_provider.session

    provider._get_session = _get_session_patched
    return provider


# ============================================================
#  Валидация одного артикула
# ============================================================

async def validate_one_articulum(conn, art: dict, provider) -> dict:
    """Запуск AI-валидации для одного артикула. Возвращает отчёт."""
    print_header(f"АРТИКУЛ: {art['articulum']} (id={art['id']}, state={art['state']})")

    listings = await get_listings_with_images(conn, art['id'])
    if not listings:
        print("  Нет объявлений с изображениями — пропуск")
        return None

    print_listings_table(listings)

    prev_results = await get_previous_ai_results(conn, art['id'])

    listings_for_ai = [
        convert_listing_dict_to_validation(l, AI_MAX_IMAGES_PER_LISTING)
        for l in listings
    ]
    total_imgs = sum(len(l.images_bytes) for l in listings_for_ai)
    print(f"\n  Отправка: {len(listings_for_ai)} объявлений, {total_imgs} изображений...")

    # Перехват raw response
    raw_response = None
    original_request = provider._request_with_retry

    async def capture_request(messages):
        nonlocal raw_response
        raw_response = await original_request(messages)
        return raw_response

    provider._request_with_retry = capture_request

    start_time = asyncio.get_event_loop().time()
    result = await provider.validate(art['articulum'], listings_for_ai, use_images=True)
    duration = asyncio.get_event_loop().time() - start_time

    # Восстанавливаем оригинальный метод
    provider._request_with_retry = original_request

    print(f"  Ответ за {duration:.1f} сек | PASS: {result.passed_count} | REJECT: {result.rejected_count}")
    print_results(result, listings, prev_results)

    return build_json_report(art, listings, result, raw_response, prev_results, duration)


# ============================================================
#  Интерактивный режим
# ============================================================

def read_articulums_from_file(filepath: Path) -> list:
    """Прочитать артикулы из файла, по одному на строку."""
    articulums = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            art = line.strip()
            if art and art not in articulums:
                articulums.append(art)
    return articulums


async def interactive_mode():
    print_header("ТЕСТ AI-ВАЛИДАЦИИ (интерактивный режим)")

    # 1. Спрашиваем файл
    print(f"\n  Файл по умолчанию: {DEFAULT_FILE}")
    user_path = input("\n  Путь к файлу с артикулами (Enter = по умолчанию): ").strip()

    if user_path:
        filepath = Path(user_path)
        if not filepath.is_absolute():
            filepath = SCRIPT_DIR / filepath
    else:
        filepath = DEFAULT_FILE

    if not filepath.exists():
        print(f"\n  Файл не найден: {filepath}")
        return

    articulums = read_articulums_from_file(filepath)
    print(f"\n  Прочитано артикулов из файла: {len(articulums)}")

    # 2. Подключаемся к БД и ищем артикулы с изображениями
    print("  Подключение к БД...")
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        found = await find_articulums_with_images(conn, articulums)

        if not found:
            print("\n  Ни одного артикула не найдено в БД!")
            return

        # 3. Показываем таблицу
        available = [r for r in found if r['with_images'] > 0]
        no_images = [r for r in found if r['with_images'] == 0 and r['total_listings'] > 0]
        not_parsed = [r for r in found if r['total_listings'] == 0]
        not_in_db = len(articulums) - len(found)

        print_header("СОСТОЯНИЕ АРТИКУЛОВ")

        if available:
            print(f"\n  ГОТОВЫ К ТЕСТУ ({len(available)} шт.) — есть объявления с фото:")
            print(f"  {'#':>3} {'Артикул':<20} {'State':<22} {'Объявл.':>8} {'С фото':>7}")
            print(f"  {'-' * 65}")
            for i, r in enumerate(available, 1):
                print(f"  {i:>3} {r['articulum']:<20} {r['state']:<22} {r['total_listings']:>8} {r['with_images']:>7}")

        if no_images:
            print(f"\n  БЕЗ ФОТО ({len(no_images)} шт.) — есть объявления, но без images_bytes:")
            for r in no_images[:5]:
                print(f"      {r['articulum']:<20} {r['state']:<22} listings={r['total_listings']}")
            if len(no_images) > 5:
                print(f"      ... и ещё {len(no_images) - 5}")

        if not_parsed:
            print(f"\n  НЕ СПАРСЕНЫ ({len(not_parsed)} шт.) — в БД, но без объявлений:")
            for r in not_parsed[:5]:
                print(f"      {r['articulum']:<20} state={r['state']}")
            if len(not_parsed) > 5:
                print(f"      ... и ещё {len(not_parsed) - 5}")

        if not_in_db > 0:
            print(f"\n  НЕ В БД: {not_in_db} артикулов из файла отсутствуют в базе")

        if not available:
            print("\n  Нет артикулов с изображениями для теста!")
            return

        # 4. Выбор
        print(f"\n  Что тестировать?")
        print(f"    [a] Все {len(available)} артикулов с фото")
        if len(available) > 1:
            print(f"    [1-{len(available)}] Конкретный номер из списка")
            print(f"    [1,3,5] Несколько номеров через запятую")
        else:
            print(f"    [1] Единственный доступный")

        choice = input("\n  Выбор (Enter = все): ").strip().lower()

        if choice == '' or choice == 'a':
            selected = available
        else:
            try:
                indices = [int(x.strip()) for x in choice.split(',')]
                selected = [available[i - 1] for i in indices if 1 <= i <= len(available)]
            except (ValueError, IndexError):
                print("  Неверный ввод!")
                return

        if not selected:
            print("  Ничего не выбрано!")
            return

        print(f"\n  Выбрано: {len(selected)} артикулов")

        # 5. Запуск валидации
        provider = create_provider()
        all_reports = []

        for art in selected:
            try:
                report = await validate_one_articulum(conn, art, provider)
                if report:
                    all_reports.append(report)
            except Exception as e:
                logger.error(f"Ошибка при валидации {art['articulum']}: {e}")

        await provider.close()

        if not all_reports:
            print("\n  Нет результатов для сохранения")
            return

        # 6. Сохранение JSON
        output_dir = SCRIPT_DIR / 'data'
        output_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if len(all_reports) == 1:
            output_file = output_dir / f'ai_test_{all_reports[0]["test_info"]["articulum"]}_{timestamp}.json'
            save_data = all_reports[0]
        else:
            output_file = output_dir / f'ai_test_batch_{len(all_reports)}_{timestamp}.json'
            total_passed = sum(r['summary']['passed'] for r in all_reports)
            total_rejected = sum(r['summary']['rejected'] for r in all_reports)
            total_listings = sum(r['test_info']['total_listings_sent'] for r in all_reports)
            total_duration = sum(r['test_info']['duration_seconds'] for r in all_reports)
            save_data = {
                'batch_info': {
                    'timestamp': datetime.now().isoformat(),
                    'articulums_count': len(all_reports),
                    'total_listings': total_listings,
                    'total_duration_seconds': round(total_duration, 2),
                    'model': FIREWORKS_MODEL,
                },
                'batch_summary': {
                    'total_passed': total_passed,
                    'total_rejected': total_rejected,
                    'overall_pass_rate': round(total_passed / total_listings * 100, 1) if total_listings else 0,
                },
                'articulums': all_reports,
            }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(save_data, f, ensure_ascii=False, indent=2, default=str)

        # 7. Итого
        print_header("ИТОГО")
        print(f"\n  Артикулов протестировано: {len(all_reports)}")
        total_l = sum(r['test_info']['total_listings_sent'] for r in all_reports)
        total_p = sum(r['summary']['passed'] for r in all_reports)
        total_r = sum(r['summary']['rejected'] for r in all_reports)
        total_d = sum(r['test_info']['duration_seconds'] for r in all_reports)
        print(f"  Объявлений отправлено: {total_l}")
        print(f"  Принято: {total_p}")
        print(f"  Отклонено: {total_r}")
        print(f"  Pass rate: {total_p / total_l * 100:.1f}%" if total_l else "  Pass rate: 0%")
        print(f"  Общее время: {total_d:.1f} сек")
        print(f"\n  JSON: {output_file}")
        print(f"  Размер: {output_file.stat().st_size / 1024:.1f} KB\n")

    finally:
        await conn.close()


def main():
    asyncio.run(interactive_mode())


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Прервано пользователем")
        sys.exit(0)
