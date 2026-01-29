#!/usr/bin/env python3
"""Экспорт брендов и цен по артикулам.

Скрипт читает список артикулов из текстового файла (по умолчанию
``validated_missing_qwen.txt``), собирает тексты объявлений из БД и
пытается извлечь возможные бренды на основе токенов в названиях и
описаниях. Также добавляется минимальная цена по артикулу. Результат
сохраняется в Excel.

Алгоритм выделения брендов:
- разбивает текст на слова (поддерживаются кириллица и латиница);
- фильтрует стоп-слова и числовые токены;
- объединяет последовательности слов длиной до трёх, чтобы поймать
  составные бренды вроде «LAND ROVER» или «MERCEDES BENZ»;
- сохраняет частоты встречаемости и выводит бренды по убыванию
  частоты.

Подходит для больших списков брендов и не зависит от конкретного
языка написания в тексте.
"""

import argparse
import asyncio
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Sequence

import asyncpg
import pandas as pd

# Добавляем модули из container для доступа к config.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'container'))
from config import DB_CONFIG  # noqa: E402


WORD_RE = r"[^0-9A-Za-zА-Яа-яЁё]+"

# Кириллические символы, визуально совпадающие с латиницей.
LOOKALIKE_TRANSLIT = str.maketrans({
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H', 'О': 'O',
    'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X', 'Ё': 'E',
    'а': 'a', 'в': 'b', 'е': 'e', 'к': 'k', 'м': 'm', 'н': 'h', 'о': 'o',
    'р': 'p', 'с': 'c', 'т': 't', 'у': 'y', 'х': 'x', 'ё': 'e',
})
LOOKALIKE_CYR_SET = set(LOOKALIKE_TRANSLIT.keys())

BRAND_KEYS = [
    'производитель',
    'марка',
    'бренд',
    'марка автомобиля',
    'бренд (производитель)',
    'производитель (бренд)',
    'brand',
    'manufacturer',
    'make',
]

# Базовые стоп-слова (верхний регистр) — чтобы не путать общие слова с брендами
STOPWORDS = {
    'ОРИГИНАЛ', 'АРТИКУЛ', 'ЗАПЧАСТИ', 'ЗАПЧАСТЬ', 'НОМЕР', 'НОМЕРА',
    'ПРОИЗВОДИТЕЛЬ', 'ПРОИЗВОДСТВА', 'ПРОИЗВОДИТЕЛЯ', 'ДЛЯ', 'НА', 'С',
    'БЕЗ', 'ПОД', 'ПЕРЕДНИЙ', 'ЗАДНИЙ', 'ЛЕВЫЙ', 'ПРАВЫЙ', 'ВЕРХНИЙ',
    'НИЖНИЙ', 'КОМПЛЕКТ', 'КОМПЛЕКТА', 'ДВИГАТЕЛЬ', 'КУЗОВ', 'ГОД',
    'ОРIGINAL', 'ORIGINAL', 'OEM', 'PART', 'PARTS', 'GENUINE', 'OF',
    'FOR', 'THE', 'AND', 'WITH', 'WITHOUT', 'НАБОР', 'КОРОБКА', 'КЛАПАН',
    'ДАТЧИК', 'ЛАМПА', 'СВЕЧА', 'ТОРМОЗ', 'ТОРМОЗНЫЕ', 'ДИСК', 'ДИСКИ',
    'КОЛОДКИ', 'ЩЁТКИ', 'ЩЕТКИ', 'ПРИВОД', 'АМОРТИЗАТОР', 'ФАРА', 'ФАРЫ',
    'ФИЛЬТР', 'ФИЛЬТРЫ', 'САЛОННЫЙ', 'МАСЛЯНЫЙ', 'ВОЗДУШНЫЙ',
    'PRODUCER', 'BRAND', 'NUMBER', 'MODEL', 'MODELS', 'PARTS', 'KIT',
    'SET', 'NEW', 'USED', 'OR', 'AND', 'FROM', 'НАЧАЛО', 'КОНЕЦ', 'ШТ',
    'ШТУК', 'ОРИГ', 'ОРИГИНАЛЬНЫЙ', 'ОРИГИНАЛЬНАЯ', 'ОРИГИНАЛЬНОЕ',
}

EN_STOPWORDS = {
    'THE', 'A', 'AN', 'FOR', 'AND', 'WITH', 'WITHOUT', 'FROM', 'BY', 'ON',
    'IN', 'AT', 'OF', 'TO', 'NEW', 'ORIGINAL', 'GENUINE', 'OEM', 'PART',
    'PARTS', 'SET', 'KIT', 'ALL', 'LEFT', 'RIGHT', 'FRONT', 'REAR', 'TOP',
    'BOTTOM', 'UPPER', 'LOWER', 'SIDE', 'ENGINE', 'BODY', 'CAR', 'AUTO',
    'SPARE', 'SPAREPARTS', 'QUALITY', 'STOCK', 'AVAILABLE', 'YEAR',
    'MODEL', 'MODELS', 'TYPE', 'ITEM', 'NUMBER', 'CODE', 'BRAND', 'MAKE',
    'SUITABLE', 'OR', 'NOT', 'INCLUDING', 'INCLUDES', 'SUPPORTS', 'LINE',
    'SYSTEM', 'UNIT', 'ASSEMBLY', 'GROUP', 'PRODUCT', 'SHIPPING', 'DELIVERY',
    'ORIGINALS', 'COMPLETE', 'PACKAGE', 'PAYMENT', 'WARRANTY', 'GUARANTEE',
    'STOCKS', 'STOCKED', 'FACTORY', 'PLANT', 'HOUSE', 'CENTER', 'SERVICE',
    'USA', 'EU', 'OEMPART', 'AUTOPARTS', 'AUTO PARTS', 'CAR PARTS',
}

RU_STOPWORDS = {
    'И', 'В', 'ДЛЯ', 'НА', 'ПО', 'С', 'О', 'ОТ', 'БЕЗ', 'ПОД', 'МЕЖДУ',
    'ЧЕРЕЗ', 'У', 'К', 'ИЗ', 'ИСПОЛЬЗОВАНИЕ', 'ДО', 'ПОСЛЕ', 'ЛЕВЫЙ',
    'ПРАВЫЙ', 'ПЕРЕДНИЙ', 'ЗАДНИЙ', 'ВЕРХНИЙ', 'НИЖНИЙ', 'СПРАВА', 'СЛЕВА',
    'ОРИГИНАЛ', 'ОРИГИНАЛЬНЫЙ', 'НОВЫЙ', 'БУ', 'Б/У', 'КОМПЛЕКТ', 'ЗАПЧАСТЬ',
    'ЗАПЧАСТИ', 'НОМЕР', 'КОД', 'АРТИКУЛ', 'ДЕТАЛЬ', 'ДЕТАЛИ', 'ТОВАР',
    'ТОВАРА', 'СОСТОЯНИЕ', 'ДОСТАВКА', 'ОТПРАВКА', 'ПРОДАЖА', 'МАГАЗИН',
    'СКЛАД', 'ВАЛ', 'РЫЧАГ', 'КОЛЕСО', 'ДИСК', 'КОЛОДКИ', 'НАБОРА', 'НАЛИЧИИ',
    'РЕМОНТ', 'ПРОИЗВОДИТЕЛЬ', 'ПРОИЗВОДСТВА', 'УСТАНОВКА', 'ИНСТРУКЦИЯ',
    'СОВМЕСТИМОСТЬ', 'ПРИМЕНЯЕМОСТЬ', 'ГАРАНТИЯ', 'БЫСТРАЯ', 'ОТПРАВИМ',
    'СРАЗУ', 'ЛУЧШЕЕ', 'КАЧЕСТВО', 'АКЦИЯ', 'СКИДКА', 'НАЛОЖЕННЫЙ', 'ПЛАТЕЖ',
    'ПОДХОДИТ', 'ПОДХОДИТЬ', 'ПРОДАЮ', 'ПРОДАМ', 'ОБЪЯВЛЕНИЕ', 'ОРИГ',
    'ОРИГИНАЛЬНАЯ', 'ОРИГИНАЛЬНЫЕ', 'ПРОДАЖИ', 'ПОСТАВКА', 'ПОСТАВЩИК',
    'ДВИГАТЕЛЬ', 'КПП', 'АКПП', 'ДВС', 'МКПП', 'КОРОБКА', 'РАЗДАТКА', 'МОСТ',
    'СЦЕПЛЕНИЕ', 'ТОРМОЗ', 'ТОРМОЗНОЙ', 'ТОРМОЗНЫЕ', 'ПЕРЕДАЧА', 'РЕМЕНЬ',
    'НАТЯЖИТЕЛЬ', 'НАСОС', 'ПОДВЕСКА', 'САЙЛЕНТБЛОК', 'ШРУС', 'СТУПИЦА',
    'АМОРТИЗАТОР', 'САЙЛЕНТ', 'СТАБИЛИЗАТОР', 'ТЯГА', 'РЕЙКА', 'РУЛЕВАЯ',
    'ТУРБИНА', 'УПЛОТНИТЕЛЬ', 'КОЖУХ', 'ЩИТ', 'ФАРА', 'ФАРЫ', 'ЗАЩИТА',
    'КОРПУС', 'КОЖА', 'КОМФОРТ', 'ПРОФЕССИОНАЛЬНАЯ', 'РАБОТА', 'ДОГОВОР',
    'ЗАМЕНА', 'ОРИГИНАЛОМ', 'ОКРАСКА', 'ЦВЕТ', 'ЧЁРНЫЙ', 'ЧЕРНЫЙ', 'БЕЛЫЙ',
    'СЕРЫЙ', 'СИНИЙ', 'КРАСНЫЙ', 'ЗЕЛЕНЫЙ', 'ЖЕЛТЫЙ', 'КОФЕЙНЫЙ', 'БЕЖЕВЫЙ',
}

STOPWORDS = STOPWORDS | EN_STOPWORDS | RU_STOPWORDS

CYRILLIC_BRANDS = {
    'ВАЗ', 'ЛАДА', 'LADA', 'УАЗ', 'ГАЗ', 'ГАЗЕЛЬ', 'КАМАЗ', 'ЗИЛ',
    'ЗМЗ', 'МАЗ', 'БЕЛАЗ', 'ПАЗ', 'НЕФАЗ', 'ТАГАЗ', 'НИВА', 'МОСКВИЧ',
    'СОБОЛЬ', 'ВОЛГА', 'АЗЛК', 'БЕЛМАГ', 'ПЕКАР', 'БРТ', 'АВТОВАЗ',
    'СИБИРЬ', 'СИБКАР', 'ГРУЗОВИКИ', 'ЗАЗ', 'ЛИАЗ', 'КРАЗ', 'УРАЛ',
    'КИРОВЕЦ', 'ДОН', 'ЕВРОТЕХ', 'ДЕТАЛИ МАШИН', 'ГАЗОН', 'СПЕЦМАШ',
    'AURUS', 'Ё-МОБИЛЬ', 'ИЖ', 'ИЖЕВСК', 'СОКОЛ', 'ЛУАЗ', 'РОСТАР',
    'РОСДЕТАЛЬ', 'РОСТСЕЛЬМАШ', 'АГРОМАШ', 'БАВ',
}

BASE_BRANDS = {
    'AUDI', 'VOLKSWAGEN', 'VW', 'VAG', 'SKODA', 'SEAT', 'PORSCHE', 'LAMBORGHINI',
    'BENTLEY', 'BUGATTI', 'MERCEDES', 'MERCEDES BENZ', 'BENZ', 'MAYBACH', 'AMG',
    'BMW', 'MINI', 'ROLLS ROYCE', 'TOYOTA', 'LEXUS', 'NISSAN', 'INFINITI',
    'HONDA', 'ACURA', 'MAZDA', 'SUZUKI', 'MITSUBISHI', 'SUBARU', 'ISUZU',
    'DAIHATSU', 'HYUNDAI', 'KIA', 'DAEWOO', 'SSANGYONG', 'CHEVROLET', 'CADILLAC',
    'GMC', 'BUICK', 'CHRYSLER', 'DODGE', 'JEEP', 'RAM', 'FORD', 'LINCOLN',
    'OPEL', 'VAUXHALL', 'PEUGEOT', 'CITROEN', 'DS', 'RENAULT', 'DACIA',
    'ALFA ROMEO', 'FIAT', 'LANCIA', 'MASERATI', 'FERRARI', 'ASTON MARTIN',
    'JAGUAR', 'LAND ROVER', 'RANGE ROVER', 'VOLVO', 'SAAB', 'GEELY', 'CHERY',
    'HAVAL', 'GREAT WALL', 'BYD', 'MG', 'LIFAN', 'FAW', 'BAIC', 'JAC', 'ZOTYE',
    'GONOW', 'PROTON', 'PERODUA', 'TATA', 'MAHINDRA', 'HINO', 'SCANIA', 'MAN',
    'IVECO', 'DAF', 'BOSCH', 'HELLA', 'LUK', 'SACHS', 'ZF', 'VDO', 'ATE', 'TRW',
    'BREMBO', 'FERODO', 'TEXTAR', 'JURID', 'BENDIX', 'NISSIN', 'NISSENS', 'MAHLE',
    'KNECHT', 'MANN', 'HENGST', 'FILTRON', 'WIX', 'K&N', 'DELPHI', 'DENSO',
    'NGK', 'BERU', 'VALEO', 'DAYCO', 'GATES', 'CONTINENTAL', 'INA', 'FEBI',
    'MEYLE', 'SWAG', 'MAPCO', 'KYB', 'MONROE', 'BILSTEIN', 'LEMFOERDER',
    'LEMFORDER', 'FEBEST', 'CTR', 'NOK', 'AISIN', 'HITACHI', 'OSRAM', 'PHILIPS',
    'PIERBURG', 'WAHLER', 'TOPRAN', 'HERTH+BUSS', 'HANS PRIES', 'BORGWARNER',
    'GARRETT', 'HOLSET', 'AKEBONO', 'SUMITOMO', 'ADVICS', 'MOBIL', 'CASTROL',
    'SHELL', 'TOTAL', 'LIQUI MOLY', 'MOTUL', 'RAVENOL', 'PETRONAS', 'FUCHS',
    'ENEOS', 'IDEMITSU', 'ZIC', 'ERSA', 'VAICO', 'SCHAEFFLER', 'NTN', 'NSK',
    'SKF', 'TIMKEN', 'KAYABA', 'BOSAL', 'WALKER', 'SIMENS', 'SIEMENS', 'HENGSTLER',
    # Powersports / BRP
    'BRP', 'CAN AM', 'CAN-AM', 'CANAM', 'SEA DOO', 'SEA-DOO', 'SEADOO',
    'SKI DOO', 'SKI-DOO', 'SKIDOO', 'LYNX', 'OUTLANDER', 'MAVERICK', 'SPARK',
    'COMMANDER', 'DEFENDER', 'RENEGADE', 'SUMMIT', 'XPS', 'LINQ', 'G3',
    'CAN AM SPYDER', 'SPYDER', 'RYKER', 'ROTAX', 'EVINRUDE', 'SEA', 'DOO',
    # Marine electronics / Mercury
    'MERCURY', 'MERCURY MARINE', 'QUICKSILVER', 'VESSELVIEW', 'SMARTCRAFT',
    'ACTIVE TRIM', 'DTS', 'NMEA',
}


def chunked(seq: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    """Разбивает список на батчи фиксированного размера."""

    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _should_transliterate(token: str) -> bool:
    letters = [ch for ch in token if ch.isalpha()]
    if not letters:
        return False
    has_cyr = any('А' <= ch <= 'Я' or 'а' <= ch <= 'я' or ch in {'Ё', 'ё'} for ch in letters)
    if not has_cyr:
        return False
    has_latin = any('A' <= ch <= 'Z' or 'a' <= ch <= 'z' for ch in letters)
    if has_latin:
        return True
    # Если все кириллические буквы входят в lookalike набор — считаем это псевдо-латиницей.
    return all(ch in LOOKALIKE_CYR_SET for ch in letters)


def normalize_token(token: str) -> str:
    token = token.replace('-', ' ').strip()
    if not token:
        return ''
    if _should_transliterate(token):
        token = token.translate(LOOKALIKE_TRANSLIT)
    token = token.replace('Ё', 'Е').replace('ё', 'е')
    return token.strip().upper()


def is_brand_word(token: str) -> bool:
    if not token or len(token) < 2:
        return False
    if token in STOPWORDS:
        return False
    if any(ch.isdigit() for ch in token):
        return False
    has_alpha = any(ch.isalpha() for ch in token)
    if not has_alpha:
        return False
    has_cyrillic = any('А' <= ch <= 'Я' or ch == 'Ё' for ch in token)
    has_latin = any('A' <= ch <= 'Z' for ch in token)
    if has_cyrillic and not has_latin and token not in CYRILLIC_BRANDS:
        return False
    # Исключаем слова, где только одна буква и цифры (пример: «A6»)
    if len(token) == 2 and sum(ch.isalpha() for ch in token) == 1:
        return False
    return True


def extract_brand_candidates(
    text: str,
    limit: int = 5,
    allowed_phrases: Sequence[str] | None = None,
) -> List[str]:
    if not text:
        return []

    # Используем Counter, чтобы понимать частоту встречаемости
    counter: Counter[str] = Counter()
    buffer: List[str] = []

    def flush_buffer():
        nonlocal buffer
        if not buffer:
            return
        max_len = min(3, len(buffer))
        for size in range(1, max_len + 1):
            for i in range(len(buffer) - size + 1):
                phrase = ' '.join(buffer[i : i + size])
                if not phrase or phrase in STOPWORDS:
                    continue
                if allowed_phrases and phrase not in allowed_phrases:
                    continue
                counter[phrase] += 1
        buffer = []

    for raw_token in re.split(WORD_RE, text):
        token = normalize_token(raw_token)
        if is_brand_word(token):
            buffer.append(token)
        else:
            flush_buffer()

    flush_buffer()
    if not counter:
        return []
    # Сортируем по частоте, потом по алфавиту для детерминированности
    sorted_items = sorted(
        counter.items(), key=lambda item: (-item[1], item[0])
    )
    brands = [item[0] for item in sorted_items]
    return brands[:limit]


def split_structured_brands(values: Sequence[str]) -> List[str]:
    """Разбивает значения из характеристик на отдельные бренды."""

    results: List[str] = []
    seen = set()
    for value in values or []:
        for chunk in re.split(r"[,/;|]+", value):
            token = normalize_token(chunk)
            if not token or token in seen:
                continue
            if is_brand_word(token):
                seen.add(token)
                results.append(token)
    return results


def filter_brands(brands: Sequence[str]) -> List[str]:
    """Удаляет пустые и слишком короткие бренды (менее 3 символов)."""

    filtered: List[str] = []
    seen: set[str] = set()
    for brand in brands:
        if not brand:
            continue
        candidate = brand.strip()
        if not candidate:
            continue
        # Убираем пробелы и спецсимволы при подсчёте длины
        length_key = re.sub(r"[^A-Z0-9А-ЯЁ]", "", candidate, flags=re.IGNORECASE)
        if len(length_key) < 3:
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        filtered.append(candidate)
    return filtered


async def fetch_articulum_data(
    conn: asyncpg.Connection, articulums: Sequence[str]
):
    """Загружает тексты, бренды и цены из БД для пачки артикулов."""

    query = """
    WITH target AS (
        SELECT id, articulum
        FROM articulums
        WHERE articulum = ANY($1::text[])
    ), catalog_ranked AS (
        SELECT
            articulum_id,
            price,
            COALESCE(title, '') || ' ' || COALESCE(snippet_text, '') AS text_blob,
            ROW_NUMBER() OVER (
                PARTITION BY articulum_id
                ORDER BY price ASC NULLS LAST, id ASC
            ) AS price_rank
        FROM catalog_listings
        WHERE articulum_id IN (SELECT id FROM target)
    ), catalog AS (
        SELECT
            articulum_id,
            MIN(price) FILTER (WHERE price IS NOT NULL) AS min_price,
            STRING_AGG(text_blob, ' ||| ' ORDER BY price) FILTER (WHERE price_rank <= 3)
                AS text_blob
        FROM catalog_ranked
        GROUP BY articulum_id
    ), validated_catalog AS (
        SELECT cl.*
        FROM catalog_listings cl
        WHERE cl.articulum_id IN (SELECT id FROM target)
          AND EXISTS (
              SELECT 1 FROM validation_results vr1
              WHERE vr1.avito_item_id = cl.avito_item_id
                AND vr1.validation_type = 'price_filter'
                AND vr1.passed = TRUE
          )
          AND EXISTS (
              SELECT 1 FROM validation_results vr2
              WHERE vr2.avito_item_id = cl.avito_item_id
                AND vr2.validation_type = 'mechanical'
                AND vr2.passed = TRUE
          )
          AND EXISTS (
              SELECT 1 FROM validation_results vr3
              WHERE vr3.avito_item_id = cl.avito_item_id
                AND vr3.validation_type = 'ai'
                AND vr3.passed = TRUE
          )
    ), validated_ranked AS (
        SELECT
            articulum_id,
            price,
            COALESCE(title, '') || ' ' || COALESCE(snippet_text, '') AS text_blob,
            ROW_NUMBER() OVER (
                PARTITION BY articulum_id
                ORDER BY price ASC NULLS LAST, id ASC
            ) AS price_rank
        FROM validated_catalog
    ), validated AS (
        SELECT
            articulum_id,
            MIN(price) FILTER (WHERE price IS NOT NULL) AS min_price,
            STRING_AGG(text_blob, ' ||| ' ORDER BY price) FILTER (WHERE price_rank <= 3)
                AS text_blob
        FROM validated_ranked
        GROUP BY articulum_id
    ), brand_values AS (
        SELECT
            o.articulum_id,
            ARRAY_AGG(DISTINCT NULLIF(TRIM(kv.value), '')) FILTER (
                WHERE kv.value IS NOT NULL
            ) AS brands
        FROM object_data o
        CROSS JOIN LATERAL jsonb_each_text(o.characteristics) AS kv(key, value)
        WHERE o.articulum_id IN (SELECT id FROM target)
          AND o.avito_item_id IN (SELECT DISTINCT avito_item_id FROM validated_catalog)
          AND lower(kv.key) = ANY($2::text[])
        GROUP BY o.articulum_id
    )
    SELECT
        t.articulum,
        v.min_price AS validated_min_price,
        COALESCE(v.text_blob, '') AS validated_text,
        c.min_price AS catalog_min_price,
        COALESCE(c.text_blob, '') AS catalog_text,
        COALESCE(b.brands, ARRAY[]::text[]) AS brand_values
    FROM target t
    LEFT JOIN validated v ON v.articulum_id = t.id
    LEFT JOIN catalog c ON c.articulum_id = t.id
    LEFT JOIN brand_values b ON b.articulum_id = t.id
    ORDER BY t.articulum;
    """

    return await conn.fetch(query, list(articulums), BRAND_KEYS)


async def build_dataset(articulums: Sequence[str]):
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        rows = []
        for batch in chunked(list(articulums), 400):
            rows.extend(await fetch_articulum_data(conn, batch))
        return rows
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Экспорт брендов и минимальных цен по артикулам'
    )
    parser.add_argument(
        '--input',
        default='validated_missing_qwen.txt',
        help='Путь к текстовому файлу со списком артикулов (один на строку)',
    )
    parser.add_argument(
        '--output',
        default='validated_missing_qwen_brands.xlsx',
        help='Путь к результирующему Excel файлу',
    )
    return parser.parse_args()


def load_articulums(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f'Не найден файл со списком артикулов: {path}')
    articulums = []
    with path.open(encoding='utf-8') as handle:
        for line in handle:
            art = line.strip()
            if art:
                articulums.append(art)
    if not articulums:
        raise ValueError('Список артикулов пуст')
    return articulums


def main():
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    articulums = load_articulums(input_path)
    print(f'Загружено {len(articulums)} артикулов из {input_path}')

    rows = asyncio.run(build_dataset(articulums))

    structured_map = {}
    known_brands = set(BASE_BRANDS)
    for row in rows:
        structured = split_structured_brands(row['brand_values'])
        structured_map[row['articulum']] = structured
        known_brands.update(structured)

    records = []
    for row in rows:
        validated_price = row['validated_min_price']
        min_price = validated_price if validated_price is not None else None

        text_sources = [row['validated_text'], row['catalog_text']]
        text = ' '.join(filter(None, text_sources))

        structured_brands = structured_map.get(row['articulum'], [])
        if structured_brands:
            brands = structured_brands[:5]
        else:
            brands = extract_brand_candidates(
                text,
                allowed_phrases=known_brands,
            )
        brands = filter_brands(brands)
        records.append(
            {
                'articulum': row['articulum'],
                'min_price': float(min_price) if min_price is not None else None,
                'brands': ', '.join(brands),
            }
        )

    if not records:
        raise RuntimeError('Не удалось собрать данные по указанным артикулам')

    df = pd.DataFrame(records)
    df.sort_values('articulum', inplace=True)
    df.to_excel(output_path, index=False)
    print(f'✅ Сохранено {len(df)} строк в {output_path}')


if __name__ == '__main__':
    main()
