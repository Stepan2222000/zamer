#!/usr/bin/env python3
"""
Экспорт артикулов с валидными объявлениями

Создает два файла:
- validated_articulums_with_counts.txt - артикулы с количеством валидных объявлений
- validated_articulums.txt - просто список артикулов

Валидное объявление = прошло все три этапа (price_filter, mechanical, ai)
"""
import asyncio
import asyncpg
import os
import sys

# Добавляем путь к модулям container
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'container'))

from config import DB_CONFIG


async def export_validated_articulums():
    """Экспорт артикулов с валидными объявлениями"""
    conn = await asyncpg.connect(**DB_CONFIG)

    try:
        # SQL запрос для подсчета валидных объявлений по артикулам
        # Объявление валидно, если прошло все три этапа валидации
        # ВАЖНО: Артикул должен быть в статусе VALIDATED или OBJECT_PARSING
        query = """
        SELECT
            a.articulum,
            COUNT(DISTINCT cl.avito_item_id) as valid_count
        FROM articulums a
        INNER JOIN catalog_listings cl ON a.id = cl.articulum_id
        WHERE a.state IN ('VALIDATED', 'OBJECT_PARSING')
        AND EXISTS (
            -- Проверяем, что объявление прошло price_filter
            SELECT 1 FROM validation_results vr1
            WHERE vr1.avito_item_id = cl.avito_item_id
            AND vr1.validation_type = 'price_filter'
            AND vr1.passed = TRUE
        )
        AND EXISTS (
            -- Проверяем, что объявление прошло mechanical
            SELECT 1 FROM validation_results vr2
            WHERE vr2.avito_item_id = cl.avito_item_id
            AND vr2.validation_type = 'mechanical'
            AND vr2.passed = TRUE
        )
        AND EXISTS (
            -- Проверяем, что объявление прошло ai
            SELECT 1 FROM validation_results vr3
            WHERE vr3.avito_item_id = cl.avito_item_id
            AND vr3.validation_type = 'ai'
            AND vr3.passed = TRUE
        )
        GROUP BY a.articulum
        HAVING COUNT(DISTINCT cl.avito_item_id) > 0
        ORDER BY valid_count DESC, a.articulum;
        """

        rows = await conn.fetch(query)

        if not rows:
            print("❌ Нет артикулов с валидными объявлениями")
            return

        # Путь к корню проекта
        project_root = os.path.join(os.path.dirname(__file__), '..')

        # Создаем файл с количествами
        counts_file = os.path.join(project_root, 'validated_articulums_with_counts.txt')
        with open(counts_file, 'w', encoding='utf-8') as f:
            for idx, row in enumerate(rows, start=1):
                f.write(f"{idx:6d}→{row['articulum']} - {row['valid_count']} объявлений\n")

        # Создаем файл без количеств (только артикулы)
        simple_file = os.path.join(project_root, 'validated_articulums.txt')
        with open(simple_file, 'w', encoding='utf-8') as f:
            for row in rows:
                f.write(f"{row['articulum']}\n")

        print(f"✅ Экспортировано {len(rows)} артикулов с валидными объявлениями")
        print(f"📄 Файл с количествами: {counts_file}")
        print(f"📄 Файл без количеств: {simple_file}")

        # Статистика
        total_items = sum(row['valid_count'] for row in rows)
        avg_items = total_items / len(rows) if rows else 0

        print(f"\n📊 Статистика:")
        print(f"   Всего артикулов: {len(rows)}")
        print(f"   Всего валидных объявлений: {total_items}")
        print(f"   Среднее объявлений на артикул: {avg_items:.1f}")
        print(f"   Минимум: {rows[-1]['valid_count']} объявлений")
        print(f"   Максимум: {rows[0]['valid_count']} объявлений")

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(export_validated_articulums())
