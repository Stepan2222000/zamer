"""Скрипт для генерации аналитического отчета по артикулам и их объявлениям"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, List

from database import connect_db

try:
    from tabulate import tabulate
    TABULATE_AVAILABLE = True
except ImportError:
    TABULATE_AVAILABLE = False


def load_articulums_from_file() -> List[str]:
    """Прочитать артикулы из файла scripts/data/report_articulums.txt"""
    filepath = Path(__file__).parent / 'data' / 'report_articulums.txt'

    if not filepath.exists():
        return []

    articulums = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # Пропустить пустые строки и комментарии
            if not line or line.startswith('#'):
                continue
            articulums.append(line)

    return articulums


async def recreate_report_table(conn):
    """Очистить и пересоздать таблицу analytics_articulum_report"""
    await conn.execute("DROP TABLE IF EXISTS analytics_articulum_report CASCADE")

    # Создать таблицу заново
    create_table_sql = """
    CREATE TABLE analytics_articulum_report (
        id SERIAL PRIMARY KEY,
        rejection_reason TEXT,
        articulum_id INTEGER NOT NULL REFERENCES articulums(id) ON DELETE CASCADE,
        articulum VARCHAR(255) NOT NULL,
        avito_item_id VARCHAR(255) NOT NULL,

        -- Данные объявления
        title TEXT,
        price NUMERIC,
        seller_name VARCHAR(500),

        -- Результаты валидации: Price Filter
        price_filter_passed BOOLEAN,
        price_filter_reason TEXT,

        -- Результаты валидации: Mechanical
        mechanical_passed BOOLEAN,
        mechanical_reason TEXT,

        -- Результаты валидации: AI
        ai_passed BOOLEAN,
        ai_reason TEXT,

        -- Итоговый результат
        final_passed BOOLEAN NOT NULL,
        rejection_stage VARCHAR(50),

        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX idx_analytics_report_articulum ON analytics_articulum_report(articulum_id, avito_item_id);
    CREATE INDEX idx_analytics_report_item ON analytics_articulum_report(avito_item_id);
    CREATE INDEX idx_analytics_report_passed ON analytics_articulum_report(final_passed);
    CREATE INDEX idx_analytics_report_stage ON analytics_articulum_report(rejection_stage);
    """

    await conn.execute(create_table_sql)
    print("✓ Таблица analytics_articulum_report пересоздана")


async def collect_and_insert_data(conn, filter_articulums: List[str] = None):
    """Собрать данные из БД и вставить в analytics_articulum_report

    Args:
        conn: подключение к БД
        filter_articulums: список артикулов для фильтрации (None = все артикулы)
    """

    # SQL запрос для сбора данных с pivot по validation_results
    if filter_articulums:
        # С фильтром по артикулам
        query = """
        WITH validation_pivot AS (
            SELECT
                articulum_id,
                avito_item_id,
                BOOL_OR(validation_type = 'price_filter' AND passed) AS price_filter_passed,
                MAX(CASE WHEN validation_type = 'price_filter' AND NOT passed THEN rejection_reason END) AS price_filter_reason,
                BOOL_OR(validation_type = 'mechanical' AND passed) AS mechanical_passed,
                MAX(CASE WHEN validation_type = 'mechanical' AND NOT passed THEN rejection_reason END) AS mechanical_reason,
                BOOL_OR(validation_type = 'ai' AND passed) AS ai_passed,
                MAX(CASE WHEN validation_type = 'ai' AND NOT passed THEN rejection_reason END) AS ai_reason,
                BOOL_OR(validation_type = 'ai') AS has_ai_validation
            FROM validation_results
            GROUP BY articulum_id, avito_item_id
        )
        SELECT
            a.id AS articulum_id,
            a.articulum,
            cl.avito_item_id,
            cl.title,
            cl.price,
            cl.seller_name,
            COALESCE(vp.price_filter_passed, FALSE) AS price_filter_passed,
            vp.price_filter_reason,
            COALESCE(vp.mechanical_passed, FALSE) AS mechanical_passed,
            vp.mechanical_reason,
            vp.ai_passed,
            vp.ai_reason,
            vp.has_ai_validation
        FROM articulums a
        INNER JOIN catalog_listings cl ON cl.articulum_id = a.id
        LEFT JOIN validation_pivot vp ON vp.articulum_id = a.id AND vp.avito_item_id = cl.avito_item_id
        WHERE a.articulum = ANY($1)
        ORDER BY a.articulum, cl.avito_item_id
        """
        rows = await conn.fetch(query, filter_articulums)
    else:
        # Без фильтра - все артикулы
        query = """
        WITH validation_pivot AS (
            SELECT
                articulum_id,
                avito_item_id,
                BOOL_OR(validation_type = 'price_filter' AND passed) AS price_filter_passed,
                MAX(CASE WHEN validation_type = 'price_filter' AND NOT passed THEN rejection_reason END) AS price_filter_reason,
                BOOL_OR(validation_type = 'mechanical' AND passed) AS mechanical_passed,
                MAX(CASE WHEN validation_type = 'mechanical' AND NOT passed THEN rejection_reason END) AS mechanical_reason,
                BOOL_OR(validation_type = 'ai' AND passed) AS ai_passed,
                MAX(CASE WHEN validation_type = 'ai' AND NOT passed THEN rejection_reason END) AS ai_reason,
                BOOL_OR(validation_type = 'ai') AS has_ai_validation
            FROM validation_results
            GROUP BY articulum_id, avito_item_id
        )
        SELECT
            a.id AS articulum_id,
            a.articulum,
            cl.avito_item_id,
            cl.title,
            cl.price,
            cl.seller_name,
            COALESCE(vp.price_filter_passed, FALSE) AS price_filter_passed,
            vp.price_filter_reason,
            COALESCE(vp.mechanical_passed, FALSE) AS mechanical_passed,
            vp.mechanical_reason,
            vp.ai_passed,
            vp.ai_reason,
            vp.has_ai_validation
        FROM articulums a
        INNER JOIN catalog_listings cl ON cl.articulum_id = a.id
        LEFT JOIN validation_pivot vp ON vp.articulum_id = a.id AND vp.avito_item_id = cl.avito_item_id
        ORDER BY a.articulum, cl.avito_item_id
        """
        rows = await conn.fetch(query)

    if not rows:
        print("⚠ Нет данных для обработки")
        return 0

    # Обработать и вставить данные
    insert_data = []

    for row in rows:
        # Определить final_passed и rejection_stage
        price_filter_passed = row['price_filter_passed']
        mechanical_passed = row['mechanical_passed']
        ai_passed = row['ai_passed']
        has_ai = row['has_ai_validation']

        # Логика определения final_passed
        if has_ai:
            # Если была AI валидация, все три этапа должны пройти
            final_passed = price_filter_passed and mechanical_passed and (ai_passed or False)
        else:
            # Если AI не было, достаточно первых двух
            final_passed = price_filter_passed and mechanical_passed

        # Определить rejection_stage
        rejection_stage = None
        if not final_passed:
            if not price_filter_passed:
                rejection_stage = 'price_filter'
            elif not mechanical_passed:
                rejection_stage = 'mechanical'
            elif has_ai and not ai_passed:
                rejection_stage = 'ai'

        # Определить финальную причину отклонения
        rejection_reason = None
        if rejection_stage == 'price_filter':
            rejection_reason = row['price_filter_reason']
        elif rejection_stage == 'mechanical':
            rejection_reason = row['mechanical_reason']
        elif rejection_stage == 'ai':
            rejection_reason = row['ai_reason']

        insert_data.append((
            rejection_reason,
            row['articulum_id'],
            row['articulum'],
            row['avito_item_id'],
            row['title'],
            row['price'],
            row['seller_name'],
            price_filter_passed,
            row['price_filter_reason'],
            mechanical_passed,
            row['mechanical_reason'],
            ai_passed,
            row['ai_reason'],
            final_passed,
            rejection_stage
        ))

    # Вставить данные батчами
    insert_query = """
    INSERT INTO analytics_articulum_report (
        rejection_reason,
        articulum_id, articulum, avito_item_id,
        title, price, seller_name,
        price_filter_passed, price_filter_reason,
        mechanical_passed, mechanical_reason,
        ai_passed, ai_reason,
        final_passed, rejection_stage
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
    """

    await conn.executemany(insert_query, insert_data)

    print(f"✓ Вставлено {len(insert_data)} записей")

    return len(insert_data)


async def get_statistics(conn) -> Dict:
    """Получить статистику по отчету"""

    # Общая статистика
    total_query = """
    SELECT
        COUNT(DISTINCT articulum_id) as total_articulums,
        COUNT(*) as total_listings,
        COUNT(*) FILTER (WHERE final_passed) as passed_total,
        COUNT(*) FILTER (WHERE NOT final_passed) as rejected_total
    FROM analytics_articulum_report
    """
    total_stats = await conn.fetchrow(total_query)

    # Статистика по этапам отклонения
    stages_query = """
    SELECT
        rejection_stage,
        COUNT(*) as count
    FROM analytics_articulum_report
    WHERE rejection_stage IS NOT NULL
    GROUP BY rejection_stage
    ORDER BY count DESC
    """
    stages_stats = await conn.fetch(stages_query)

    # ТОП причин отклонения
    reasons_query = """
    SELECT
        rejection_stage,
        CASE
            WHEN rejection_stage = 'price_filter' THEN price_filter_reason
            WHEN rejection_stage = 'mechanical' THEN mechanical_reason
            WHEN rejection_stage = 'ai' THEN ai_reason
        END as reason,
        COUNT(*) as count
    FROM analytics_articulum_report
    WHERE rejection_stage IS NOT NULL
    GROUP BY rejection_stage, reason
    ORDER BY count DESC
    LIMIT 10
    """
    reasons_stats = await conn.fetch(reasons_query)

    return {
        'total': total_stats,
        'stages': stages_stats,
        'reasons': reasons_stats
    }


def display_statistics(stats: Dict):
    """Вывести статистику"""

    print("\n" + "=" * 80)
    print("АНАЛИТИЧЕСКИЙ ОТЧЕТ ПО АРТИКУЛАМ".center(80))
    print("=" * 80 + "\n")

    total = stats['total']

    print("📊 ОБЩАЯ СТАТИСТИКА")
    print("-" * 80)
    print(f"  Всего артикулов:       {total['total_articulums']}")
    print(f"  Всего объявлений:      {total['total_listings']}")
    print(f"  Прошло валидацию:      {total['passed_total']} ({total['passed_total']/total['total_listings']*100:.1f}%)")
    print(f"  Отклонено:             {total['rejected_total']} ({total['rejected_total']/total['total_listings']*100:.1f}%)")

    print("\n📉 ОТКЛОНЕНИЯ ПО ЭТАПАМ")
    print("-" * 80)

    if TABULATE_AVAILABLE:
        stages_data = [[row['rejection_stage'], row['count']] for row in stats['stages']]
        print(tabulate(stages_data, headers=['Этап', 'Отклонено'], tablefmt='simple'))
    else:
        for row in stats['stages']:
            print(f"  {row['rejection_stage']:20} {row['count']}")

    print("\n🔍 ТОП-10 ПРИЧИН ОТКЛОНЕНИЯ")
    print("-" * 80)

    if TABULATE_AVAILABLE:
        reasons_data = [[row['rejection_stage'], row['reason'][:50] if row['reason'] else 'NULL', row['count']] for row in stats['reasons']]
        print(tabulate(reasons_data, headers=['Этап', 'Причина', 'Количество'], tablefmt='simple'))
    else:
        for row in stats['reasons']:
            reason = row['reason'][:50] if row['reason'] else 'NULL'
            print(f"  [{row['rejection_stage']}] {reason} - {row['count']}")

    print("\n" + "=" * 80 + "\n")

    print("✓ Данные сохранены в таблицу: analytics_articulum_report")
    print("  Запрос для просмотра: SELECT * FROM analytics_articulum_report ORDER BY articulum, avito_item_id;")


async def main():
    """Главная функция"""

    print("Генерация аналитического отчета по артикулам...")
    print()

    # Загрузить артикулы из файла
    filter_articulums = load_articulums_from_file()

    if filter_articulums:
        print(f"📋 Фильтр артикулов: загружено {len(filter_articulums)} артикулов из scripts/data/report_articulums.txt")
        print(f"   Отчет будет сгенерирован только по указанным артикулам")
    else:
        print("📋 Фильтр артикулов не задан")
        print(f"   Отчет будет сгенерирован по ВСЕМ артикулам из БД")

    print()

    conn = await connect_db()

    try:
        # Пересоздать таблицу
        await recreate_report_table(conn)

        # Собрать и вставить данные
        records_count = await collect_and_insert_data(conn, filter_articulums if filter_articulums else None)

        if records_count > 0:
            # Получить и вывести статистику
            stats = await get_statistics(conn)
            display_statistics(stats)

    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
