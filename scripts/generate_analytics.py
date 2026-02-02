"""Скрипт для генерации аналитики просмотров объявлений"""

import asyncio
import argparse
import sys
from datetime import datetime

import asyncpg

# ============================================
# Конфигурация подключения к БД
# ============================================
DB_CONFIG = {
    'host': '81.30.105.134',
    'port': 5432,
    'database': 'zamer_sys',
    'user': 'admin',
    'password': 'Password123',
}


async def connect_db() -> asyncpg.Connection:
    """Создать подключение к БД"""
    return await asyncpg.connect(**DB_CONFIG)


async def extract_and_calculate_analytics(conn):
    """
    Извлечь данные из object_data и рассчитать аналитику.
    Возвращает список словарей с данными для вставки в analytics_views.
    """
    # SQL запрос для извлечения первого и последнего замера для каждого объявления
    query = """
    WITH measurements AS (
        SELECT
            avito_item_id,
            MIN(parsed_at) as first_parsed,
            MAX(parsed_at) as last_parsed,
            COUNT(*) as measurement_count
        FROM object_data
        GROUP BY avito_item_id
        HAVING COUNT(*) >= 2
    ),
    articulum_aggregation AS (
        SELECT
            od.avito_item_id,
            STRING_AGG(DISTINCT a.articulum, ', ') as articulums
        FROM object_data od
        JOIN articulums a ON a.id = od.articulum_id
        GROUP BY od.avito_item_id
    )
    SELECT
        m.avito_item_id,
        aa.articulums,
        od_first.title,
        od_first.description,
        od_first.characteristics,
        od_first.price,
        od_first.views_total as first_views,
        od_last.views_total as last_views,
        m.first_parsed as first_parsed_at,
        m.last_parsed as last_parsed_at
    FROM measurements m
    LEFT JOIN articulum_aggregation aa ON aa.avito_item_id = m.avito_item_id
    JOIN object_data od_first ON od_first.avito_item_id = m.avito_item_id
        AND od_first.parsed_at = m.first_parsed
    JOIN object_data od_last ON od_last.avito_item_id = m.avito_item_id
        AND od_last.parsed_at = m.last_parsed
    """

    rows = await conn.fetch(query)

    analytics_data = []
    skipped_count = 0
    skipped_reasons = []

    for row in rows:
        # Извлечение данных
        avito_item_id = row['avito_item_id']
        articulums = row['articulums']
        title = row['title']
        description = row['description']
        characteristics = row['characteristics']
        price = row['price']
        first_views = row['first_views']
        last_views = row['last_views']
        first_parsed_at = row['first_parsed_at']
        last_parsed_at = row['last_parsed_at']

        # Проверка на NULL в критичных полях
        if first_views is None or last_views is None:
            skipped_count += 1
            skipped_reasons.append(f"{avito_item_id}: views_total is NULL")
            continue

        # Расчет метрик
        views_diff = last_views - first_views
        time_diff_seconds = (last_parsed_at - first_parsed_at).total_seconds()
        time_diff_hours = time_diff_seconds / 3600.0

        # Расчет efficiency_coefficient
        if time_diff_hours > 0:
            efficiency_coefficient = views_diff / time_diff_hours
        else:
            # Если время между замерами 0, то коэффициент NULL
            efficiency_coefficient = None
            skipped_reasons.append(f"{avito_item_id}: time_diff is 0 (same timestamp)")

        # Логирование отрицательных views_diff (баг Авито?)
        if views_diff < 0:
            skipped_reasons.append(f"{avito_item_id}: negative views_diff ({views_diff})")

        analytics_data.append({
            'avito_item_id': avito_item_id,
            'articulums': articulums,
            'title': title,
            'description': description,
            'characteristics': characteristics,
            'price': price,
            'first_views': first_views,
            'last_views': last_views,
            'views_diff': views_diff,
            'time_diff': time_diff_hours,
            'efficiency_coefficient': efficiency_coefficient,
            'first_parsed_at': first_parsed_at,
            'last_parsed_at': last_parsed_at
        })

    return analytics_data, skipped_count, skipped_reasons


async def save_analytics(conn, analytics_data: list[dict]) -> int:
    """Сохранить аналитику в таблицу analytics_views (полный пересчет)"""
    # Очистка таблицы перед вставкой (полный пересчет)
    await conn.execute('TRUNCATE TABLE analytics_views')

    if not analytics_data:
        return 0

    # Вставка данных
    insert_query = """
        INSERT INTO analytics_views (
            avito_item_id, articulums, title, description, characteristics,
            price, first_views, last_views, views_diff, time_diff,
            efficiency_coefficient, first_parsed_at, last_parsed_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    """

    # Подготовка данных для вставки
    values = [
        (
            item['avito_item_id'],
            item['articulums'],
            item['title'],
            item['description'],
            item['characteristics'],
            item['price'],
            item['first_views'],
            item['last_views'],
            item['views_diff'],
            item['time_diff'],
            item['efficiency_coefficient'],
            item['first_parsed_at'],
            item['last_parsed_at']
        )
        for item in analytics_data
    ]

    # Батчевая вставка
    await conn.executemany(insert_query, values)

    return len(values)


async def get_statistics(conn):
    """Получить статистику из analytics_views"""
    stats_query = """
        SELECT
            COUNT(*) as total,
            MIN(efficiency_coefficient) as min_efficiency,
            MAX(efficiency_coefficient) as max_efficiency,
            AVG(efficiency_coefficient) as avg_efficiency,
            COUNT(CASE WHEN efficiency_coefficient IS NULL THEN 1 END) as null_efficiency_count
        FROM analytics_views
    """

    return await conn.fetchrow(stats_query)


async def get_total_items_with_multiple_measurements(conn):
    """Получить количество объявлений с двумя и более замерами"""
    query = """
        SELECT COUNT(DISTINCT avito_item_id) as count
        FROM (
            SELECT avito_item_id
            FROM object_data
            GROUP BY avito_item_id
            HAVING COUNT(*) >= 2
        ) t
    """

    row = await conn.fetchrow(query)
    return row['count']


async def get_total_items_with_single_measurement(conn):
    """Получить количество объявлений с одним замером"""
    query = """
        SELECT COUNT(DISTINCT avito_item_id) as count
        FROM (
            SELECT avito_item_id
            FROM object_data
            GROUP BY avito_item_id
            HAVING COUNT(*) = 1
        ) t
    """

    row = await conn.fetchrow(query)
    return row['count']


async def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(description='Генерация аналитики просмотров объявлений')
    parser.add_argument('--verbose', '-v', action='store_true', help='Вывод подробной информации о пропущенных записях')
    args = parser.parse_args()

    print("=" * 60)
    print("ГЕНЕРАЦИЯ АНАЛИТИКИ ПРОСМОТРОВ")
    print("=" * 60)
    print()

    # Подключение к БД
    print("Подключение к БД...")
    conn = await connect_db()

    try:
        # Получение общей статистики
        print("Сбор статистики из object_data...")
        total_with_multiple = await get_total_items_with_multiple_measurements(conn)
        total_with_single = await get_total_items_with_single_measurement(conn)

        print(f"  Объявлений с 2+ замерами:  {total_with_multiple}")
        print(f"  Объявлений с 1 замером:    {total_with_single} (пропущено)")
        print()

        if total_with_multiple == 0:
            print("Нет объявлений с двумя и более замерами для аналитики.")
            print("Запустите повторный парсинг объявлений для сбора данных.")
            sys.exit(0)

        # Извлечение и расчет аналитики
        print("Извлечение данных и расчет метрик...")
        analytics_data, skipped_count, skipped_reasons = await extract_and_calculate_analytics(conn)

        print(f"  Обработано записей:        {len(analytics_data)}")
        if skipped_count > 0:
            print(f"  Пропущено (ошибки):        {skipped_count}")
        print()

        # Вывод причин пропуска (если --verbose)
        if args.verbose and skipped_reasons:
            print("Причины пропуска записей:")
            for reason in skipped_reasons[:20]:  # Показываем максимум 20
                print(f"  - {reason}")
            if len(skipped_reasons) > 20:
                print(f"  ... и еще {len(skipped_reasons) - 20} записей")
            print()

        # Сохранение аналитики
        print("Сохранение в analytics_views...")
        inserted_count = await save_analytics(conn, analytics_data)
        print(f"  Вставлено записей:         {inserted_count}")
        print()

        # Получение статистики по коэффициентам
        print("Статистика эффективности:")
        stats = await get_statistics(conn)

        if stats['total'] > 0:
            print(f"  Всего записей:             {stats['total']}")

            if stats['min_efficiency'] is not None:
                print(f"  Min коэффициент:           {stats['min_efficiency']:.2f} просмотров/час")
            if stats['max_efficiency'] is not None:
                print(f"  Max коэффициент:           {stats['max_efficiency']:.2f} просмотров/час")
            if stats['avg_efficiency'] is not None:
                print(f"  Avg коэффициент:           {stats['avg_efficiency']:.2f} просмотров/час")

            if stats['null_efficiency_count'] > 0:
                print(f"  С NULL коэффициентом:      {stats['null_efficiency_count']}")

        print()
        print("=" * 60)
        print("АНАЛИТИКА УСПЕШНО СГЕНЕРИРОВАНА")
        print("=" * 60)

    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
