"""Сброс и перезагрузка БД (очистка + загрузка артикулов и прокси)"""

import asyncio
import sys
from pathlib import Path

# Добавляем container в путь для импорта
sys.path.insert(0, str(Path(__file__).parent.parent / 'container'))

import asyncpg
from config import DB_CONFIG


async def clear_tables(conn):
    """Очистка таблиц"""
    print("🗑️  Очистка таблиц...")

    # Очищаем catalog_tasks и object_tasks (могут быть внешние ключи)
    try:
        await conn.execute("DELETE FROM catalog_tasks")
        print("  ✓ catalog_tasks очищена")
    except Exception as e:
        print(f"  ⚠️  catalog_tasks: {e}")

    try:
        await conn.execute("DELETE FROM object_tasks")
        print("  ✓ object_tasks очищена")
    except Exception as e:
        print(f"  ⚠️  object_tasks: {e}")

    try:
        await conn.execute("DELETE FROM catalog_listings")
        print("  ✓ catalog_listings очищена")
    except Exception as e:
        print(f"  ⚠️  catalog_listings: {e}")

    # Очищаем основные таблицы
    await conn.execute("DELETE FROM articulums")
    print("  ✓ articulums очищена")

    await conn.execute("DELETE FROM proxies")
    print("  ✓ proxies очищена")

    print("✅ Все таблицы очищены!\n")


async def load_articulums(conn, file_path: Path):
    """Загрузка артикулов из файла"""
    print(f"📥 Загрузка артикулов из {file_path.name}...")

    if not file_path.exists():
        print(f"❌ Файл {file_path} не найден!")
        return 0

    # Читаем файл
    articulums = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            articulum = line.strip()
            if articulum and not articulum.startswith('#'):
                articulums.append(articulum)

    # Убираем дубликаты
    articulums = list(set(articulums))

    # Загружаем в БД
    inserted = 0
    for articulum in articulums:
        try:
            await conn.execute("""
                INSERT INTO articulums (articulum, state)
                VALUES ($1, 'NEW')
            """, articulum)
            inserted += 1
        except Exception as e:
            print(f"  ⚠️  Ошибка при вставке '{articulum}': {e}")

    print(f"✅ Загружено артикулов: {inserted}\n")
    return inserted


async def load_proxies(conn, file_path: Path):
    """Загрузка прокси из файла"""
    print(f"📥 Загрузка прокси из {file_path.name}...")

    if not file_path.exists():
        print(f"❌ Файл {file_path} не найден!")
        return 0

    # Читаем файл
    proxies = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Формат: host:port:username:password
            parts = line.split(':')
            if len(parts) >= 2:
                host = parts[0]
                port = parts[1]
                username = parts[2] if len(parts) > 2 else None
                password = parts[3] if len(parts) > 3 else None

                proxies.append({
                    'host': host,
                    'port': int(port),
                    'username': username,
                    'password': password
                })

    # Загружаем в БД
    inserted = 0
    for proxy in proxies:
        try:
            await conn.execute("""
                INSERT INTO proxies (host, port, username, password)
                VALUES ($1, $2, $3, $4)
            """, proxy['host'], proxy['port'], proxy['username'], proxy['password'])
            inserted += 1
        except Exception as e:
            print(f"  ⚠️  Ошибка при вставке {proxy['host']}:{proxy['port']}: {e}")

    print(f"✅ Загружено прокси: {inserted}\n")
    return inserted


async def main():
    """Главная функция"""
    print()
    print("=" * 60)
    print("🔄 СБРОС И ПЕРЕЗАГРУЗКА БД")
    print("=" * 60)
    print()

    # Пути к файлам данных
    script_dir = Path(__file__).parent
    articulums_file = script_dir / 'data' / 'articulums.txt'
    proxies_file = script_dir / 'data' / 'proxies.txt'

    # Подключение к БД
    print("🔌 Подключение к БД...")
    conn = await asyncpg.connect(**DB_CONFIG)
    print("✅ Подключено!\n")

    try:
        # 1. Очистка
        await clear_tables(conn)

        # 2. Загрузка артикулов
        articulums_count = await load_articulums(conn, articulums_file)

        # 3. Загрузка прокси
        proxies_count = await load_proxies(conn, proxies_file)

        # Итоги
        print("=" * 60)
        print("📊 ИТОГИ:")
        print("=" * 60)
        print(f"  Артикулов загружено: {articulums_count}")
        print(f"  Прокси загружено:    {proxies_count}")
        print("=" * 60)
        print()
        print("✅ БД успешно сброшена и загружена!")
        print()

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await conn.close()


if __name__ == '__main__':
    asyncio.run(main())
