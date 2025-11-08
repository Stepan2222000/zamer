"""Тестирование функциональности Этапа 2: State Machine и управление прокси"""

import asyncio
import sys
from pathlib import Path

# Добавляем путь к модулям container
sys.path.insert(0, str(Path(__file__).parent.parent / 'container'))

from database import connect_db
from state_machine import (
    transition_to_catalog_parsing,
    transition_to_catalog_parsed,
    transition_to_validating,
    transition_to_validated,
    transition_to_object_parsing,
    reject_articulum,
    get_articulum_state,
    get_articulums_by_state
)
from proxy_manager import (
    acquire_proxy,
    block_proxy,
    release_proxy,
    get_proxy_stats
)
from config import ArticulumState


async def test_state_machine():
    """Тест переходов State Machine"""
    print("\n=== ТЕСТ STATE MACHINE ===\n")

    conn = await connect_db()

    try:
        # Создаем тестовый артикул
        print("1. Создание тестового артикула...")
        articulum_id = await conn.fetchval("""
            INSERT INTO articulums (articulum, state)
            VALUES ('TEST_ARTICULUM_' || gen_random_uuid()::text, 'NEW')
            RETURNING id
        """)
        print(f"   Создан артикул ID={articulum_id}, state=NEW")

        # Тест перехода NEW → CATALOG_PARSING
        print("\n2. Переход NEW → CATALOG_PARSING...")
        success = await transition_to_catalog_parsing(conn, articulum_id)
        state = await get_articulum_state(conn, articulum_id)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.CATALOG_PARSING, f"State должен быть CATALOG_PARSING, а не {state}"
        print(f"   ✓ Успех: state={state}")

        # Тест повторного перехода (должен вернуть False)
        print("\n3. Повторный переход NEW → CATALOG_PARSING (должен провалиться)...")
        success = await transition_to_catalog_parsing(conn, articulum_id)
        assert success == False, "Повторный переход должен провалиться"
        print("   ✓ Успех: повторный переход заблокирован")

        # Тест перехода CATALOG_PARSING → CATALOG_PARSED
        print("\n4. Переход CATALOG_PARSING → CATALOG_PARSED...")
        success = await transition_to_catalog_parsed(conn, articulum_id)
        state = await get_articulum_state(conn, articulum_id)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.CATALOG_PARSED, f"State должен быть CATALOG_PARSED"
        print(f"   ✓ Успех: state={state}")

        # Тест перехода CATALOG_PARSED → VALIDATING
        print("\n5. Переход CATALOG_PARSED → VALIDATING...")
        success = await transition_to_validating(conn, articulum_id)
        state = await get_articulum_state(conn, articulum_id)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.VALIDATING, f"State должен быть VALIDATING"
        print(f"   ✓ Успех: state={state}")

        # Тест перехода VALIDATING → VALIDATED
        print("\n6. Переход VALIDATING → VALIDATED...")
        success = await transition_to_validated(conn, articulum_id)
        state = await get_articulum_state(conn, articulum_id)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.VALIDATED, f"State должен быть VALIDATED"
        print(f"   ✓ Успех: state={state}")

        # Тест перехода VALIDATED → OBJECT_PARSING
        print("\n7. Переход VALIDATED → OBJECT_PARSING...")
        success = await transition_to_object_parsing(conn, articulum_id)
        state = await get_articulum_state(conn, articulum_id)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.OBJECT_PARSING, f"State должен быть OBJECT_PARSING"
        print(f"   ✓ Успех: state={state}")

        # Тест отклонения артикула
        print("\n8. Создание второго артикула для теста отклонения...")
        articulum_id2 = await conn.fetchval("""
            INSERT INTO articulums (articulum, state)
            VALUES ('TEST_REJECTED_' || gen_random_uuid()::text, 'VALIDATING')
            RETURNING id
        """)

        print("9. Переход VALIDATING → REJECTED_BY_MIN_COUNT...")
        success = await reject_articulum(conn, articulum_id2, "Тестовое отклонение")
        state = await get_articulum_state(conn, articulum_id2)
        assert success == True, "Переход должен быть успешным"
        assert state == ArticulumState.REJECTED_BY_MIN_COUNT, f"State должен быть REJECTED_BY_MIN_COUNT"
        print(f"   ✓ Успех: state={state}")

        # Очистка тестовых данных
        print("\n10. Очистка тестовых данных...")
        await conn.execute("DELETE FROM articulums WHERE id IN ($1, $2)", articulum_id, articulum_id2)
        print("   ✓ Тестовые артикулы удалены")

        print("\n✅ ВСЕ ТЕСТЫ STATE MACHINE ПРОЙДЕНЫ!")

    finally:
        await conn.close()


async def test_proxy_manager():
    """Тест управления прокси"""
    print("\n=== ТЕСТ УПРАВЛЕНИЯ ПРОКСИ ===\n")

    conn = await connect_db()

    try:
        # Получение статистики
        print("1. Получение статистики прокси...")
        stats = await get_proxy_stats(conn)
        print(f"   Всего: {stats['total']}")
        print(f"   Заблокировано: {stats['blocked']}")
        print(f"   Используется: {stats['in_use']}")
        print(f"   Доступно: {stats['available']}")

        if stats['available'] == 0:
            print("\n⚠️  Нет доступных прокси для тестирования!")
            return

        # Атомарная выдача прокси
        print("\n2. Атомарная выдача прокси для worker_id=999...")
        proxy1 = await acquire_proxy(conn, worker_id=999)
        assert proxy1 is not None, "Должен быть выдан прокси"
        print(f"   ✓ Прокси выдан: {proxy1['host']}:{proxy1['port']}")

        # Проверка, что прокси помечен как используемый
        print("\n3. Проверка состояния прокси...")
        is_in_use = await conn.fetchval("SELECT is_in_use FROM proxies WHERE id = $1", proxy1['id'])
        worker_id = await conn.fetchval("SELECT worker_id FROM proxies WHERE id = $1", proxy1['id'])
        assert is_in_use == True, "Прокси должен быть помечен как используемый"
        assert worker_id == 999, "Прокси должен быть привязан к worker_id=999"
        print(f"   ✓ Прокси корректно помечен (is_in_use=True, worker_id=999)")

        # Попытка повторно взять тот же прокси (не должен выдаться)
        print("\n4. Попытка повторно взять прокси для worker_id=1000...")
        proxy2 = await acquire_proxy(conn, worker_id=1000)
        if proxy2:
            assert proxy2['id'] != proxy1['id'], "Должен быть выдан другой прокси"
            print(f"   ✓ Выдан другой прокси: {proxy2['host']}:{proxy2['port']}")
        else:
            print(f"   ✓ Других доступных прокси нет")

        # Возврат прокси в пул
        print("\n5. Возврат прокси в пул...")
        await release_proxy(conn, proxy1['id'])
        is_in_use = await conn.fetchval("SELECT is_in_use FROM proxies WHERE id = $1", proxy1['id'])
        assert is_in_use == False, "Прокси должен быть освобожден"
        print(f"   ✓ Прокси освобожден (is_in_use=False)")

        # Блокировка прокси
        print("\n6. Постоянная блокировка прокси...")
        await block_proxy(conn, proxy1['id'], "Тестовая блокировка")
        is_blocked = await conn.fetchval("SELECT is_blocked FROM proxies WHERE id = $1", proxy1['id'])
        is_in_use = await conn.fetchval("SELECT is_in_use FROM proxies WHERE id = $1", proxy1['id'])
        assert is_blocked == True, "Прокси должен быть заблокирован"
        assert is_in_use == False, "Прокси не должен быть в использовании"
        print(f"   ✓ Прокси заблокирован навсегда")

        # Попытка взять заблокированный прокси (не должен выдаться)
        print("\n7. Попытка взять заблокированный прокси...")
        proxy3 = await acquire_proxy(conn, worker_id=999)
        if proxy3:
            assert proxy3['id'] != proxy1['id'], "Заблокированный прокси не должен выдаваться"
            print(f"   ✓ Заблокированный прокси не выдан, выдан другой")
            # Освобождаем для чистоты
            await release_proxy(conn, proxy3['id'])
        else:
            print(f"   ✓ Заблокированный прокси не выдан, других прокси нет")

        # Разблокировка для следующих тестов
        print("\n8. Разблокировка прокси (для возврата в начальное состояние)...")
        await conn.execute("UPDATE proxies SET is_blocked = FALSE WHERE id = $1", proxy1['id'])
        print(f"   ✓ Прокси разблокирован")

        # Освобождение всех тестовых прокси
        if proxy2:
            await release_proxy(conn, proxy2['id'])

        print("\n✅ ВСЕ ТЕСТЫ УПРАВЛЕНИЯ ПРОКСИ ПРОЙДЕНЫ!")

    finally:
        await conn.close()


async def test_concurrent_proxy_acquisition():
    """Тест конкурентной выдачи прокси (атомарность)"""
    print("\n=== ТЕСТ АТОМАРНОСТИ ВЫДАЧИ ПРОКСИ ===\n")

    conn = await connect_db()

    try:
        stats = await get_proxy_stats(conn)

        if stats['available'] < 3:
            print("⚠️  Недостаточно доступных прокси для теста (нужно минимум 3)")
            return

        print("1. Запуск 5 конкурентных воркеров для выдачи прокси...")

        async def worker_acquire(worker_id: int):
            worker_conn = await connect_db()
            try:
                proxy = await acquire_proxy(worker_conn, worker_id)
                if proxy:
                    print(f"   Worker {worker_id}: получил прокси {proxy['id']}")
                    return proxy['id']
                else:
                    print(f"   Worker {worker_id}: прокси не доступны")
                    return None
            finally:
                await worker_conn.close()

        # Запуск конкурентных воркеров
        results = await asyncio.gather(*[worker_acquire(i) for i in range(1, 6)])

        acquired_proxy_ids = [r for r in results if r is not None]

        print(f"\n2. Всего выдано прокси: {len(acquired_proxy_ids)}")

        # Проверка уникальности (нет дублирования)
        unique_ids = set(acquired_proxy_ids)
        assert len(unique_ids) == len(acquired_proxy_ids), "Прокси не должны дублироваться!"
        print(f"   ✓ Все выданные прокси уникальны (нет дублирования)")

        # Освобождение прокси
        print("\n3. Освобождение всех прокси...")
        for proxy_id in acquired_proxy_ids:
            await release_proxy(conn, proxy_id)
        print(f"   ✓ {len(acquired_proxy_ids)} прокси освобождены")

        print("\n✅ ТЕСТ АТОМАРНОСТИ ПРОЙДЕН!")

    finally:
        await conn.close()


async def main():
    """Запуск всех тестов"""
    print("=" * 70)
    print("ТЕСТИРОВАНИЕ ЭТАПА 2: STATE MACHINE И УПРАВЛЕНИЕ ПРОКСИ")
    print("=" * 70)

    try:
        await test_state_machine()
        await test_proxy_manager()
        await test_concurrent_proxy_acquisition()

        print("\n" + "=" * 70)
        print("✅ ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ!")
        print("=" * 70)

    except AssertionError as e:
        print(f"\n❌ ОШИБКА ТЕСТА: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
