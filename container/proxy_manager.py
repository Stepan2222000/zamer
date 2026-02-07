"""Управление прокси"""

import asyncpg
import asyncio
from config import PROXY_WAIT_TIMEOUT


async def acquire_proxy(conn: asyncpg.Connection, worker_id: str) -> dict:
    """
    Атомарно получить свободный прокси для воркера

    Использует SELECT FOR UPDATE SKIP LOCKED для предотвращения блокировок
    Возвращает None если нет доступных прокси
    """
    async with conn.transaction():
        proxy = await conn.fetchrow("""
            SELECT * FROM proxies
            WHERE is_blocked = FALSE
              AND is_in_use = FALSE
            ORDER BY id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)

        if not proxy:
            return None

        await conn.execute("""
            UPDATE proxies
            SET is_in_use = TRUE,
                worker_id = $1,
                updated_at = NOW()
            WHERE id = $2
        """, worker_id, proxy['id'])

        return dict(proxy)


async def acquire_proxy_with_wait(
    conn: asyncpg.Connection,
    worker_id: str,
    max_attempts: int = None
) -> dict:
    """
    Получить свободный прокси с ожиданием

    Если нет доступных прокси, ждет и повторяет попытку
    max_attempts: максимальное количество попыток (None = бесконечно)
    """
    attempts = 0

    while max_attempts is None or attempts < max_attempts:
        proxy = await acquire_proxy(conn, worker_id)

        if proxy:
            return proxy

        attempts += 1
        print(f"Worker {worker_id}: нет свободных прокси, ожидание {PROXY_WAIT_TIMEOUT}с... (попытка {attempts})")
        await asyncio.sleep(PROXY_WAIT_TIMEOUT)

    raise RuntimeError(f"Worker {worker_id}: не удалось получить прокси после {max_attempts} попыток")


async def block_proxy(conn: asyncpg.Connection, proxy_id: int, reason: str = None) -> None:
    """
    Постоянная блокировка прокси

    Механизма разблокировки нет
    Вызывается при детекции проблем: 403, AUTH
    """
    if proxy_id is None:
        return
    await conn.execute("""
        UPDATE proxies
        SET is_blocked = TRUE,
            is_in_use = FALSE,
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = $1
    """, proxy_id)

    reason_msg = f" ({reason})" if reason else ""
    print(f"Прокси {proxy_id} заблокирован навсегда{reason_msg}")


async def release_proxy(conn: asyncpg.Connection, proxy_id: int) -> None:
    """
    Вернуть прокси в пул (без блокировки)

    Используется при нерешенной капче
    Не освобождает заблокированные прокси
    """
    if proxy_id is None:
        return
    await conn.execute("""
        UPDATE proxies
        SET is_in_use = FALSE,
            worker_id = NULL,
            updated_at = NOW()
        WHERE id = $1 AND is_blocked = FALSE
    """, proxy_id)


async def increment_proxy_error(conn: asyncpg.Connection, proxy_id: int, error_description: str) -> None:
    """
    Увеличить счетчик последовательных ошибок прокси

    После 3 последовательных ошибок прокси блокируется навсегда
    Если ошибок < 3, прокси возвращается в пул
    """
    if proxy_id is None:
        return
    # Получаем текущий счетчик и увеличиваем его
    current_errors = await conn.fetchval("""
        SELECT consecutive_errors FROM proxies WHERE id = $1
    """, proxy_id)

    new_errors = (current_errors or 0) + 1

    # Если достигли лимита - блокируем навсегда
    if new_errors >= 3:
        await conn.execute("""
            UPDATE proxies
            SET is_blocked = TRUE,
                is_in_use = FALSE,
                worker_id = NULL,
                consecutive_errors = $2,
                last_error_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
        """, proxy_id, new_errors)
        print(f"Прокси {proxy_id} заблокирован после {new_errors} последовательных ошибок ({error_description})")
    else:
        # Иначе увеличиваем счетчик и освобождаем прокси
        await conn.execute("""
            UPDATE proxies
            SET is_in_use = FALSE,
                worker_id = NULL,
                consecutive_errors = $2,
                last_error_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
        """, proxy_id, new_errors)
        print(f"Прокси {proxy_id}: transient error #{new_errors}/3 ({error_description})")


async def reset_proxy_error_counter(conn: asyncpg.Connection, proxy_id: int) -> None:
    """
    Сбросить счетчик последовательных ошибок

    Вызывается после успешного выполнения задачи
    """
    if proxy_id is None:
        return
    await conn.execute("""
        UPDATE proxies
        SET consecutive_errors = 0,
            updated_at = NOW()
        WHERE id = $1
    """, proxy_id)


async def get_proxy_stats(conn: asyncpg.Connection) -> dict:
    """Получить статистику по прокси"""
    total = await conn.fetchval("SELECT COUNT(*) FROM proxies")
    blocked = await conn.fetchval("SELECT COUNT(*) FROM proxies WHERE is_blocked = TRUE")
    in_use = await conn.fetchval("SELECT COUNT(*) FROM proxies WHERE is_in_use = TRUE")
    available = await conn.fetchval(
        "SELECT COUNT(*) FROM proxies WHERE is_blocked = FALSE AND is_in_use = FALSE"
    )

    return {
        'total': total,
        'blocked': blocked,
        'in_use': in_use,
        'available': available
    }


async def get_worker_proxy(conn: asyncpg.Connection, worker_id: str) -> dict:
    """Получить прокси, используемый воркером"""
    return await conn.fetchrow("""
        SELECT * FROM proxies
        WHERE worker_id = $1 AND is_in_use = TRUE
    """, worker_id)
