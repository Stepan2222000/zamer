"""Управление State Machine артикулов"""

import asyncpg
from config import ArticulumState, ALL_STATES, FINAL_STATES


class StateTransitionError(Exception):
    """
    Исключение при критической ошибке перехода состояния.

    Вызывается когда переход состояния не удался в контексте,
    где это критическая ошибка (например, внутри транзакции после сохранения данных).
    """
    pass


async def transition_state(
    conn: asyncpg.Connection,
    articulum_id: int,
    from_state: str,
    to_state: str
) -> bool:
    """
    Атомарный переход артикула из одного состояния в другое

    Проверяет валидность состояний и запрещает переходы из финальных состояний.
    Использует UPDATE WHERE для атомарности (race condition protection).

    Возвращает True если переход выполнен, False если артикул уже в другом состоянии
    """
    if from_state not in ALL_STATES or to_state not in ALL_STATES:
        raise ValueError(f"Недопустимые состояния: {from_state} → {to_state}")

    if from_state in FINAL_STATES:
        raise ValueError(f"Переход из финального состояния {from_state} запрещен")

    result = await conn.execute("""
        UPDATE articulums
        SET state = $2,
            state_updated_at = NOW(),
            updated_at = NOW()
        WHERE id = $1 AND state = $3
    """, articulum_id, to_state, from_state)

    # Проверка и логирование
    success = result == 'UPDATE 1'
    if success:
        print(f"Артикул {articulum_id}: {from_state} → {to_state}")
    else:
        print(f"Переход {from_state}→{to_state} для артикула {articulum_id} не выполнен (артикул уже в другом состоянии)")

    return success


async def get_articulum_state(conn: asyncpg.Connection, articulum_id: int) -> str:
    """
    Получить текущее состояние артикула

    Возвращает строку с текущим state (например, 'NEW', 'CATALOG_PARSING')
    """
    return await conn.fetchval(
        "SELECT state FROM articulums WHERE id = $1",
        articulum_id
    )


async def get_articulums_by_state(
    conn: asyncpg.Connection,
    state: str,
    limit: int = None
) -> list:
    """
    Получить список артикулов в заданном состоянии

    Возвращает список записей артикулов, отсортированных по дате создания.
    Используется для выборки артикулов на обработку (например, NEW для старта парсинга).
    """
    if state not in ALL_STATES:
        raise ValueError(f"Недопустимое состояние: {state}")

    query = "SELECT * FROM articulums WHERE state = $1 ORDER BY created_at ASC"

    if limit:
        query += " LIMIT $2"
        return await conn.fetch(query, state, limit)
    else:
        return await conn.fetch(query, state)


async def transition_to_catalog_parsing(
    conn: asyncpg.Connection,
    articulum_id: int
) -> bool:
    """
    NEW → CATALOG_PARSING

    Переводит артикул в режим парсинга каталога.
    Вызывается при создании catalog_task для артикула.
    """
    return await transition_state(
        conn,
        articulum_id,
        ArticulumState.NEW,
        ArticulumState.CATALOG_PARSING
    )


async def transition_to_catalog_parsed(
    conn: asyncpg.Connection,
    articulum_id: int
) -> bool:
    """
    CATALOG_PARSING → CATALOG_PARSED

    Каталог полностью спарсен, все страницы обработаны.
    Артикул готов к валидации Validation Worker'ом.
    """
    return await transition_state(
        conn,
        articulum_id,
        ArticulumState.CATALOG_PARSING,
        ArticulumState.CATALOG_PARSED
    )


async def transition_to_validating(
    conn: asyncpg.Connection,
    articulum_id: int
) -> bool:
    """
    CATALOG_PARSED → VALIDATING (атомарно)

    Validation Worker атомарно захватывает артикул для валидации.
    Если возвращает False - артикул уже взят другим воркером.
    """
    return await transition_state(
        conn,
        articulum_id,
        ArticulumState.CATALOG_PARSED,
        ArticulumState.VALIDATING
    )


async def transition_to_validated(
    conn: asyncpg.Connection,
    articulum_id: int
) -> bool:
    """
    VALIDATING → VALIDATED

    Валидация успешна, достаточно валидных объявлений.
    После этого создаются object_tasks для парсинга объявлений.
    """
    return await transition_state(
        conn,
        articulum_id,
        ArticulumState.VALIDATING,
        ArticulumState.VALIDATED
    )


async def transition_to_object_parsing(
    conn: asyncpg.Connection,
    articulum_id: int
) -> bool:
    """
    VALIDATED → OBJECT_PARSING

    Начат парсинг объявлений артикула (финальное состояние).
    Переход происходит автоматически при взятии первой object_task.
    """
    return await transition_state(
        conn,
        articulum_id,
        ArticulumState.VALIDATED,
        ArticulumState.OBJECT_PARSING
    )


async def reject_articulum(
    conn: asyncpg.Connection,
    articulum_id: int,
    reason: str
) -> bool:
    """
    VALIDATING → REJECTED_BY_MIN_COUNT (финальное состояние)

    Артикул отклонен из-за недостаточного количества валидных объявлений.
    Причины: после ценовой фильтрации, стоп-слов или ИИ-валидации осталось < MIN_VALIDATED_ITEMS.
    """
    success = await transition_state(
        conn,
        articulum_id,
        ArticulumState.VALIDATING,
        ArticulumState.REJECTED_BY_MIN_COUNT
    )

    if success:
        print(f"Артикул {articulum_id} отклонен: {reason}")

    return success
