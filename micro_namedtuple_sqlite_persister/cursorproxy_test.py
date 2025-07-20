from __future__ import annotations

import sqlite3
from typing import Any, NamedTuple, assert_type

from .cursorproxy import TypedCursorProxy
from .persister import Engine


def test_engine_query_fetchone(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(ModelX)
    sql = "SELECT 1 as id, 'Alice' as name, 30 as age;"

    cur = engine.query(ModelX, sql)
    row = cur.fetchone()

    assert row == ModelX(1, "Alice", 30)
    assert type(row) is ModelX

    assert_type(cur, TypedCursorProxy[ModelX])
    assert_type(row, ModelX)


def test_engine_query_fetchall(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(ModelX)
    sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"

    cur = engine.query(ModelX, sql)
    rows = cur.fetchall()

    assert rows == [ModelX(1, "Alice", 30), ModelX(2, "Bob", 40)]
    assert type(rows[0]) is ModelX

    assert_type(cur, TypedCursorProxy[ModelX])
    assert_type(rows, list[ModelX])


def test_engine_query_fetchmany(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(ModelX)
    sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"

    cur = engine.query(ModelX, sql)
    rows = cur.fetchmany(1)

    assert rows == [ModelX(1, "Alice", 30)]
    assert type(rows[0]) is ModelX

    assert_type(cur, TypedCursorProxy[ModelX])
    assert_type(rows, list[ModelX])


def test_engine_query_row_factory_persists_after_usage(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(ModelX)
    sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"

    cur = engine.query(ModelX, sql)

    rows = cur.fetchmany(1)
    assert rows == [ModelX(1, "Alice", 30)]
    assert type(rows[0]) is ModelX

    rows = cur.fetchmany(1)
    assert rows == [ModelX(2, "Bob", 40)]
    assert type(rows[0]) is ModelX


def test_engine_query_cursorproxy_getattr_maintains_typehints(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(ModelX)
    sql = "SELECT 1 as id, 'Alice' as name, 30 as age;"

    cur = engine.query(ModelX, sql)
    assert_type(cur.fetchone(), ModelX | None)
    assert_type(cur.fetchall(), list[ModelX])
    assert_type(cur.fetchmany(1), list[ModelX])
    assert_type(cur.rowcount, int)
    assert_type(cur.connection, sqlite3.Connection)


def test_that_row_factory_doesnt_leak_to_other_cursors(engine: Engine) -> None:
    class ModelX(NamedTuple):
        id: int | None
        name: str

    sql = "SELECT 1 as id, 'Alice' as name"

    # Engine.query gives back Model typed rows
    cur = engine.query(ModelX, sql)
    row = cur.fetchone()
    assert_type(row, ModelX | None)
    assert isinstance(row, ModelX)

    # Engine.connection.cursor still gives back raw rows
    cur = engine.connection.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    assert_type(row, Any)
    assert not isinstance(row, ModelX)
