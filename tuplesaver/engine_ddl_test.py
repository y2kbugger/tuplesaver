from __future__ import annotations

import datetime as dt
from dataclasses import fields
from typing import Any

import pytest

from .engine import Engine, TableSchemaMismatch
from .model import InvalidTableName, Row, TableRow


class TableInfo(Row):
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int


def test_ensure_table_created__adhoc_row_model__raises(engine: Engine) -> None:
    class T(Row):
        a: int

    with pytest.raises(AssertionError):
        engine.ensure_table_created(T)  # ty:ignore[invalid-argument-type]


def test_ensure_table_created__dataclass__raises(engine: Engine) -> None:
    from dataclasses import dataclass

    @dataclass
    class T:
        a: int

    with pytest.raises(AssertionError):
        engine.ensure_table_created(T)  # ty:ignore[invalid-argument-type]


def test_ensure_table_created__namedtuple__raises(engine: Engine) -> None:
    from typing import NamedTuple

    class T(NamedTuple):
        a: int

    with pytest.raises(AssertionError):
        engine.ensure_table_created(T)  # ty:ignore[invalid-argument-type]


def test_ensure_table_created(engine: Engine) -> None:
    class TblDates(TableRow):
        name: str
        score: float
        age: int
        data: bytes
        startdate: dt.date
        modified: dt.datetime
        serial: int | None

    engine.ensure_table_created(TblDates)

    # Table as a whole
    class SqliteSchema(Row):
        type: str
        name: str
        tbl_name: str
        rootpage: int
        sql: str

    tables = engine.query(SqliteSchema, "SELECT * FROM sqlite_schema WHERE type='table'").fetchall()
    assert len(tables) == 1
    table = tables[0]
    assert table.name == TblDates.__name__

    # Columns
    cols = engine.query(TableInfo, f"PRAGMA table_info({TblDates.__name__})").fetchall()

    assert len(cols) == len(fields(TblDates))

    assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
    assert cols[1] == TableInfo(1, "name", "TEXT", 1, None, 0)
    assert cols[2] == TableInfo(2, "score", "REAL", 1, None, 0)
    assert cols[3] == TableInfo(3, "age", "INTEGER", 1, None, 0)
    assert cols[4] == TableInfo(4, "data", "BLOB", 1, None, 0)
    assert cols[5] == TableInfo(5, "startdate", "datetime.date", 1, None, 0)
    assert cols[6] == TableInfo(6, "modified", "datetime.datetime", 1, None, 0)
    assert cols[7] == TableInfo(7, "serial", "INTEGER", 0, None, 0)  # optional field


def test_ensure_table_created_with_related_table(engine: Engine) -> None:
    class A(TableRow):
        pass

    class B(TableRow):
        team: A

    engine.ensure_table_created(A)
    engine.ensure_table_created(B)

    cols = engine.query(TableInfo, f"PRAGMA table_info({B.__name__})").fetchall()

    assert len(cols) == len(fields(B))

    assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
    assert cols[1] == TableInfo(1, "team", "A_ID", 1, None, 0)


def test_ensure_table_created_with_optional_related_table(engine: Engine) -> None:
    class A(TableRow):
        pass

    class B(TableRow):
        team: A | None  # Optional relationship

    engine.ensure_table_created(A)
    engine.ensure_table_created(B)

    cols = engine.query(TableInfo, f"PRAGMA table_info({B.__name__})").fetchall()

    assert len(cols) == len(fields(B))

    assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
    assert cols[1] == TableInfo(1, "team", "A_ID", 0, None, 0)


def test_ensure_table_created_with_table_already_created_correct_is_silent(engine: Engine) -> None:
    class TblAlreadyCreated(TableRow):
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)
    engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created_with_table_already_created_incorrect_raises(engine: Engine) -> None:
    class TblAlreadyCreated(TableRow):  # type: ignore shadowing is part of the test
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)

    class TblAlreadyCreated(TableRow):
        name: str
        age: int
        data: bytes

    with pytest.raises(TableSchemaMismatch):
        engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created_catches_mismatched_from_out_of_band_alters(engine: Engine) -> None:
    class TblAlreadyCreated(TableRow):
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)
    alter = f"ALTER TABLE {TblAlreadyCreated.__name__} ADD COLUMN out_of_band_edit TEXT"
    engine.connection.execute(alter)

    with pytest.raises(TableSchemaMismatch):
        engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created__nontable_model_raises(engine: Engine) -> None:
    class NonTable_Model(TableRow):
        name: str

    with pytest.raises(InvalidTableName):
        engine.ensure_table_created(NonTable_Model)
