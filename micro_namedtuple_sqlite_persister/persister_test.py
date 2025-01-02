import datetime as dt
from typing import NamedTuple

import pytest

from .persister import Engine


@pytest.fixture
def engine() -> Engine:
    return Engine(":memory:")


def test_get_connection(engine: Engine) -> None:
    from sqlite3 import Connection

    assert isinstance(engine.connection, Connection)


class T(NamedTuple):
    id: int | None
    name: str
    age: int


class TblDates(NamedTuple):
    id: int | None
    name: str
    score: float
    age: int
    data: bytes
    startdate: dt.date
    modified: dt.datetime


def test_ensure_table_created(engine: Engine) -> None:
    engine.ensure_table_created(TblDates)

    # Check that a table was created
    cursor = engine.connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    assert isinstance(tables, list)
    assert len(tables) == 1

    # Table Name
    assert tables[0][0] == TblDates.__name__

    # Primary Key
    cursor.execute(f"PRAGMA table_info({TblDates.__name__});")
    columns = cursor.fetchall()
    assert len(columns) == len(TblDates._fields)
    assert columns[0][1] == "id"  # Column Name
    assert columns[0][2] == "INTEGER"  # Column Type
    assert columns[0][3] == 1  # Not Null
    assert columns[0][5] == 1  # Primary Key

    # Name Field
    assert columns[1][1] == "name"  # Column Name
    assert columns[1][2] == "TEXT"  # Column Type
    assert columns[1][3] == 1  # Not Null
    assert columns[1][5] == 0  # Not Primary Key

    # Score Field
    assert columns[2][1] == "score"  # Column Name
    assert columns[2][2] == "REAL"  # Column Type
    assert columns[2][3] == 1  # Not Null
    assert columns[2][5] == 0  # Not Primary Key

    # Age Field
    assert columns[3][1] == "age"  # Column Name
    assert columns[3][2] == "INTEGER"  # Column Type
    assert columns[3][3] == 1  # Not Null
    assert columns[3][5] == 0  # Not Primary Key

    # Data Field
    assert columns[4][1] == "data"  # Column Name
    assert columns[4][2] == "BLOB"  # Column Type
    assert columns[4][3] == 1  # Not Null
    assert columns[4][5] == 0  # Not Primary Key

    # Startdate Field
    assert columns[5][1] == "startdate"  # Column Name
    assert columns[5][2] == "TEXT"  # Column Type
    assert columns[5][3] == 1  # Not Null
    assert columns[5][5] == 0  # Not Primary Key

    # Modified Field
    assert columns[6][1] == "modified"  # Column Name
    assert columns[6][2] == "TEXT"  # Column Type
    assert columns[6][3] == 1  # Not Null
    assert columns[6][5] == 0  # Not Primary Key


def test_insert_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(1, "Alice", 30)

    engine.insert(row)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (1, "Alice", 30)
    assert row == T(*rows[0])


def test_get_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(1, "Alice", 30)
    engine.insert(row)

    retrieved_row = engine.get(T, 1)

    assert retrieved_row == row


def test_insert_fills_in_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)

    returned_row = engine.insert(row)

    assert returned_row.id == 1

    returned_row = engine.insert(row)

    assert returned_row.id == 2


def test_cannot_insert_null_value_in_not_null_column(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", None)  # type: ignore this bug is part of the test

    with pytest.raises(Exception, match="NOT NULL constraint failed"):
        engine.insert(row)


def test_update_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))
    engine.update(row._replace(name="Bob"))

    assert row.id is not None
    retrieved_row = engine.get(T, row.id)

    assert retrieved_row == T(row.id, "Bob", 30)


def test_delete_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    assert row.id is not None
    engine.delete(T, row.id)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 0
