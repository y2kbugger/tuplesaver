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
    id: int
    name: str


def test_ensure_table_created(engine: Engine) -> None:
    engine.ensure_table_created(T)

    # Check that a table was created
    cursor = engine.connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    assert isinstance(tables, list)
    assert len(tables) == 1

    # Table Name
    assert tables[0][0] == "T"

    # Primary Key
    cursor.execute("PRAGMA table_info(T);")
    columns = cursor.fetchall()
    assert len(columns) == 2
    assert columns[0][1] == "id"  # Column Name
    assert columns[0][2] == "INTEGER"  # Column Type
    assert columns[0][3] == 0  # Not Null
    assert columns[0][5] == 1  # Primary Key

    # Name Field
    assert columns[1][1] == "name"  # Column Name
    assert columns[1][2] == "TEXT"  # Column Type
    assert columns[1][3] == 0  # Not Null
    assert columns[1][5] == 0  # Not Primary Key
