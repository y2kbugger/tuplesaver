from __future__ import annotations

import datetime as dt
import sqlite3
from typing import NamedTuple, Optional, Union

import pytest

from micro_namedtuple_sqlite_persister.persister import unwrap_optional_type

from .persister import Engine, FieldZeroIdRequired, InvalidAdaptConvertType, TableSchemaMismatch, UnregisteredFieldTypeError, enable_included_adaptconverters, register_adapt_convert


@pytest.fixture(autouse=True, scope="session")
def enable_adapt_converters() -> None:
    enable_included_adaptconverters()


@pytest.fixture
def engine() -> Engine:
    return Engine(":memory:", echo_sql=False)


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
    serial: int | None


def test_forgetting_id_column_as_first_field_raises(engine: Engine) -> None:
    class TblNoId(NamedTuple):
        name: str

    with pytest.raises(FieldZeroIdRequired):
        engine.ensure_table_created(TblNoId)


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
    assert columns[5][2] == "datetime.date"  # Column Type
    assert columns[5][3] == 1  # Not Null
    assert columns[5][5] == 0  # Not Primary Key

    # Modified Field
    assert columns[6][1] == "modified"  # Column Name
    assert columns[6][2] == "datetime.datetime"  # Column Type
    assert columns[6][3] == 1  # Not Null
    assert columns[6][5] == 0  # Not Primary Key

    # Serial Field
    assert columns[7][1] == "serial"  # Column Name
    assert columns[7][2] == "INTEGER"  # Column Type
    assert columns[7][3] == 0  # Nullable
    assert columns[7][5] == 0  # Not Primary Key


def test_ensure_table_created_with_table_already_created_correct_is_silent(engine: Engine) -> None:
    class TblAlreadyCreated(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)
    engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created_with_table_already_created_incorrect_raises(engine: Engine) -> None:
    class TblAlreadyCreated(NamedTuple):  # type: ignore shadowing is part of the test
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)

    class TblAlreadyCreated(NamedTuple):
        id: int | None
        name: str
        age: int
        data: bytes

    with pytest.raises(TableSchemaMismatch):
        engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created_catches_mismatched_from_out_of_band_alters(engine: Engine) -> None:
    class TblAlreadyCreated(NamedTuple):
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)
    alter = f"ALTER TABLE {TblAlreadyCreated.__name__} ADD COLUMN out_of_band_edit TEXT"
    engine.connection.execute(alter)

    with pytest.raises(TableSchemaMismatch):
        engine.ensure_table_created(TblAlreadyCreated)


def test_ensure_table_created_catches_force_recreate(engine: Engine) -> None:
    class TblAlreadyCreated(NamedTuple):  # type: ignore shadowing is part of the test
        id: int | None
        name: str
        age: int

    engine.ensure_table_created(TblAlreadyCreated)

    class TblAlreadyCreated(NamedTuple):
        id: int | None
        name: str
        age: int
        data: bytes

    engine.ensure_table_created(TblAlreadyCreated, force_recreate=True)
    engine.ensure_table_created(TblAlreadyCreated)  # just a double check for it being recreated.


def test_creating_table_including_unknown_type_raises_error(engine: Engine) -> None:
    class NewType: ...

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        unknown: NewType

    with pytest.raises(UnregisteredFieldTypeError):
        engine.ensure_table_created(ModelUnknownType)


def test_unwrap_optional_type() -> None:
    # Non-optional hint
    assert unwrap_optional_type(int) == (False, int)

    # Show that any pair optional syntaxs are == equivalent
    assert Union[int, None] == Optional[int]
    assert Union[int, None] == int | None
    assert Optional[int] == int | None
    assert Optional[int] == Union[int, None]
    assert int | None == Union[int, None]
    assert int | None == Optional[int]

    # Simple standard optional hints
    assert unwrap_optional_type(Union[int, None]) == (True, int)
    assert unwrap_optional_type(Optional[int]) == (True, int)
    assert unwrap_optional_type(int | None) == (True, int)

    # Unions including more than one type in addition to None
    assert unwrap_optional_type(Union[int, str, None]) == (True, int | str)
    assert unwrap_optional_type(int | str | None) == (True, int | str)

    # Unions not including None
    U = Union[int, str]
    UT = int | str
    assert U == UT
    assert unwrap_optional_type(U) == (False, int | str)
    assert unwrap_optional_type(UT) == (False, int | str)

    # Types nested within optional
    assert unwrap_optional_type(Union[U, None]) == (True, int | str)
    assert unwrap_optional_type(Optional[U]) == (True, int | str)
    assert unwrap_optional_type((U) | None) == (True, int | str)

    assert unwrap_optional_type(Union[UT, None]) == (True, int | str)
    assert unwrap_optional_type(Optional[UT]) == (True, int | str)
    assert unwrap_optional_type((UT) | None) == (True, int | str)

    # Nest unions are flattened and deduped and thus nested optionals are not preserved
    OU = Optional[Union[int, None]]
    OUT = Optional[int | None]
    assert OU == OUT
    assert unwrap_optional_type(Union[OU, None]) == (True, (int))
    assert unwrap_optional_type(Optional[OU]) == (True, (int))
    assert unwrap_optional_type((OU) | None) == (True, (int))

    assert unwrap_optional_type(Union[OUT, None]) == (True, (int))
    assert unwrap_optional_type(Optional[OUT]) == (True, (int))


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


def test_get_row_with_id_as_none(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot SELECT, id=None"):
        engine.get(T, None)


def test_get_row_with_non_existent_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot SELECT, no row with id="):
        engine.get(T, 78787)


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

    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL constraint failed"):
        engine.insert(row)


def test_update_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))
    engine.update(row._replace(name="Bob"))

    assert row.id is not None
    retrieved_row = engine.get(T, row.id)

    assert retrieved_row == T(row.id, "Bob", 30)


def test_update_row_with_null_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))

    with pytest.raises(ValueError, match="Cannot UPDATE, id=None"):
        engine.update(row._replace(id=None))


def test_update_row_with_non_existent_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot UPDATE, no row with id="):
        engine.update(T(78787, "Bob", 30))


def test_delete_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(T, row.id)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete_row_with_object(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.insert(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(row)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete_row_with_non_existent_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot DELETE, no row with id="):
        engine.delete(T, 78787)


def test_delete_row_with_id_as_none(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot DELETE, id=None"):
        engine.delete(T, None)


def test_can_insert_and_retrieve_datetime(engine: Engine) -> None:
    engine.ensure_table_created(TblDates)
    row = TblDates(None, "Alice", 30.0, 30, b"some data", dt.date(2021, 1, 1), dt.datetime(2021, 1, 1, 5, 33), None)
    assert type(row.modified) is dt.datetime

    row = engine.insert(row)
    retrieved_row = engine.get(TblDates, row.id)

    assert type(retrieved_row.modified) is dt.datetime
    assert retrieved_row.modified == row.modified


def test_can_insert_and_retrieve_date(engine: Engine) -> None:
    engine.ensure_table_created(TblDates)
    row = TblDates(None, "Alice", 30.0, 30, b"some data", dt.date(2021, 1, 1), dt.datetime(2021, 1, 1, 5, 33), None)
    assert type(row.startdate) is dt.date

    row = engine.insert(row)
    retrieved_row = engine.get(TblDates, row.id)

    assert type(retrieved_row.startdate) is dt.date
    assert retrieved_row.startdate == row.startdate


def test_registering_adapt_convert_pair(engine: Engine) -> None:
    class NewType:
        def __init__(self, values: list[str]) -> None:
            self.values = values

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        custom: NewType

    def adapt_newtype(value: NewType) -> bytes:
        return ",".join(value.values).encode()

    def convert_newtype(value: bytes) -> NewType:
        return NewType(value.decode().split(","))

    register_adapt_convert(NewType, adapt_newtype, convert_newtype)

    ### Table Creation
    engine.ensure_table_created(ModelUnknownType)

    cursor = engine.connection.cursor()
    cursor.execute(f"PRAGMA table_info({ModelUnknownType.__name__});")
    columns = cursor.fetchall()
    assert len(columns) == len(ModelUnknownType._fields)
    assert columns[2][1] == "custom"  # Column Name
    assert columns[2][2] == "micro_namedtuple_sqlite_persister.persister_test.NewType"  # Column Type
    assert columns[2][3] == 1  # Not Null
    assert columns[2][5] == 0  # Not Primary Key

    ### Adapt
    row = ModelUnknownType(1, "Alice", NewType(["a", "b", "c"]))
    row = engine.insert(row)

    cursor.execute(f"SELECT substr(custom,0) FROM {ModelUnknownType.__name__};")  # substr converting to NewType with converter
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == b'a,b,c'  # adapted to a binary format

    ### Convert
    retrieved_row = engine.get(ModelUnknownType, row.id)
    assert type(retrieved_row.custom) is NewType
    assert retrieved_row.custom.values == ["a", "b", "c"]


def test_attempted_registration_of_an_union_raises() -> None:
    class NewType: ...

    with pytest.raises(InvalidAdaptConvertType):
        register_adapt_convert(Optional[NewType], lambda x: x, lambda x: x)  # type: ignore this is part of test
