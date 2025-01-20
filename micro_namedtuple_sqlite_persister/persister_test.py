from __future__ import annotations

import datetime as dt
import sqlite3
from random import choice, random
from typing import Any, NamedTuple, assert_type

import pytest
from pytest_benchmark.plugin import BenchmarkFixture

from .model import is_registered_row_model, is_registered_table_model
from .persister import (
    Engine,
    FieldZeroIdRequired,
    TableSchemaMismatch,
    TypedCursorProxy,
    UnregisteredFieldTypeError,
)


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


class TableInfo(NamedTuple):
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int


class SqliteSchema(NamedTuple):
    type: str
    name: str
    tbl_name: str
    rootpage: int
    sql: str


def test_ensure_table_created_using_table_info(engine: Engine) -> None:
    engine.ensure_table_created(TblDates)

    # Table as a whole
    tables = engine.query(SqliteSchema, "SELECT * FROM sqlite_schema WHERE type='table'").fetchall()
    assert len(tables) == 1
    table = tables[0]
    assert table.name == TblDates.__name__

    # Columns
    cols = engine.query(TableInfo, f"PRAGMA table_info({TblDates.__name__})").fetchall()

    assert len(cols) == len(TblDates._fields)

    assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
    assert cols[1] == TableInfo(1, "name", "TEXT", 1, None, 0)
    assert cols[2] == TableInfo(2, "score", "REAL", 1, None, 0)
    assert cols[3] == TableInfo(3, "age", "INTEGER", 1, None, 0)
    assert cols[4] == TableInfo(4, "data", "BLOB", 1, None, 0)
    assert cols[5] == TableInfo(5, "startdate", "datetime.date", 1, None, 0)
    assert cols[6] == TableInfo(6, "modified", "datetime.datetime", 1, None, 0)
    assert cols[7] == TableInfo(7, "serial", "INTEGER", 0, None, 0)


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


def test_creating_table_with_unknown_type_raises_error(engine: Engine) -> None:
    class NewType: ...

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        unknown: NewType

    with pytest.raises(UnregisteredFieldTypeError, match="has not been registered with an adapter and converter"):
        engine.ensure_table_created(ModelUnknownType)


def test_creating_table_with_optional_unknown_type_raises_error(engine: Engine) -> None:
    class NewType: ...

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        unknown: NewType | None

    with pytest.raises(UnregisteredFieldTypeError, match="has not been registered with an adapter and converter"):
        engine.ensure_table_created(ModelUnknownType)


def test_creating_table_including_unknown_model_type_raises_error(engine: Engine) -> None:
    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str

    class Model(NamedTuple):
        id: int | None
        name: str
        unknown: ModelUnknownType

    with pytest.raises(UnregisteredFieldTypeError, match="is a NamedTuple Row Model"):
        engine.ensure_table_created(Model)


def test_creating_table_including_optional_unknown_model_type_raises_error(engine: Engine) -> None:
    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str

    class Model(NamedTuple):
        id: int | None
        name: str
        unknown: ModelUnknownType | None

    with pytest.raises(UnregisteredFieldTypeError, match="is a NamedTuple Row Model"):
        engine.ensure_table_created(Model)


def test_table_model_only_registered_when_successful(engine: Engine) -> None:
    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str

    class Model(NamedTuple):
        id: int | None
        name: str
        unknown: ModelUnknownType | None

    with pytest.raises(UnregisteredFieldTypeError, match="is a NamedTuple Row Model"):
        engine.ensure_table_created(Model)

    assert not is_registered_table_model(Model)


def test_table_creation_registers_table(engine: Engine) -> None:
    class Model(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(Model) is False
    assert is_registered_table_model(Model) is False

    engine.ensure_table_created(Model)

    assert is_registered_row_model(Model) is True
    assert is_registered_table_model(Model) is True


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
    assert type(retrieved_row) is T


def test_get_row_with_id_as_none(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot SELECT, id=None"):
        engine.get(T, None)


def test_insert_row_benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)

    def insert_row():
        engine.insert(row)

    benchmark(insert_row)


def test_get_row_benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)
    engine.insert(row)

    def get_row():
        engine.get(T, 1)

    benchmark(get_row)


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
    assert_type(cur.fetchone(), ModelX)
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
    assert_type(row, ModelX)
    assert isinstance(row, ModelX)

    # Engine.connection.cursor still gives back raw rows
    cur = engine.connection.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    assert_type(row, Any)
    assert not isinstance(row, ModelX)


class TestRelatedTable:
    class Team(NamedTuple):
        id: int | None
        name: str

    class Person(NamedTuple):
        id: int | None
        name: str
        team: TestRelatedTable.Team

    class Arm(NamedTuple):
        id: int | None
        length: float
        person: TestRelatedTable.Person

    def test_ensure_table_created_with_related_table(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        cols = engine.query(TableInfo, f"PRAGMA table_info({self.Person.__name__})").fetchall()

        assert len(cols) == len(self.Person._fields)

        assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
        assert cols[1] == TableInfo(1, "name", "TEXT", 1, None, 0)
        assert cols[2] == TableInfo(2, "team", "Team_ID", 1, None, 0)

    def test_insert_row_with_related_table(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = self.Team(None, "Team A")
        person = self.Person(None, "Alice", team)
        person = engine.insert(person)

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row == self.Person(1, "Alice", self.Team(1, "Team A"))
        assert person == self.Person(*row)

    def test_three_model_relation_chain(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)
        engine.ensure_table_created(self.Arm)

        team = self.Team(None, "Team A")
        person = self.Person(None, "Alice", team)
        arm = self.Arm(None, 30.0, person)
        arm = engine.insert(arm)

        row = engine.query(self.Arm, "SELECT * FROM Arm;").fetchone()

        assert row == self.Arm(1, 30.0, self.Person(1, "Alice", self.Team(1, "Team A")))


class TestOptionalRelatedTable:
    class Team(NamedTuple):
        id: int | None
        name: str

    class Person(NamedTuple):
        id: int | None
        name: str
        team: TestOptionalRelatedTable.Team | None

    def test_ensure_table_created_with_related_table(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        cols = engine.query(TableInfo, f"PRAGMA table_info({self.Person.__name__})").fetchall()

        assert len(cols) == len(self.Person._fields)

        assert cols[0] == TableInfo(0, "id", "INTEGER", 1, None, 1)
        assert cols[1] == TableInfo(1, "name", "TEXT", 1, None, 0)
        assert cols[2] == TableInfo(2, "team", "Team_ID", 0, None, 0)

    def test_insert_row_with_related_table(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = self.Team(None, "Team A")
        person = self.Person(None, "Alice", team)
        person = engine.insert(person)

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row == self.Person(1, "Alice", self.Team(1, "Team A"))
        assert person == self.Person(*row)

    def test_insert_row_with_null_relation(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        person = self.Person(None, "Alice", None)
        person = engine.insert(person)

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row == self.Person(1, "Alice", None)
        assert person == self.Person(*row)


class TestBomSelfJoin:
    class BOM(NamedTuple):
        id: int | None
        name: str
        value: float
        child_a: TestBomSelfJoin.BOM | None
        child_b: TestBomSelfJoin.BOM | None

    @staticmethod
    def generate_node_name_node(depth: int) -> str:
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        return f"{choice(alphabet)}{choice(alphabet)}{choice(alphabet)}{depth:05d}"

    @staticmethod
    def create_bom(depth: int) -> BOM:
        if depth == 1:
            child_a = None
            child_b = None
        else:
            child_a = TestBomSelfJoin.create_bom(depth - 1)
            child_b = TestBomSelfJoin.create_bom(depth - 1)

        return TestBomSelfJoin.BOM(
            None,
            TestBomSelfJoin.generate_node_name_node(depth),
            random() * 1000 - 500,
            child_a,
            child_b,
        )

    @staticmethod
    def create_linear_bom(depth: int) -> BOM:
        return TestBomSelfJoin.BOM(
            None,
            TestBomSelfJoin.generate_node_name_node(depth),
            random() * 1000 - 500,
            None,
            None if depth == 1 else TestBomSelfJoin.create_bom(depth - 1),
        )

    def test_insert_bom(self, engine: Engine) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(3)
        _inserted_root = engine.insert(root)
        engine.connection.commit()

    def test_get_bom(self, engine: Engine) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(3)
        inserted_root = engine.insert(root)
        engine.connection.commit()

        retrieved_root = engine.get(self.BOM, inserted_root.id)
        assert retrieved_root == inserted_root

    @pytest.mark.xfail(reason="Insert still uses recursion")
    def test_for_stack_overflow(self, engine: Engine) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_linear_bom(1500)
        inserted_root = engine.insert(root)
        engine.connection.commit()

        retrieved_root = engine.get(self.BOM, inserted_root.id)
        assert retrieved_root == inserted_root

    def test_benchmark_insert_bom(self, engine: Engine, benchmark: BenchmarkFixture) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(7)

        @benchmark
        def insert_bom():
            _inserted_root = engine.insert(root)
            engine.connection.commit()

    def test_benchmark_get_bom(self, engine: Engine, benchmark: BenchmarkFixture) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(7)

        inserted_root = engine.insert(root)
        engine.connection.commit()

        @benchmark
        def get_bom():
            assert inserted_root is not None
            _retrieved_root = engine.get(self.BOM, inserted_root.id)


def test_query_registers_row_but_not_table(engine: Engine) -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    cur = engine.query(ModelA, "SELECT 1 as id, 'Alice' as name;")

    # I don't care either way, just documenring current behavior
    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    cur.fetchone()

    assert is_registered_row_model(ModelA) is True
    assert is_registered_table_model(ModelA) is False
