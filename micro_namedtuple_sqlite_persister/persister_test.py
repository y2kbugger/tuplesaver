from __future__ import annotations

import datetime as dt
import sqlite3
from random import choice, random
from typing import Any, NamedTuple

import pytest
from pytest_benchmark.plugin import BenchmarkFixture

from micro_namedtuple_sqlite_persister.conftest import SqlLog

from .model import FieldZeroIdRequired, is_registered_row_model, is_registered_table_model
from .persister import (
    Engine,
    IdNoneError,
    IdNotFoundError,
    InvalidKwargFieldSpecifiedError,
    NoKwargFieldSpecifiedError,
    TableSchemaMismatch,
    UnpersistedRelationshipError,
)


def test_get_connection(engine: Engine) -> None:
    from sqlite3 import Connection

    assert isinstance(engine.connection, Connection)


class T(NamedTuple):
    id: int | None
    name: str
    age: int


class TableInfo(NamedTuple):
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int


def test_ensure_table_created_using_table_info(engine: Engine) -> None:
    class TblDates(NamedTuple):
        id: int | None
        name: str
        score: float
        age: int
        data: bytes
        startdate: dt.date
        modified: dt.datetime
        serial: int | None

    engine.ensure_table_created(TblDates)

    # Table as a whole
    class SqliteSchema(NamedTuple):
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


def test_ensure_table_created__fails__doesnt_register_table_model(engine: Engine) -> None:
    class TNoId(NamedTuple):
        name: str

    with pytest.raises(FieldZeroIdRequired):
        engine.ensure_table_created(TNoId)

    assert not is_registered_table_model(TNoId)


def test_ensure_table_created__is_successful__registers_table_model(engine: Engine) -> None:
    class Model(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(Model) is False
    assert is_registered_table_model(Model) is False

    engine.ensure_table_created(Model)

    assert is_registered_row_model(Model) is True
    assert is_registered_table_model(Model) is True


def test_save_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.save(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (row.id, "Alice", 30)
    assert row == T(*rows[0])


def test_save_benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)

    def save():
        engine.save(row)

    benchmark(save)


def test_find_byid(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)
    row = engine.save(row)

    retrieved_row = engine.find(T, row.id)

    assert retrieved_row == row
    assert type(retrieved_row) is T


def test_find_byid_as_none(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(IdNoneError, match="Cannot SELECT, id=None"):
        engine.find(T, None)


def test_find_byid_id_not_found(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(IdNotFoundError, match="Cannot SELECT, no row with id="):
        engine.find(T, 78787)


def test_find_benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(T)
    engine.save(T(None, "Alice", 30))

    def find():
        engine.find(T, 1)

    benchmark(find)


def test_find_by_field(engine: Engine) -> None:
    # one field
    engine.ensure_table_created(T)

    engine.save(T(None, "Alice", 30))
    engine.save(T(None, "Bob", 33))

    assert engine.find_by(T, name="Alice") == T(1, "Alice", 30)
    assert engine.find_by(T, age=30) == T(1, "Alice", 30)
    assert engine.find_by(T, name="Bob") == T(2, "Bob", 33)
    assert engine.find_by(T, age=33) == T(2, "Bob", 33)


def test_find_by_field_no_match(engine: Engine) -> None:
    engine.ensure_table_created(T)

    engine.save(T(None, "Alice", 30))
    engine.save(T(None, "Bob", 33))

    assert engine.find_by(T, name="Karl") is None


def test_find_by_fields(engine: Engine) -> None:
    # multiple fields
    engine.ensure_table_created(T)
    r1 = engine.save(T(None, "Alice", 30))
    r2 = engine.save(T(None, "Bob", 33))
    r3 = engine.save(T(None, "Alice", 33))

    assert engine.find_by(T, name="Alice", age=30) == r1
    assert engine.find_by(T, name="Bob", age=33) == r2
    assert engine.find_by(T, name="Alice", age=33) == r3


def test_find_by_fields_with_no_kwargs(engine: Engine) -> None:
    with pytest.raises(NoKwargFieldSpecifiedError, match="At least one field must be specified to find a row."):
        engine.find_by(T)


def test_find_by_fields_with_invalid_kwargs(engine: Engine) -> None:
    with pytest.raises(InvalidKwargFieldSpecifiedError):
        engine.find_by(T, doesnt_exist="test")


def test_save_fills_in_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", 30)

    returned_row = engine.save(row)

    assert returned_row.id == 1

    returned_row = engine.save(row)

    assert returned_row.id == 2


def test_save_cannot_insert_null_value_in_not_null_column(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = T(None, "Alice", None)  # type: ignore this bug is part of the test

    with pytest.raises(sqlite3.IntegrityError, match="NOT NULL constraint failed"):
        engine.save(row)


def test_save_updates_row(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.save(T(None, "Alice", 30))
    engine.save(row._replace(name="Bob"))

    assert row.id is not None
    retrieved_row = engine.find(T, row.id)

    assert retrieved_row == T(row.id, "Bob", 30)


def test_save_with_nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot UPDATE, no row with id="):
        engine.save(T(78787, "Bob", 30))


def test_delete_byid(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.save(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(T, row.id)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete_byrow(engine: Engine) -> None:
    engine.ensure_table_created(T)
    row = engine.save(T(None, "Alice", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(row)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM T;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete_with_nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot DELETE, no row with id="):
        engine.delete(T, 78787)


def test_delete_row_with_id_as_none(engine: Engine) -> None:
    engine.ensure_table_created(T)
    with pytest.raises(ValueError, match="Cannot DELETE, id=None"):
        engine.delete(T, None)


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

    def test_save__when_has_related_model(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = engine.save(self.Team(None, "Team A"))
        person = engine.save(self.Person(None, "Alice", team))

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row is not None
        assert row == self.Person(1, "Alice", self.Team(1, "Team A"))
        assert person == self.Person(*row)

    def test_save__when_related_model_is_unpersisted__raises(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = self.Team(None, "Team A")
        with pytest.raises(UnpersistedRelationshipError):
            _person = engine.save(self.Person(None, "Alice", team))

    def test_three_model_relation_chain(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)
        engine.ensure_table_created(self.Arm)

        team = engine.save(self.Team(None, "Team A"))
        person = engine.save(self.Person(None, "Alice", team))
        _arm = engine.save(self.Arm(None, 30.0, person))

        row = engine.query(self.Arm, "SELECT * FROM Arm;").fetchone()

        assert row == self.Arm(1, 30.0, self.Person(1, "Alice", self.Team(1, "Team A")))

    def test_deep_insert_doesnt_duplicate_twice_used_related_model_separate_saves(self, engine: Engine) -> None:
        """This was a real bug where the same related model was inserted twice,
        causing duplicate entries in the database."""
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = self.Team(None, "Team A")
        alice = engine.save(self.Person(None, "Alice", team), deep=True)
        bob = engine.save(self.Person(None, "Bob", team), deep=True)

        # we can go either way on this one
        # but at least make sure we have a test for it
        # would need some persisted identity map to make sure this is the same object
        assert alice.team.id != bob.team.id

    def test_deep_insert_doesnt_duplicate_twice_used_related_model_single_save(self, engine: Engine) -> None:
        class PersonWithTwoTeams(NamedTuple):
            id: int | None
            name: str
            team_primary: TestRelatedTable.Team
            team_secondary: TestRelatedTable.Team

        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(PersonWithTwoTeams)

        teamb = self.Team(None, "Team B")
        person = engine.save(PersonWithTwoTeams(None, "Alice", teamb, teamb), deep=True)

        # this could be handled by a temporary identity map, but that
        # complicates api/implementation of save
        # and it might be suprising if teamb was was inserted twice
        # but it is a rare edge, lets document it here and add fix later.
        assert person.team_primary.id != person.team_secondary.id

    def test_deep_insert_cannot_reliably_distinguish_between_identical_tuples(self, engine: Engine) -> None:
        class PersonWithTwoTeams(NamedTuple):
            id: int | None
            name: str
            team_primary: TestRelatedTable.Team
            team_secondary: TestRelatedTable.Team

        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(PersonWithTwoTeams)

        teama = self.Team(None, "myteam")
        teamb = self.Team(None, "myteam")
        person = engine.save(PersonWithTwoTeams(None, "Alice", teama, teamb), deep=True)

        # this is a tricky/unstable case, because of the tuple cache
        if id(teama) == id(teamb):  # this is sometimes true, sometimes not
            # this is the case where we have only a single object, because of the tuple cache
            # and we must treat them as the same
            assert person.team_primary.id == person.team_secondary.id
        else:
            # this is the case where we have two different objects
            # and we should not assume they are the same
            assert person.team_primary.id != person.team_secondary.id

    def test_that_you_can_get_more_than_one_top_level_result(self, engine: Engine) -> None:
        """This was a real bug where the proxy cursor row factory reused the
        same outer cursor and through away subsequent results."""

        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        team = engine.save(self.Team(None, "Team A"))
        person1 = engine.save(self.Person(None, "Alice", team))
        person2 = engine.save(self.Person(None, "Bob", team))

        rows = engine.query(self.Person, "SELECT * FROM Person;").fetchall()

        assert rows == [person1, person2]


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

        team = engine.save(self.Team(None, "Team A"))
        person = self.Person(None, "Alice", team)
        person = engine.save(person)

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row is not None
        assert row == self.Person(1, "Alice", self.Team(1, "Team A"))
        assert person == self.Person(*row)

    def test_insert_row_with_null_relation(self, engine: Engine) -> None:
        engine.ensure_table_created(self.Team)
        engine.ensure_table_created(self.Person)

        person = self.Person(None, "Alice", None)
        person = engine.save(person)

        row = engine.query(self.Person, "SELECT * FROM Person;").fetchone()
        assert row is not None
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
        _inserted_root = engine.save(root, deep=True)
        engine.connection.commit()

    def test_get_bom(self, engine: Engine) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(3)
        inserted_root = engine.save(root, deep=True)
        engine.connection.commit()

        retrieved_root = engine.find(self.BOM, inserted_root.id)
        assert retrieved_root == inserted_root

    def test_get_bom_with_raw_fk(self, engine: Engine) -> None:
        engine.ensure_table_created(self.BOM)

        root = self.create_bom(3)
        inserted_root = engine.save(root, deep=True)
        engine.connection.commit()
        assert inserted_root.child_a is not None
        assert inserted_root.child_b is not None

        class BOM_View(NamedTuple):
            id: int | None
            name: str
            value: float
            child_a: int | None  # as fk
            child_b: int | None  # as fk

        retrieved_root = engine.find(BOM_View, inserted_root.id)

        assert retrieved_root == BOM_View(
            inserted_root.id,
            inserted_root.name,
            inserted_root.value,
            inserted_root.child_a.id,
            inserted_root.child_b.id,
        )

    def test_get_bom_with_fks_excluded(self, engine: Engine, sql_log: SqlLog) -> None:
        engine.ensure_table_created(self.BOM)

        root = self.create_bom(3)
        inserted_root = engine.save(root, deep=True)
        engine.connection.commit()

        class BOM_View(NamedTuple):
            id: int | None
            name: str
            value: float

        sql_log.clear()
        retrieved_root = engine.find(BOM_View, inserted_root.id)
        assert "child_a" not in sql_log  # Ensure that the query did not include the child_a column, even if it ended up discarding it.
        assert retrieved_root == BOM_View(inserted_root.id, inserted_root.name, inserted_root.value)

    @pytest.mark.xfail(reason="Insert still uses recursion")
    def test_for_stack_overflow(self, engine: Engine, limit_stack_depth: None) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_linear_bom(1500)
        inserted_root = engine.save(root, deep=True)
        engine.connection.commit()

        retrieved_root = engine.find(self.BOM, inserted_root.id)
        assert retrieved_root == inserted_root

    def test_benchmark_insert_bom(self, engine: Engine, benchmark: BenchmarkFixture) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(7)

        @benchmark
        def insert_bom():
            _inserted_root = engine.save(root, deep=True)

    def test_benchmark_get_bom(self, engine: Engine, benchmark: BenchmarkFixture) -> None:
        engine.ensure_table_created(self.BOM)
        root = self.create_bom(7)

        inserted_root = engine.save(root, deep=True)
        engine.connection.commit()

        @benchmark
        def get_bom():
            assert inserted_root is not None
            _retrieved_root = engine.find(self.BOM, inserted_root.id)


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
