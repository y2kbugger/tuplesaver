from __future__ import annotations

from dataclasses import replace

import apsw
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from .engine import (
    Engine,
    IdNoneError,
    InvalidKwargFieldSpecifiedError,
    LookupByAdHocModelImpossible,
    NoKwargFieldSpecifiedError,
    UnpersistedRelationshipError,
)
from .model import Row, TableRow


class Team(TableRow):
    name: str
    size: int


class Person(TableRow):
    name: str
    team: Team


class Arm(TableRow):
    length: float
    person: Person


class AdHoc(Row):
    score: float


def test_engine_connection(engine: Engine) -> None:
    hasattr(engine.connection, "cursor")
    hasattr(engine.connection, "execute")


def test_find__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)
    row = engine.save(row)

    retrieved_row = engine.find(Team, row.id)

    assert retrieved_row is not None
    assert retrieved_row == row
    assert type(retrieved_row) is Team


def test_find__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    engine.save(Team("Lions", 30))

    def find():
        engine.find(Team, 1)

    benchmark(find)


def test_find__id_is_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(IdNoneError, match="Cannot SELECT, id=None"):
        engine.find(Team, None)


def test_find__id_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    assert engine.find(Team, 78787) is None


def test_find__adhoc_model(engine: Engine) -> None:
    with pytest.raises(LookupByAdHocModelImpossible, match="Cannot lookup via adhoc model: `AdHoc`"):
        engine.find(AdHoc, 1)  # ty:ignore[invalid-argument-type]


def test_find_by__field(engine: Engine) -> None:
    # one field
    engine.ensure_table_created(Team)

    engine.save(Team("Lions", 30))
    engine.save(Team("Tigers", 33))

    found = engine.find_by(Team, name="Lions")
    assert isinstance(found, Team)

    assert engine.find_by(Team, name="Lions") == Team("Lions", 30, id=1)
    assert engine.find_by(Team, size=30) == Team("Lions", 30, id=1)
    assert engine.find_by(Team, name="Tigers") == Team("Tigers", 33, id=2)
    assert engine.find_by(Team, size=33) == Team("Tigers", 33, id=2)


def test_find_by__field_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)

    engine.save(Team("Lions", 30))
    engine.save(Team("Tigers", 33))

    assert engine.find_by(Team, name="Karl") is None


def test_find_by__fields(engine: Engine) -> None:
    # multiple fields
    engine.ensure_table_created(Team)
    r1 = engine.save(Team("Lions", 30))
    r2 = engine.save(Team("Tigers", 33))
    r3 = engine.save(Team("Lions", 33))

    assert engine.find_by(Team, name="Lions", size=30) == r1
    assert engine.find_by(Team, name="Tigers", size=33) == r2
    assert engine.find_by(Team, name="Lions", size=33) == r3


def test_find_by__fields_with_no_kwargs(engine: Engine) -> None:
    with pytest.raises(NoKwargFieldSpecifiedError, match=r"At least one field must be specified to find a row\."):
        engine.find_by(Team)


def test_find_by__fields_with_invalid_kwargs(engine: Engine) -> None:
    with pytest.raises(InvalidKwargFieldSpecifiedError):
        engine.find_by(Team, doesnt_exist="test")


def test_find_by__adhoc_model(engine: Engine) -> None:
    with pytest.raises(LookupByAdHocModelImpossible, match="Cannot lookup via adhoc model: `AdHoc`"):
        engine.find_by(AdHoc, total=7.7)  # ty:ignore[invalid-argument-type]


def test_query__table_model__succeeds_with_returns_cursor_proxy(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.save(Team("Lions", 30))

    cur = engine.query(Team, "SELECT * FROM Team;")

    row = cur.fetchone()
    assert isinstance(row, Team)
    assert row == Team("Lions", 30, id=1)


def test_query__adhoc_model__succeeds_with_returns_cursor_proxy(engine: Engine) -> None:
    cur = engine.query(AdHoc, "SELECT 7.7 as score;")

    row = cur.fetchone()
    assert isinstance(row, AdHoc)
    assert row == AdHoc(7.7)


def test_save__on_success__inserts_record_to_db(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (row.id, "Lions", 30)


def test_save__on_success__returns_model_with_filled_in_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)

    returned_row = engine.save(row)

    assert returned_row.id == 1

    returned_row = engine.save(row)

    assert returned_row.id == 2


def test_save__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)

    def save():
        engine.save(row)

    benchmark(save)


def test_save__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(ValueError, match="Cannot UPDATE, no row with id="):
        engine.save(Team("Lions", 30, id=78787))


def test_save__related_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = engine.save(Team("Lions", 5))
    person = engine.save(Person("Alice", team))

    row = engine.query(Person, "SELECT * FROM Person;").fetchone()
    assert row is not None
    assert row == person


def test_save__unpersisted_relation__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = Team("Lions", 5)
    with pytest.raises(UnpersistedRelationshipError):
        _person = engine.save(Person("Alice", team))


def test_save__three_model_relation_chain(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    engine.ensure_table_created(Arm)

    team = engine.save(Team("Lions", 5))
    person = engine.save(Person("Alice", team))
    arm = engine.save(Arm(30.0, person))

    row = engine.query(Arm, "SELECT * FROM Arm;").fetchone()

    assert row == arm


def test_save__null_relation(engine: Engine) -> None:
    class A(TableRow):
        pass

    class B(TableRow):
        team: A | None  # Optional relationship

    engine.ensure_table_created(A)
    engine.ensure_table_created(B)

    person = engine.save(B(None))

    row = engine.query(B, "SELECT * FROM B;").fetchone()
    assert row is not None
    assert row == person


def test_save__none_in_not_null_column__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", None)  # type: ignore this bug is part of the test

    with pytest.raises(apsw.ConstraintError, match="NOT NULL constraint failed"):
        engine.save(row)


def test_save__updates_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team("Lions", 30))
    engine.save(replace(row, name="Alice"))

    assert row.id is not None
    retrieved_row = engine.find(Team, row.id)

    assert retrieved_row is not None
    assert retrieved_row == Team("Alice", 30, id=row.id)


def test_delete__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(Team, row.id)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete__by_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 1

    engine.delete(row)

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 0


def test_delete__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(ValueError, match="Cannot DELETE, no row with id="):
        engine.delete(Team, 78787)


def test_delete__id_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(ValueError, match="Cannot DELETE, id=None"):
        engine.delete(Team, None)
