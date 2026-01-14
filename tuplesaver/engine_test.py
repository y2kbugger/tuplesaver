from __future__ import annotations

from typing import NamedTuple

import apsw
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from .engine import (
    Engine,
    IdNoneError,
    InvalidKwargFieldSpecifiedError,
    LookupByAdHocModelImpossible,
    MatchNotFoundError,
    NoKwargFieldSpecifiedError,
    UnpersistedRelationshipError,
)
from .RM import Roww


class Team(Roww):
    id: int | None
    name: str
    size: int


class Person(Roww):
    id: int | None
    name: str
    team: Team


class Arm(Roww):
    id: int | None
    length: float
    person: Person


class AdHoc(NamedTuple):
    score: float


def test_engine_connection(engine: Engine) -> None:
    hasattr(engine.connection, "cursor")
    hasattr(engine.connection, "execute")


def test_find__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team(None, "Lions", 30)
    row = engine.save(row)

    retrieved_row = engine.find(Team, row.id)

    assert retrieved_row == row
    assert type(retrieved_row) is Team


def test_find__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    engine.save(Team(None, "Lions", 30))

    def find():
        engine.find(Team, 1)

    benchmark(find)


def test_find__id_is_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(IdNoneError, match="Cannot SELECT, id=None"):
        engine.find(Team, None)


def test_find__id_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(MatchNotFoundError, match="Cannot SELECT, no row with id="):
        engine.find(Team, 78787)


def test_find__adhoc_model(engine: Engine) -> None:
    with pytest.raises(LookupByAdHocModelImpossible, match="Cannot lookup via adhoc model: `AdHoc`"):
        engine.find(AdHoc, 1)


def test_find_by__field(engine: Engine) -> None:
    # one field
    engine.ensure_table_created(Team)

    engine.save(Team(None, "Lions", 30))
    engine.save(Team(None, "Tigers", 33))

    found = engine.find_by(Team, name="Lions")
    assert isinstance(found, Team)

    assert engine.find_by(Team, name="Lions") == Team(1, "Lions", 30)
    assert engine.find_by(Team, size=30) == Team(1, "Lions", 30)
    assert engine.find_by(Team, name="Tigers") == Team(2, "Tigers", 33)
    assert engine.find_by(Team, size=33) == Team(2, "Tigers", 33)


def test_find_by__field_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)

    engine.save(Team(None, "Lions", 30))
    engine.save(Team(None, "Tigers", 33))

    with pytest.raises(MatchNotFoundError, match="Cannot SELECT, no row with name='Karl' in table `Team`"):
        assert engine.find_by(Team, name="Karl") is None


def test_find_by__fields(engine: Engine) -> None:
    # multiple fields
    engine.ensure_table_created(Team)
    r1 = engine.save(Team(None, "Lions", 30))
    r2 = engine.save(Team(None, "Tigers", 33))
    r3 = engine.save(Team(None, "Lions", 33))

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
        engine.find_by(AdHoc, total=7.7)


def test_query__table_model__succeeds_with_returns_cursor_proxy(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.save(Team(None, "Lions", 30))

    cur = engine.query(Team, "SELECT * FROM Team;")

    row = cur.fetchone()
    assert isinstance(row, Team)
    assert row == Team(1, "Lions", 30)


def test_query__adhoc_model__succeeds_with_returns_cursor_proxy(engine: Engine) -> None:
    cur = engine.query(AdHoc, "SELECT 7.7 as score;")

    row = cur.fetchone()
    assert isinstance(row, AdHoc)
    assert row == AdHoc(7.7)


def test_save__on_success__inserts_record_to_db(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team(None, "Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (row.id, "Lions", 30)
    assert row == Team(*rows[0])


def test_save__on_success__returns_model_with_filled_in_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team(None, "Lions", 30)

    returned_row = engine.save(row)

    assert returned_row.id == 1

    returned_row = engine.save(row)

    assert returned_row.id == 2


def test_save__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    row = Team(None, "Lions", 30)

    def save():
        engine.save(row)

    benchmark(save)


def test_save__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(ValueError, match="Cannot UPDATE, no row with id="):
        engine.save(Team(78787, "Lions", 30))


def test_save__related_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = engine.save(Team(None, "Lions", 5))
    person = engine.save(Person(None, "Alice", team))

    row = engine.query(Person, "SELECT * FROM Person;").fetchone()
    assert row is not None
    assert row == person


def test_save__unpersisted_relation__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = Team(None, "Lions", 5)
    with pytest.raises(UnpersistedRelationshipError):
        _person = engine.save(Person(None, "Alice", team))


def test_save__three_model_relation_chain(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    engine.ensure_table_created(Arm)

    team = engine.save(Team(None, "Lions", 5))
    person = engine.save(Person(None, "Alice", team))
    arm = engine.save(Arm(None, 30.0, person))

    row = engine.query(Arm, "SELECT * FROM Arm;").fetchone()

    assert row == arm


def test_save__null_relation(engine: Engine) -> None:
    class A(Roww):
        id: int | None

    class B(Roww):
        id: int | None
        team: A | None  # Optional relationship

    engine.ensure_table_created(A)
    engine.ensure_table_created(B)

    person = engine.save(B(None, None))

    row = engine.query(B, "SELECT * FROM B;").fetchone()
    assert row is not None
    assert row == person


def test_save__none_in_not_null_column__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team(None, "Lions", None)  # type: ignore this bug is part of the test

    with pytest.raises(apsw.ConstraintError, match="NOT NULL constraint failed"):
        engine.save(row)


def test_save__updates_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team(None, "Lions", 30))
    engine.save(row._replace(name="Alice"))

    assert row.id is not None
    retrieved_row = engine.find(Team, row.id)

    assert retrieved_row == Team(row.id, "Alice", 30)


def test_delete__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.save(Team(None, "Lions", 30))

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
    row = engine.save(Team(None, "Lions", 30))

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
