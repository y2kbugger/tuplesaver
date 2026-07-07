import re
from dataclasses import FrozenInstanceError, replace
from typing import Any, cast

import apsw
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from .conftest import SqlLog
from .engine import (
    Engine,
    InvalidKwargFieldSpecifiedError,
    NoKwargFieldSpecifiedError,
    RecordNotFoundError,
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


class AdHoc_Score(Row):
    score: float

    __select_query__ = "SELECT 7.7 AS score"


class SqliteMaster(Row):
    __tablename__ = "sqlite_master"
    type: str
    sql: str
    name: str


class Team_NameUpper(Row):
    name: str
    shout: str

    __select_query__ = t"SELECT {Team.name}, upper({Team.name}) AS shout FROM {Team}"


class Team_Info(Row):
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int

    __select_query__ = t"PRAGMA table_info({Team})"


class Team_MinSize(Row):
    name: str
    size: int

    # the named parameter is supplied/overridden via the `params` arg
    __select_query__ = t"SELECT {Team.name}, {Team.size} FROM {Team} WHERE {Team.size} >= :min_size"


class Team_AvgSize(Row):
    # the inner query names this column `avg(size)`, NOT `avg_size`; a target on
    # `avg_size` must still resolve via the CTE column-name aliasing.
    avg_size: float

    __select_query__ = t"SELECT avg({Team.size}) FROM {Team}"


def test_engine_connection(engine: Engine) -> None:
    hasattr(engine.connection, "cursor")
    hasattr(engine.connection, "execute")


def test_find__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)
    row = engine.insert(row)

    retrieved_row = engine.find(Team, row.id)

    assert retrieved_row is not None
    assert retrieved_row == row
    assert type(retrieved_row) is Team


def test_find__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))

    def find():
        engine.find(Team, 1)

    benchmark(find)


def test_find__id_is_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(RecordNotFoundError):
        engine.find(Team, None)


def test_find__id_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(RecordNotFoundError):
        engine.find(Team, 78787)


def test_find__no_target_returns_first_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    r1 = engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    assert engine.find(Team) == r1


def test_find__no_target_with_order(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    r2 = engine.insert(Team("Tigers", 33))

    assert engine.find(Team, order="size DESC") == r2


def test_find__order_tstring(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    r2 = engine.insert(Team("Tigers", 33))

    # refactorable: `order` interpolates the field, not a string column name
    assert engine.find(Team, order=t"{Team.size} DESC") == r2


def test_select__order_tstring_fk_path(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    lions = engine.insert(Team("Lions", 30))
    tigers = engine.insert(Team("Tigers", 33))
    engine.insert(Person("Zoe", lions))
    engine.insert(Person("Abe", tigers))

    rows = engine.select(Person, order=t"{Person.team.name} DESC").fetchall()

    assert [p.name for p in rows] == ["Abe", "Zoe"]


def test_find__no_target_empty_table(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(RecordNotFoundError):
        engine.find(Team)


def test_find__adhoc_model(engine: Engine) -> None:
    with pytest.raises(apsw.SQLError):
        engine.find(AdHoc, 1)


def test_find__returned_row_is_immutable(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    retrieved = engine.find(Team, row.id)
    with pytest.raises(FrozenInstanceError):
        retrieved.name = "Tigers"  # type: ignore[misc]


def test_find_by__field(engine: Engine) -> None:
    engine.ensure_table_created(Team)

    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    found = engine.find(Team, Team.name == "Lions")
    assert isinstance(found, Team)

    assert engine.find(Team, Team.name == "Lions") == Team("Lions", 30, id=1)
    assert engine.find(Team, Team.size == 30) == Team("Lions", 30, id=1)
    assert engine.find(Team, Team.name == "Tigers") == Team("Tigers", 33, id=2)
    assert engine.find(Team, Team.size == 33) == Team("Tigers", 33, id=2)


def test_find_by__field_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)

    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    with pytest.raises(RecordNotFoundError):
        engine.find(Team, Team.name == "Karl")


def test_find_by__fields(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    r1 = engine.insert(Team("Lions", 30))
    r2 = engine.insert(Team("Tigers", 33))
    r3 = engine.insert(Team("Lions", 33))

    assert engine.find(Team, (Team.name == "Lions") & (Team.size == 30)) == r1
    assert engine.find(Team, (Team.name == "Tigers") & (Team.size == 33)) == r2
    assert engine.find(Team, (Team.name == "Lions") & (Team.size == 33)) == r3


def test_find_by__fields_with_no_kwargs(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(RecordNotFoundError):
        engine.find(Team, None)


def test_find_by__fields_with_invalid_kwargs(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(AttributeError, match="doesnt_exist"):
        engine.find(Team, Team.doesnt_exist == "test")


def test_find_by__adhoc_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    r = engine.find(SqliteMaster, SqliteMaster.type == 'table')
    assert r is not None
    assert r.type == 'table'
    assert r.name == 'Team'


def test_select__returns_all_rows(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))
    engine.insert(Team("Bears", 25))

    rows = engine.select(Team).fetchall()

    assert len(rows) == 3
    assert all(isinstance(r, Team) for r in rows)
    assert rows[0] == Team("Lions", 30, id=1)
    assert rows[1] == Team("Tigers", 33, id=2)
    assert rows[2] == Team("Bears", 25, id=3)


def test_select__with_kwargs_filters(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))
    engine.insert(Team("Lions", 25))

    rows = engine.select(Team, Team.name == "Lions").fetchall()

    assert len(rows) == 2
    assert rows[0] == Team("Lions", 30, id=1)
    assert rows[1] == Team("Lions", 25, id=3)


def test_select__with_kwargs_no_match(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))

    rows = engine.select(Team, Team.name == "Nobody").fetchall()

    assert rows == []


def test_select__invalid_kwargs(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    with pytest.raises(AttributeError, match="doesnt_exist"):
        engine.select(Team, Team.doesnt_exist == "test")


def test_select__adhoc_model(engine: Engine) -> None:
    r = engine.select(SqliteMaster, SqliteMaster.type == 'table').fetchall()
    assert len(r) == 0
    engine.ensure_table_created(Team)
    r = engine.select(SqliteMaster, SqliteMaster.type == 'table').fetchall()
    assert len(r) == 1
    assert any(row.name == 'Team' for row in r)


def test_select__select_query_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    rows = engine.select(Team_NameUpper, order="name").fetchall()

    assert rows == [Team_NameUpper("Lions", "LIONS"), Team_NameUpper("Tigers", "TIGERS")]


def test_select__select_query_model__order_limit_offset(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))
    engine.insert(Team("Bears", 25))

    rows = engine.select(Team_NameUpper, order="name", limit=1, offset=1).fetchall()

    assert rows == [Team_NameUpper("Lions", "LIONS")]


def test_select__select_query_model__pragma(engine: Engine) -> None:
    engine.ensure_table_created(Team)

    rows = engine.select(Team_Info).fetchall()

    assert [r.name for r in rows] == ["id", "name", "size"]
    assert rows[0].pk == 1


def test_find__select_query_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))

    assert engine.find(Team_NameUpper) == Team_NameUpper("Lions", "LIONS")


def test_select__select_query_model__target(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    # target references the model's OWN output columns, not the source table.
    rows = engine.select(Team_NameUpper, Team_NameUpper.shout == "LIONS").fetchall()

    assert rows == [Team_NameUpper("Lions", "LIONS")]


def test_select__select_query_model__target_with_order_limit(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))
    engine.insert(Team("Lynx", 12))

    rows = engine.select(Team_NameUpper, t"{Team_NameUpper.name} LIKE 'L%'", order="name", limit=1).fetchall()

    assert rows == [Team_NameUpper("Lions", "LIONS")]


def test_select__select_query_model__order_tstring(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    # order references the model's OWN output columns via the wrapping CTE.
    rows = engine.select(Team_NameUpper, order=t"{Team_NameUpper.shout} DESC").fetchall()

    assert rows == [Team_NameUpper("Tigers", "TIGERS"), Team_NameUpper("Lions", "LIONS")]


def test_find__select_query_model__target(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    assert engine.find(Team_NameUpper, Team_NameUpper.name == "Tigers") == Team_NameUpper("Tigers", "TIGERS")


def test_select__select_query_model__params(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    # the `params` arg flows through to the model-bound query's named parameter
    rows = engine.select(Team_MinSize, None, {"min_size": 31}).fetchall()

    assert rows == [Team_MinSize("Tigers", 33)]


def test_find__select_query_model__params(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    assert engine.find(Team_MinSize, None, {"min_size": 31}) == Team_MinSize("Tigers", 33)


def test_select__select_query_model__target_on_aggregated_column(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 40))

    # avg(size) == 35; the field is `avg_size` even though the inner SQL names
    # the column `avg(size)`. The CTE column-name list aliases it positionally,
    # so a target on `avg_size` resolves reliably.
    assert engine.select(Team_AvgSize, Team_AvgSize.avg_size > 25).fetchall() == [Team_AvgSize(35.0)]
    assert engine.select(Team_AvgSize, Team_AvgSize.avg_size > 50).fetchall() == []


def test_find__select_query_model__target_on_aggregated_column(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 40))

    assert engine.find(Team_AvgSize, Team_AvgSize.avg_size > 25) == Team_AvgSize(35.0)


@pytest.mark.parametrize(
    ("api_call", "expected_sql_statements"),
    [
        pytest.param(
            lambda engine: engine.ensure_table_created(Team),
            (
                r"^CREATE TABLE Team \(",
                r"^SELECT sql FROM sqlite_master WHERE type='table' AND name='Team'",
            ),
            id="ensure_table_created-existing",
        ),
        pytest.param(
            lambda engine: engine.ensure_table_created(Person),
            (r"^CREATE TABLE Person \(",),
            id="ensure_table_created-create",
        ),
        pytest.param(
            lambda engine: engine.insert(Team("Lions", 30)),
            (r"^INSERT INTO Team .* RETURNING id, name, size",),
            id="insert",
        ),
        pytest.param(
            lambda engine: engine.find(Team, 1),
            (r"^SELECT .* WHERE id = 1 LIMIT 1",),
            id="find",
        ),
        pytest.param(
            lambda engine: engine.select(Team, Team.name == "Lions"),
            (r"^SELECT .* Team\.name = 'Lions'",),
            id="select",
        ),
        pytest.param(
            lambda engine: engine.select(Team_NameUpper),
            (r"^SELECT Team\.name, upper\(Team\.name\) AS shout FROM Team",),
            id="select-query-model",
        ),
        pytest.param(
            lambda engine: engine.update(Team, 1, name="Tigers"),
            (r"^UPDATE Team SET name = 'Tigers' WHERE id = 1",),
            id="update",
        ),
        pytest.param(
            lambda engine: engine.delete(Team, 1),
            (r"^DELETE FROM Team WHERE id = 1",),
            id="delete",
        ),
    ],
)
def test_engine_crud_calls__emit_only_expected_sql_statements(
    engine: Engine,
    sql_log: SqlLog,
    api_call: Any,
    expected_sql_statements: tuple[str, ...],
) -> None:
    """Keep this in sync with added engine.* api methods."""
    engine.ensure_table_created(Team)
    sql_log.clear()

    try:
        api_call(engine)
    except RecordNotFoundError:
        pass

    assert len(sql_log.entrys) == len(expected_sql_statements), sql_log.entrys
    for statement, pattern in zip(sql_log.entrys, expected_sql_statements, strict=True):
        assert re.search(pattern, re.sub(r"\s+", " ", statement).strip()), statement


def test_select__table_model__returns_cursor_proxy(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))

    cur = engine.select(Team)
    assert isinstance(cur, apsw.Cursor)

    row = cur.fetchone()
    assert isinstance(row, Team)
    assert row == Team("Lions", 30, id=1)


def test_select__adhoc_select_query_model__returns_cursor_proxy(engine: Engine) -> None:
    cur = engine.select(AdHoc_Score)
    assert isinstance(cur, apsw.Cursor)

    row = cur.fetchone()
    assert isinstance(row, AdHoc_Score)
    assert row == AdHoc_Score(7.7)


def test_find__datetime_target_is_adapted(engine: Engine) -> None:
    import datetime as dt

    class Event(TableRow):
        name: str
        happened_at: dt.datetime

    engine.ensure_table_created(Event)
    ts = dt.datetime(2024, 6, 15, 12, 0, 0)
    engine.insert(Event("launch", ts))
    engine.insert(Event("deploy", dt.datetime(2025, 1, 1, 9, 0, 0)))

    row = engine.find(Event, Event.happened_at == ts)

    assert row.name == "launch"
    assert row.happened_at == ts


def test_update__datetime_values_are_adapted(engine: Engine) -> None:
    import datetime as dt

    class Event(TableRow):
        name: str
        happened_at: dt.datetime

    engine.ensure_table_created(Event)
    old = dt.datetime(2024, 6, 15, 12, 0, 0)
    new = dt.datetime(2025, 1, 1, 9, 0, 0)
    engine.insert(Event("launch", old))

    # both the target value AND the patch value are non-native (datetime) and
    # must be adapted by the cursor on the way to SQLite.
    changes = engine.update(Event, Event.happened_at == old, happened_at=new)

    assert changes == 1
    assert engine.find(Event, Event.name == "launch").happened_at == new


def test_delete__datetime_target_value_is_adapted(engine: Engine) -> None:
    import datetime as dt

    class Event(TableRow):
        name: str
        happened_at: dt.datetime

    engine.ensure_table_created(Event)
    ts = dt.datetime(2024, 6, 15, 12, 0, 0)
    engine.insert(Event("launch", ts))
    engine.insert(Event("deploy", dt.datetime(2025, 1, 1, 9, 0, 0)))

    # the non-native target value must be adapted for the WHERE comparison.
    changes = engine.delete(Event, Event.happened_at == ts)

    assert changes == 1
    assert [r.name for r in engine.select(Event)] == ["deploy"]


def test_save__on_success__inserts_record_to_db(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0] == (row.id, "Lions", 30)


def test_save__on_success__returns_model_with_filled_in_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)

    returned_row = engine.insert(row)
    assert returned_row.id == 1

    returned_row = engine.insert(row)
    assert returned_row.id == 2


def test_save__benchmark(engine: Engine, benchmark: BenchmarkFixture) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", 30)

    def save():
        engine.insert(row)

    benchmark(save)


def test_save__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30, id=78787))


def test_save__related_model(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = engine.insert(Team("Lions", 5))
    person = engine.insert(Person("Alice", team))

    row = engine.find(Person)
    assert row is not None
    assert row == person


def test_save__unpersisted_relation__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = Team("Lions", 5)
    with pytest.raises(UnpersistedRelationshipError):
        _person = engine.insert(Person("Alice", team))


def test_save__three_model_relation_chain(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    engine.ensure_table_created(Arm)

    team = engine.insert(Team("Lions", 5))
    person = engine.insert(Person("Alice", team))
    arm = engine.insert(Arm(30.0, person))

    row = engine.find(Arm)
    assert row == arm


def test_save__null_relation(engine: Engine) -> None:
    class A(TableRow):
        pass

    class B(TableRow):
        team: A | None

    engine.ensure_table_created(A)
    engine.ensure_table_created(B)

    person = engine.insert(B(None))

    row = engine.find(B)
    assert row is not None
    assert row == person


def test_save__none_in_not_null_column__raises(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = Team("Lions", cast(Any, None))

    with pytest.raises(apsw.ConstraintError, match="NOT NULL constraint failed"):
        engine.insert(row)


def test_save__updates_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    with pytest.raises(apsw.ConstraintError):
        engine.insert(replace(row, name="Alice"))


def test_delete__by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    assert len(cursor.fetchall()) == 1

    engine.delete(Team, row.id)

    cursor.execute("SELECT * FROM Team;")
    assert len(cursor.fetchall()) == 0


def test_delete__by_row(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    cursor = engine.connection.cursor()
    cursor.execute("SELECT * FROM Team;")
    assert len(cursor.fetchall()) == 1

    engine.delete(Team, row.id)

    cursor.execute("SELECT * FROM Team;")
    assert len(cursor.fetchall()) == 0


def test_delete__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    changes = engine.delete(Team, 78787)
    assert changes == 0


def test_delete__id_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    changes = engine.delete(Team, None)
    assert changes == 0


def test_update__by_model_and_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    changes = engine.update(Team, row.id, name="Tigers")

    assert changes == 1
    assert engine.find(Team, row.id) == Team("Tigers", 30, id=row.id)


def test_update__by_instance(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    changes = engine.update(Team, row.id, name="Tigers")

    assert changes == 1
    assert engine.find(Team, row.id) == Team("Tigers", 30, id=row.id)


def test_update__multiple_fields(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))

    changes = engine.update(Team, row.id, name="Tigers", size=50)

    assert changes == 1
    assert engine.find(Team, row.id) == Team("Tigers", 50, id=row.id)


def test_update__id_none(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    changes = engine.update(Team, None, name="Tigers")
    assert changes == 0


def test_update__nonexistent_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    changes = engine.update(Team, 78787, name="Tigers")
    assert changes == 0


def test_update__no_kwargs(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))
    with pytest.raises(NoKwargFieldSpecifiedError, match="At least one field must be specified"):
        engine.update(Team, row.id)


def test_update__invalid_kwargs(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    row = engine.insert(Team("Lions", 30))
    with pytest.raises(InvalidKwargFieldSpecifiedError):
        engine.update(Team, row.id, doesnt_exist="test")


# --- t-string predicate param overrides --------------------------------------


def test_select__tstring_param_override(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))
    engine.insert(Team("Bears", 25))

    min_size = 0
    predicate = t"{Team.size} >= {min_size}"

    assert len(engine.select(Team, predicate).fetchall()) == 3
    assert len(engine.select(Team, predicate, {"min_size": 30}).fetchall()) == 2
    assert len(engine.select(Team, predicate, {"min_size": 100}).fetchall()) == 0


def test_find__tstring_param_override(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    name = "Lions"
    predicate = t"{Team.name} = {name}"

    assert engine.find(Team, predicate).name == "Lions"
    assert engine.find(Team, predicate, {"name": "Tigers"}).name == "Tigers"


def test_update__tstring_param_override(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    target_name = "Lions"
    predicate = t"{Team.name} = {target_name}"

    changes = engine.update(Team, predicate, {"target_name": "Tigers"}, size=99)
    assert changes == 1
    assert engine.find(Team, Team.name == "Tigers").size == 99
    assert engine.find(Team, Team.name == "Lions").size == 30


def test_delete__tstring_param_override(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.insert(Team("Lions", 30))
    engine.insert(Team("Tigers", 33))

    target_name = "Lions"
    predicate = t"{Team.name} = {target_name}"

    changes = engine.delete(Team, predicate, {"target_name": "Tigers"})
    assert changes == 1
    assert engine.find(Team, Team.name == "Lions") is not None
    with pytest.raises(RecordNotFoundError):
        engine.find(Team, Team.name == "Tigers")
