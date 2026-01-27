from __future__ import annotations

from typing import NamedTuple, assert_type

import apsw
import pytest

from .cursorproxy import Lazy, TableRow, TypedCursorProxy
from .engine import Engine
from .engine_test import Person, Team
from .model import Row


class M(TableRow):
    name: str
    age: int


sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"


@pytest.fixture
def proxy(engine: Engine) -> TypedCursorProxy[M]:
    connection = engine.connection
    cursor = connection.execute(sql)
    proxy = TypedCursorProxy.proxy_cursor_lazy(M, cursor, engine)
    assert_type(proxy, TypedCursorProxy[M])  # type: ignore slight bug in pyright, masked by both fixure here and engine.query in engine.py
    return proxy


def test_proxy_typehints(proxy: TypedCursorProxy[M]) -> None:
    assert_type(proxy, TypedCursorProxy[M])
    assert_type(proxy.connection, apsw.Connection)

    assert_type(proxy.fetchone(), M | None)
    assert_type(proxy.fetchall(), list[M])
    assert_type(proxy.connection.changes(), int)
    assert_type(proxy.sql, str)
    assert_type(proxy.is_explain, int)
    assert_type(proxy.has_vdbe, bool)
    assert_type(proxy.expanded_sql, str)


def test_proxy_fetchone(proxy: TypedCursorProxy[M]) -> None:
    row = proxy.fetchone()
    assert type(row) is M
    assert row == M("Alice", 30, id=1)


def test_proxy_fetchall(proxy: TypedCursorProxy[M]) -> None:
    rows = proxy.fetchall()
    assert rows == [M("Alice", 30, id=1), M("Bob", 40, id=2)]


def test_proxy__after_usage__rowfactory_persists(proxy: TypedCursorProxy[M]) -> None:
    row = proxy.fetchone()
    assert row == M("Alice", 30, id=1)

    row = proxy.fetchone()
    assert proxy.row_trace is not None
    assert row == M("Bob", 40, id=2)


def test_proxy__fetchone_returns_none(proxy: TypedCursorProxy[M]) -> None:
    proxy.fetchone()
    proxy.fetchone()
    row = proxy.fetchone()
    assert row is None


def test_proxy__after_usage__rowfactory_doesnt_leak_to_new_cursors(engine: Engine) -> None:
    # Proxy a cursor with custom row_factory
    cursor = engine.connection.cursor()
    proxy = TypedCursorProxy.proxy_cursor_lazy(M, cursor, engine)
    assert cursor.row_trace is proxy.row_trace

    # new cursors must come back completely standard
    new_cursor = engine.connection.cursor()
    assert new_cursor.row_trace is not proxy.row_trace
    assert new_cursor.row_trace is not cursor.row_trace


def test_lazy__same_model_and_id__are_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore engine is not needed for this test
    l2 = Lazy(None, M, 1)  # type: ignore
    assert l1 == l2


def test_lazy__different_model_and_id__are_not_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore
    l2 = Lazy(None, M, 2)  # type: ignore
    assert l1 != l2

    l3 = Lazy(None, M, 1)  # type: ignore
    l4 = Lazy(None, str, 1)  # type: ignore
    assert l3 != l4


def test_lazy__lazyid_and_int_same__are_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore
    assert l1 == 1
    assert 1 == l1  # noqa: SIM300
    assert l1 != 2
    assert 2 != l1  # noqa: SIM300


def test_lazy__ids_same__are_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore
    m1 = M("Alice", 30, id=1)
    m1 = M("Alice", 30, id=1)
    assert l1 == m1
    assert m1 == l1


def test_lazy__ids_different__are_not_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore
    m1 = M("Bob", 40, id=2)
    assert l1 != m1
    assert m1 != l1


def test_lazy__models_different__are_not_equal() -> None:
    class N(NamedTuple):
        pass

    l1 = Lazy(None, M, 1)  # type: ignore
    l2 = Lazy(None, N, 1)  # type: ignore
    assert l1 != l2
    assert l2 != l1


def test_lazy__lazymodel_and_model_different__are_not_equal() -> None:
    class N(NamedTuple):
        id: int

    l1 = Lazy(None, M, 1)  # type: ignore
    n1 = N(1)

    assert l1 != n1
    assert n1 != l1


def test_proxy__relations_can_be_fetched_lazily(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    team = engine.save(Team("Team A", 5))
    person = engine.save(Person("Alice", team))

    found_person = engine.find(Person, person.id)

    assert found_person == person  # equal despite not having relationship loaded
    # Access raw field value (before Lazy is unwrapped) via object.__getattribute__
    assert isinstance(object.__getattribute__(found_person, 'team'), Lazy)
    assert isinstance(found_person.team, Team)
    assert found_person.team.id == team.id
    assert found_person.team.name == team.name
    assert found_person == person  # Still equal after lazy loading


def test_proxy__lazy_relations__equal_by_id(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    team = engine.save(Team("Team A", 5))
    person = engine.save(Person("Alice", team))

    found_person1 = engine.find(Person, person.id)
    found_person2 = engine.find(Person, person.id)

    assert found_person1 == found_person2
    assert found_person2 == found_person1


def test_proxy__lazy_query__multiple_rows__doesnt_fail(engine: Engine) -> None:
    """This was a real bug where the proxy cursor row factory reused the
    same outer cursor and through away subsequent results."""

    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = engine.save(Team("Team A", 5))
    person1 = engine.save(Person("Alice", team))
    person2 = engine.save(Person("Bob", team))

    rows = engine.query(Person, "SELECT * FROM Person;").fetchall()

    assert rows == [person1, person2]


def test_proxy__when_querying_view_model__does_not_register_as_table_model(engine: Engine) -> None:
    class ModelA(Row):
        name: str

    from .model import is_row_model

    assert is_row_model(ModelA) is False

    cur = engine.query(ModelA, "SELECT 'Alice' as name;")

    assert is_row_model(ModelA) is False

    cur.fetchone()

    assert is_row_model(ModelA) is False
