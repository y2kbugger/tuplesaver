from __future__ import annotations

import sqlite3
from typing import NamedTuple, assert_type

import pytest

from .cursorproxy import Lazy, TypedCursorProxy
from .engine import Engine
from .engine_test import Person, Team


class M(NamedTuple):
    id: int | None
    name: str
    age: int


sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"


@pytest.fixture
def proxy() -> TypedCursorProxy[M]:
    connection = sqlite3.connect(":memory:")
    cursor = connection.execute(sql)
    proxy = TypedCursorProxy.proxy_cursor_deep(M, cursor)
    assert_type(proxy, TypedCursorProxy[M])  # type: ignore slight bug in pyright, masked by both fixure here and engine.query in engine.py
    return proxy


def test_proxy_typehints(proxy: TypedCursorProxy[M]) -> None:
    assert_type(proxy, TypedCursorProxy[M])
    assert_type(proxy.fetchone(), M | None)
    assert_type(proxy.fetchall(), list[M])
    assert_type(proxy.fetchmany(1), list[M])
    assert_type(proxy.rowcount, int)
    assert_type(proxy.connection, sqlite3.Connection)


def test_proxy_fetchone(proxy: TypedCursorProxy[M]) -> None:
    row = proxy.fetchone()
    assert type(row) is M
    assert row == M(1, "Alice", 30)


def test_proxy_fetchall(proxy: TypedCursorProxy[M]) -> None:
    rows = proxy.fetchall()
    assert rows == [M(1, "Alice", 30), M(2, "Bob", 40)]


def test_proxy_fetchmany(proxy: TypedCursorProxy[M]) -> None:
    rows = proxy.fetchmany(1)
    assert rows == [M(1, "Alice", 30)]


def test_proxy__after_usage__rowfactory_persists(proxy: TypedCursorProxy[M]) -> None:
    row = proxy.fetchone()
    assert row == M(1, "Alice", 30)

    row = proxy.fetchone()
    assert proxy.row_factory is not None
    assert row == M(2, "Bob", 40)


def test_proxy__fetchone_returns_none(proxy: TypedCursorProxy[M]) -> None:
    proxy.fetchone()
    proxy.fetchone()
    row = proxy.fetchone()
    assert row is None


def test_proxy__after_usage__rowfactory_doesnt_leak_to_new_cursors() -> None:
    connection = sqlite3.connect(":memory:")

    # Proxy a cursor with custom row_factory
    cursor = connection.cursor()
    proxy = TypedCursorProxy.proxy_cursor_deep(M, cursor)
    assert proxy.row_factory is not None

    # new cursors must come back completely standard
    new_cursor = connection.cursor()
    assert new_cursor.row_factory is None


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
    m1 = M(1, "Alice", 30)
    m1 = M(1, "Alice", 30)
    assert l1 == m1
    assert m1 == l1


def test_lazy__ids_different__are_not_equal() -> None:
    l1 = Lazy(None, M, 1)  # type: ignore
    m1 = M(2, "Bob", 40)
    assert l1 != m1
    assert m1 != l1


def test_lazy__models_different__are_not_equal() -> None:
    class N(NamedTuple):
        id: int | None

    l1 = Lazy(None, M, 1)  # type: ignore
    l2 = Lazy(None, N, 1)  # type: ignore
    assert l1 != l2
    assert l2 != l1


def test_lazy__lazymodel_and_model_different__are_not_equal() -> None:
    class N(NamedTuple):
        id: int | None

    l1 = Lazy(None, M, 1)  # type: ignore
    n1 = N(1)

    assert l1 != n1
    assert n1 != l1


def test_proxy__relations_can_be_fetched_eagerly(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    team = engine.save(Team(None, "Team A", 5))
    person = engine.save(Person(None, "Alice", team))

    found_person = engine.find(Person, person.id, deep=True)

    assert found_person == person
    assert isinstance(found_person[2], Team)
    assert isinstance(found_person.team, Team)
    assert found_person.team == team


def test_proxy__relations_can_be_fetched_lazily(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    team = engine.save(Team(None, "Team A", 5))
    person = engine.save(Person(None, "Alice", team))

    found_person = engine.find(Person, person.id, deep=False)

    assert found_person == person  # equal despite not having relationship loaded
    assert isinstance(found_person[2], Lazy)
    assert isinstance(found_person.team, Team)
    assert found_person.team.id == team.id
    assert found_person.team.name == team.name
    assert found_person == person  # Still equal after lazy loading


def test_proxy__lazy_and_eager__are_equal(engine: Engine) -> None:
    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)
    person = engine.save(Person(None, "Alice", Team(None, "Team A", 5)), deep=True)

    found_person_lazy = engine.find(Person, person.id, deep=False)
    found_person_eager = engine.find(Person, person.id, deep=True)

    assert found_person_lazy == found_person_eager
    assert found_person_eager == found_person_lazy


def test_proxy__deep_regression_case__doesnt_fail(engine: Engine) -> None:
    """This was a real bug where the proxy cursor row factory reused the
    same outer cursor and through away subsequent results."""

    engine.ensure_table_created(Team)
    engine.ensure_table_created(Person)

    team = engine.save(Team(None, "Team A", 5))
    person1 = engine.save(Person(None, "Alice", team))
    person2 = engine.save(Person(None, "Bob", team))

    rows = engine.query(Person, "SELECT * FROM Person;", deep=True).fetchall()

    assert rows == [person1, person2]


def test_proxy__when_querying_view_model__does_not_register_as_table_model(engine: Engine) -> None:
    class ModelA(NamedTuple):
        name: str

    from .model import is_registered_row_model, is_registered_table_model

    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    cur = engine.query(ModelA, "SELECT 'Alice' as name;")

    # I don't care either way, just documenring current behavior
    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    cur.fetchone()

    assert is_registered_row_model(ModelA) is True
    assert is_registered_table_model(ModelA) is False
