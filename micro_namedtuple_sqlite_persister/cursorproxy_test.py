from __future__ import annotations

import sqlite3
from typing import NamedTuple, assert_type

import pytest

from .cursorproxy import TypedCursorProxy


class M(NamedTuple):
    id: int | None
    name: str
    age: int


sql = "SELECT 1 as id, 'Alice' as name, 30 as age UNION SELECT 2, 'Bob', 40;"


@pytest.fixture
def proxy() -> TypedCursorProxy[M]:
    connection = sqlite3.connect(":memory:")
    cursor = connection.execute(sql)
    proxy = TypedCursorProxy.proxy_cursor(M, cursor)
    assert_type(proxy, TypedCursorProxy[M])  # type: ignore slight bug in pyright, masked by both fixure here and engine.query in persister.py
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


def test_proxy__after_usage__rowfactory_doesnt_leak_to_new_cursors() -> None:
    connection = sqlite3.connect(":memory:")

    # Proxy a cursor with custom row_factory
    cursor = connection.cursor()
    proxy = TypedCursorProxy.proxy_cursor(M, cursor)
    assert proxy.row_factory is not None

    # new cursors must come back completely standard
    new_cursor = connection.cursor()
    assert new_cursor.row_factory is None
