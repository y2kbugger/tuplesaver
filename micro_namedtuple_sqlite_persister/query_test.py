import datetime as dt
from textwrap import dedent
from typing import NamedTuple

import pytest

from .query import CSV, and_, eq, get_column_name, get_field_idx, get_table_name, gt, gte, lt, lte, ne, or_, render_query, select


class MyModel(NamedTuple):
    id: int | None
    name: str
    date: dt.datetime


####
# Helper Function Tests


def test_get_field_idx() -> None:
    assert get_field_idx(MyModel.id) == 0
    assert get_field_idx(MyModel.name) == 1
    assert get_field_idx(MyModel.date) == 2


def test_get_column_name() -> None:
    assert get_column_name(MyModel, 0) == 'MyModel.id'
    assert get_column_name(MyModel, 1) == 'MyModel.name'
    assert get_column_name(MyModel, 2) == 'MyModel.date'


def test_get_table_name() -> None:
    assert get_table_name(MyModel) == 'MyModel'


####
# Query DSL Tests


def test_eq() -> None:
    assert eq(MyModel.name, "Starfruit") == (MyModel.name, '=', 'Starfruit')
    assert eq(MyModel.id, 42) == (MyModel.id, '=', 42)


def test_select() -> None:
    simple = select(MyModel)
    assert simple == ('SELECT', CSV(('id', 'name', 'date')), 'FROM', MyModel)


def test_select_with_where_clause() -> None:
    with_where = select(MyModel, where=eq(MyModel.name, "Apple"))
    assert with_where == (
        'SELECT',
        CSV(('id', 'name', 'date')),
        'FROM',
        MyModel,
        ('WHERE', (MyModel.name, '=', 'Apple')),
    )


def test_select_with_complex_where_clause() -> None:
    complex_query = select(
        MyModel,
        where=or_(
            eq(MyModel.name, "Apple"),
            eq(MyModel.id, 42),
        ),
    )
    assert complex_query == (
        'SELECT',
        CSV(('id', 'name', 'date')),
        'FROM',
        MyModel,
        (
            'WHERE',
            (
                (MyModel.name, '=', 'Apple'),
                'OR',
                (MyModel.id, '=', 42),
            ),
        ),
    )


def test_select_with_limit() -> None:
    """Test SELECT with LIMIT clause"""
    limited = select(MyModel, limit=10)
    assert limited == ('SELECT', CSV(('id', 'name', 'date')), 'FROM', MyModel, ('LIMIT', 10))


def test_select_with_limit_and_where() -> None:
    limited_with_where = select(MyModel, where=eq(MyModel.name, "Apple"), limit=5)
    assert limited_with_where == (
        'SELECT',
        CSV(('id', 'name', 'date')),
        'FROM',
        MyModel,
        (
            'WHERE',
            (MyModel.name, '=', 'Apple'),
        ),
        ('LIMIT', 5),
    )


def test_or() -> None:
    """Test OR conditions"""
    condition1 = eq(MyModel.name, "Apple")
    condition2 = eq(MyModel.id, 42)

    result = or_(condition1, condition2)

    assert result == (
        (MyModel.name, '=', 'Apple'),
        'OR',
        (MyModel.id, '=', 42),
    )


def test_and() -> None:
    """Test AND conditions"""
    condition1 = eq(MyModel.name, "Apple")
    condition2 = eq(MyModel.id, 42)

    result = and_(condition1, condition2)

    assert result == (
        (MyModel.name, '=', 'Apple'),
        'AND',
        (MyModel.id, '=', 42),
    )


def dd(sql: str) -> str:
    return dedent(sql).strip()


def test_render_query_simple() -> None:
    """Test rendering a simple query"""
    query = select(MyModel)
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
    """)


def test_render_query_with_where() -> None:
    """Test rendering a query with WHERE clause"""
    query = select(MyModel, where=eq(MyModel.name, "Apple"))
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (MyModel.name = Apple)
    """)


def test_render_query_with_complex_where() -> None:
    """Test rendering a query with complex WHERE clause"""
    query = select(
        MyModel,
        where=(
            or_(
                eq(MyModel.name, "Apple"),
                eq(MyModel.id, 42),
            ),
        ),
    )
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE ((MyModel.name = Apple) OR (MyModel.id = 42))
    """)


def test_render_query_with_where_and_limit() -> None:
    """Test rendering a query with WHERE clause and LIMIT"""
    query = (
        select(
            MyModel,
            where=eq(MyModel.name, "Apple"),
            limit=5,
        ),
    )
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (MyModel.name = Apple)
        LIMIT 5
    """)


def test_render_query_with_and() -> None:
    """Test rendering a query with AND condition"""
    query = select(
        MyModel,
        where=and_(
            eq(MyModel.name, "Apple"),
            eq(MyModel.id, 42),
        ),
    )
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE ((MyModel.name = Apple) AND (MyModel.id = 42))
    """)


def test_render_query_with_complex_and_or() -> None:
    """Test rendering a query with complex AND and OR conditions"""
    query = select(
        MyModel,
        where=or_(
            and_(
                eq(MyModel.name, "Apple"),
                eq(MyModel.id, 42),
            ),
            eq(MyModel.name, "Banana"),
        ),
    )
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (((MyModel.name = Apple) AND (MyModel.id = 42)) OR (MyModel.name = Banana))
    """)


@pytest.mark.skip(reason="Not implemented yet")
def test_supra_binary_logical_operators() -> None:
    """Test all supra binary logical operators"""
    assert or_(1, 2, 3) == (1, 'OR', 2, 'OR', 3)  # type: ignore


def test_comparison_operators() -> None:
    """Test all comparison operators"""
    assert gt(MyModel.id, 42) == (MyModel.id, '>', 42)
    assert lt(MyModel.id, 42) == (MyModel.id, '<', 42)
    assert gte(MyModel.id, 42) == (MyModel.id, '>=', 42)
    assert lte(MyModel.id, 42) == (MyModel.id, '<=', 42)
    assert ne(MyModel.id, 42) == (MyModel.id, '!=', 42)


def test_render_query_with_comparisons() -> None:
    """Test rendering queries with different comparison operators"""
    tests = [
        (gt(MyModel.id, 42), "WHERE (MyModel.id > 42)"),
        (lt(MyModel.id, 42), "WHERE (MyModel.id < 42)"),
        (gte(MyModel.id, 42), "WHERE (MyModel.id >= 42)"),
        (lte(MyModel.id, 42), "WHERE (MyModel.id <= 42)"),
        (ne(MyModel.id, 42), "WHERE (MyModel.id != 42)"),
    ]

    for condition, expected_where in tests:
        query = select(MyModel, where=condition)
        assert render_query(query) == dd(f"""
            SELECT id, name, date
            FROM MyModel
            {expected_where}
        """)


def test_render_query_with_complex_comparisons() -> None:
    """Test rendering a query with multiple comparison types"""
    query = select(
        MyModel,
        where=and_(
            and_(
                gt(MyModel.id, 42),
                lt(MyModel.id, 100),
            ),
            ne(MyModel.name, "Test"),
        ),
    )
    assert render_query(query) == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (((MyModel.id > 42) AND (MyModel.id < 100)) AND (MyModel.name != Test))
    """)
