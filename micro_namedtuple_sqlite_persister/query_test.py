import datetime as dt
from textwrap import dedent
from typing import NamedTuple

import pytest

from .query import CSV, _select, and_, eq, get_column_name, get_field_idx, get_table_name, gt, gte, lt, lte, ne, or_, select


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
    simple = _select(MyModel)
    assert simple == ('SELECT', CSV(('id', 'name', 'date')), 'FROM', MyModel)


def test_select_with_where_clause() -> None:
    with_where = _select(MyModel, where=eq(MyModel.name, "Apple"))
    assert with_where == (
        'SELECT',
        CSV(('id', 'name', 'date')),
        'FROM',
        MyModel,
        ('WHERE', (MyModel.name, '=', 'Apple')),
    )


def test_select_with_complex_where_clause() -> None:
    complex_query = _select(
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
    limited = _select(MyModel, limit=10)
    assert limited == ('SELECT', CSV(('id', 'name', 'date')), 'FROM', MyModel, ('LIMIT', 10))


def test_select_with_limit_and_where() -> None:
    limited_with_where = _select(MyModel, where=eq(MyModel.name, "Apple"), limit=5)
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
    _, sql = select(MyModel)
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
    """)


def test_render_query_with_where() -> None:
    """Test rendering a query with WHERE clause"""
    _, sql = select(MyModel, where=eq(MyModel.name, "Apple"))
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (MyModel.name = 'Apple')
    """)


def test_render_query_with_complex_where() -> None:
    """Test rendering a query with complex WHERE clause"""
    Model, sql = select(
        MyModel,
        where=(
            or_(
                eq(MyModel.name, "Apple"),
                eq(MyModel.id, 42),
            ),
        ),
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE ((MyModel.name = 'Apple') OR (MyModel.id = 42))
    """)


def test_render_query_with_where_and_limit() -> None:
    """Test rendering a query with WHERE clause and LIMIT"""
    Model, sql = select(
        MyModel,
        where=eq(MyModel.name, "Apple"),
        limit=5,
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (MyModel.name = 'Apple')
        LIMIT 5
    """)


def test_render_query_with_order_by() -> None:
    """Test rendering a query with ORDER BY clause"""
    Model, sql = select(
        MyModel,
        order_by=(MyModel.id,),
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        ORDER BY MyModel.id
    """)


def test_render_query_with_and() -> None:
    """Test rendering a query with AND condition"""
    Model, sql = select(
        MyModel,
        where=and_(
            eq(MyModel.name, "Apple"),
            eq(MyModel.id, 42),
        ),
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE ((MyModel.name = 'Apple') AND (MyModel.id = 42))
    """)


def test_render_query_with_complex_and_or() -> None:
    """Test rendering a query with complex AND and OR conditions"""
    Model, sql = select(
        MyModel,
        where=or_(
            and_(
                eq(MyModel.name, "Apple"),
                eq(MyModel.id, 42),
            ),
            eq(MyModel.name, "Banana"),
        ),
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (((MyModel.name = 'Apple') AND (MyModel.id = 42)) OR (MyModel.name = 'Banana'))
    """)


@pytest.mark.xfail(reason="Not implemented yet")
def test_supra_binary_logical_operators() -> None:
    """Test all supra binary logical operators"""
    assert or_(1, 2, 3) == (1, 'OR', 2, 'OR', 3)  # type: ignore


@pytest.mark.xfail(reason="Agg Not implmenented yet")
def test_render_query_with_agg_functions() -> None:
    M, q = select((avg(MyModel.score), count(MyModel)))  # type: ignore  # noqa: F821
    assert MyModel == M
    assert q == dd("""
        SELECT AVG(score), COUNT(*)
        FROM MyModel
    """)


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
        _, sql = select(MyModel, where=condition)
        assert sql == dd(f"""
            SELECT id, name, date
            FROM MyModel
            {expected_where}
        """)


def test_render_query_with_complex_comparisons() -> None:
    """Test rendering a query with multiple comparison types"""
    Model, sql = select(
        MyModel,
        where=and_(
            and_(
                gt(MyModel.id, 42),
                lt(MyModel.id, 100),
            ),
            ne(MyModel.name, "Test"),
        ),
    )

    assert Model == MyModel
    assert sql == dd("""
        SELECT id, name, date
        FROM MyModel
        WHERE (((MyModel.id > 42) AND (MyModel.id < 100)) AND (MyModel.name != 'Test'))
    """)
