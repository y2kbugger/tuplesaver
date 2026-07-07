"""Whole-statement SQL generation.

This module owns CREATE/SELECT/INSERT/UPDATE/DELETE templates built from
compiled model class attributes, as well as assembly of full statements that
splice in WHERE clauses produced by `sql_rel`. Predicate AST lowering
itself stays in `sql_rel.py`.
"""

from functools import cache
from string.templatelib import Template
from textwrap import dedent
from typing import Any

# NOTE: sql.py should only depend on .model and .sql_rel — never .engine or .query.
from .model import RowMeta, TableRow, has_select_query
from .sql_rel import compile_expr

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _append_where(sql: str, target: Any, params: dict[str, Any], start_idx: int = 0) -> tuple[str, int]:
    """Append a `WHERE <clause>` compiled from `target` to `sql`, mutating `params`.

    `start_idx` seeds the generated `p<idx>` parameter names so a wrapped
    `__select_query__` can avoid colliding with the inner query's bound params.
    Returns the SQL and the next free `p<idx>` index for further clauses.
    """
    if target is None:
        return sql, start_idx
    where_clause, next_idx = compile_expr(target, params, start_idx)
    if where_clause:
        sql += f"\nWHERE {where_clause}"
    return sql, next_idx


@cache
def _select_clause(Model: RowMeta) -> str:
    table = Model.__tablename__
    cols = ", ".join(f"{table}.{f.name}" for f in Model.__fields__)
    return f"SELECT {cols} FROM {table}"


@cache
def _update_clause(Model: type[TableRow], field_names: frozenset[str]) -> str:
    sets = ", ".join(f"{name} = :{name}" for name in field_names)
    return f"UPDATE {Model.__tablename__} SET {sets}"


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------


@cache
def build_create_table_sql(Model: type[TableRow]) -> str:
    """`CREATE TABLE` DDL for a table model."""
    cols = ", ".join(f.sql_columndef for f in Model.__fields__)
    return dedent(f"""
        CREATE TABLE {Model.__tablename__} (
        {cols}
        )""").strip()


def build_sqlite_schema_query(table_name: str) -> str:
    """Query `sqlite_master` for the stored `CREATE TABLE` text of `table_name`."""
    return f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"


# ---------------------------------------------------------------------------
# SELECT
# ---------------------------------------------------------------------------


def build_select_sql(
    Model: RowMeta,
    target: Any = None,
    params: dict[str, Any] | None = None,
    *,
    order: str | Template | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """Assemble a full `SELECT` for `Model`."""
    if params is None:
        params = {}

    # Add WHERE clause
    if has_select_query(Model):
        # Wrap the bound `__select_query__` in a CTE, to allow further filtering
        base, bound, next_idx = _lower_select_query(Model)
        params.update(bound)
        if target is None and order is None and limit is None and offset is None:
            return base
        alias = Model.__tablename__
        cols = ", ".join(f.name for f in Model.__fields__)
        sql, next_idx = _append_where(f"WITH {alias}({cols}) AS (\n{base}\n)\nSELECT * FROM {alias}", target, params, next_idx)
    else:
        sql, next_idx = _append_where(_select_clause(Model), target, params)

    separator = "\n" if "\n" in sql else " "
    if order:
        # A t-string order lowers field interpolations to qualified column refs
        # (or scalar subqueries for FK paths), keeping it refactorable.
        order_sql = order if isinstance(order, str) else compile_expr(order, params, next_idx)[0]
        sql += f"{separator}ORDER BY {order_sql}"
        separator = "\n"
    if limit is not None:
        sql += f"{separator}LIMIT {limit}"
        separator = "\n"
    if offset is not None:
        sql += f"{separator}OFFSET {offset}"
    return sql


@cache
def _lower_select_query(Model: RowMeta) -> tuple[str, tuple[tuple[str, Any], ...], int]:
    """Lower a model's `__select_query__` (t-string or plain str) to SQL + bound params.

    Returns the SQL text, the bound params as items, and the next free
    `p<idx>` parameter index (so a wrapping WHERE can avoid name collisions).

    This is a good caching point, since the bound SQL is reusable across all calls to `find`/`select`
    """
    raw = Model.__select_query__
    # PEP 750 t-string: lower model/field interpolations to SQL via sql_rel.
    if hasattr(raw, "interpolations") and hasattr(raw, "strings"):
        bound: dict[str, Any] = {}
        sql, next_idx = compile_expr(raw, bound)
        return dedent(sql).strip(), tuple(bound.items()), next_idx
    return dedent(str(raw)).strip(), (), 0


# ---------------------------------------------------------------------------
# INSERT
# ---------------------------------------------------------------------------


@cache
def build_insert_sql(Model: type[TableRow]) -> str:
    """`INSERT INTO ... RETURNING ...` for every field of `Model`."""
    names = [f.name for f in Model.__fields__]
    cols = ", ".join(names)
    placeholders = ", ".join(f":{name}" for name in names)
    return dedent(f"""
        INSERT INTO {Model.__tablename__} (
            {cols}
        ) VALUES (
            {placeholders}
        )
        RETURNING {cols}
        """).strip()


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def build_update_sql(
    Model: type[TableRow],
    target: Any,
    params: dict[str, Any],
    field_names: frozenset[str],
) -> str:
    """Assemble a full `UPDATE` setting `field_names`, with WHERE compiled from `target`."""
    return _append_where(_update_clause(Model, field_names), target, params)[0]


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def build_delete_sql(
    Model: type[TableRow],
    target: Any,
    params: dict[str, Any],
) -> str:
    """Assemble a full `DELETE` for `Model`, with WHERE compiled from `target`."""
    return _append_where(f"DELETE FROM {Model.__tablename__}", target, params)[0]
