from __future__ import annotations

import logging
import os
import re
import sqlite3
from collections.abc import Sequence
from typing import Any, overload

from .cursorproxy import TypedCursorProxy

# NOTE: engine.py should only know about .model, but not .query
from .model import (
    Row,
    get_meta,
    is_row_model,
)
from .sql import (
    generate_create_table_ddl,
    generate_delete_sql,
    generate_insert_sql,
    generate_select_by_field_sql,
    generate_update_sql,
)

logger = logging.getLogger(__name__)


class TableSchemaMismatch(Exception):
    def __init__(self, table_name: str, existing_table_schema: str, new_table_schema: str) -> None:
        super().__init__(
            f"Table `{table_name}` already exists but the schema does not match the expected schema.\nExisting schema:\n\t{existing_table_schema}.\nExpected schema:\n\t{new_table_schema}"
        )


class UnpersistedRelationshipError(Exception):
    def __init__(self, model_name: str, field_name: str, row: Row) -> None:
        super().__init__(self, f"Cannot save {model_name} with unpersisted {model_name}.{field_name} of row {row}")


class LookupByAdHocModelImpossible(Exception):
    def __init__(self, model_name: str) -> None:
        super().__init__(f"Cannot lookup via adhoc model: `{model_name}`. Only table or alt models can be used for lookups.")


class NoKwargFieldSpecifiedError(ValueError):
    def __init__(self) -> None:
        super().__init__("At least one field must be specified to find a row.")


class InvalidKwargFieldSpecifiedError(ValueError):
    def __init__(self, Model: type[Row], kwargs: dict[str, Any]) -> None:
        super().__init__(f"Invalid fields for {Model.__name__}: {', '.join(kwargs.keys())}. Valid fields are: {', '.join(f.name for f in get_meta(Model).fields)}")


class IdNoneError(ValueError):
    pass


class MatchNotFoundError(ValueError):
    pass


class Engine:
    def __init__(self, db_path: str | os.PathLike[str]) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL")

    def ensure_table_created(self, Model: type[Row]) -> None:
        meta = get_meta(Model)
        ddl = generate_create_table_ddl(Model)

        try:
            self.connection.execute(ddl)
        except sqlite3.OperationalError as e:
            assert meta.table_name is not None, "Table name must be defined for the model to create it."
            if f"table {meta.table_name} already exists" in str(e):
                # Check existing table, it might be ok
                def _normalize_whitespace(s: str) -> str:
                    return re.sub(r'\s+', ' ', s).strip()

                def _get_sql_for_existing_table(table_name: str) -> str:
                    query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                    cursor = self.connection.execute(query)
                    return cursor.fetchone()[0]

                existing_table_schema = _normalize_whitespace(_get_sql_for_existing_table(meta.table_name))
                new_table_schema = _normalize_whitespace(ddl)

                if existing_table_schema != new_table_schema:
                    raise TableSchemaMismatch(meta.table_name, existing_table_schema, new_table_schema) from e
            else:
                # error is not about the table already existing
                raise
        except Exception as e:
            raise e

        sqlite3.register_adapter(Model, lambda row: row[0])  # insert Model as its id when used as FK related field in other tables

    ##### Reading
    def find[R: Row](self, Model: type[R], row_id: int | None, *, deep: bool = False) -> R:
        """Find a row by its id. This is a special case of find_by."""
        if row_id is None:
            raise IdNoneError("Cannot SELECT, id=None")

        return self.find_by(Model, deep=deep, id=row_id)

    def find_by[R: Row](self, Model: type[R], *, deep: bool = False, **kwargs: Any) -> R:
        """Find a row by its fields, e.g. `find_by(Model, name="Alice")`"""

        if not kwargs:
            raise NoKwargFieldSpecifiedError()

        if not is_row_model(Model):
            raise LookupByAdHocModelImpossible(Model.__name__)

        meta = get_meta(Model)

        field_names = [f.name for f in meta.fields]
        if not all(k in field_names for k in kwargs):
            raise InvalidKwargFieldSpecifiedError(Model, kwargs)

        # Use cached query generation
        sql = generate_select_by_field_sql(Model, frozenset(kwargs.keys()))

        row = self.query(Model, sql, kwargs, deep=deep).fetchone()

        if row is None:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
            raise MatchNotFoundError(f"Cannot SELECT, no row with {kwargs_str} in table `{Model.__name__}`")

        return row

    def query[R: Row](self, Model: type[R], sql: str, parameters: Sequence | dict = tuple(), *, deep: bool = False) -> TypedCursorProxy[R]:
        cursor = self.connection.execute(sql, parameters)
        if deep:
            return TypedCursorProxy.proxy_cursor_deep(Model, cursor)
        else:
            return TypedCursorProxy.proxy_cursor_lazy(Model, cursor, self)

    #### Writing
    def save[R: Row](self, row: R, *, deep: bool = False) -> R:
        """insert or update records, based on the presence of an id"""

        if deep:
            # If deep is True, we save all related rows recursively
            row = row._make(self.save(f, deep=deep) if is_row_model(type(f)) else f for f in row)
        else:
            # Don't allow saving if a related row is not persisted
            if any(related_row.id is None for related_row in row[1:] if is_row_model(related_row.__class__)):
                raise UnpersistedRelationshipError(type(row).__name__, "related row", row)

        if row[0] is None:
            insert = generate_insert_sql(type(row))
            cur = self.connection.execute(insert, row._asdict())
            return row._replace(id=cur.lastrowid)
        else:
            update = generate_update_sql(type(row))
            cur = self.connection.execute(update, row._asdict())
            if cur.rowcount == 0:
                raise MatchNotFoundError(f"Cannot UPDATE, no row with id={row[0]} in table `{row.__class__.__name__}`")
            return row

    @overload
    def delete(self, Model: type[Row], row_id: int | None) -> None: ...

    @overload
    def delete(self, row: Row) -> None: ...

    def delete(self, Model_or_row: type[Row] | Row, row_id: int | None = None) -> None:  # pyright: ignore [reportInconsistentOverload] allow overloads with different parameter names
        if not isinstance(Model_or_row, type):
            row = Model_or_row
            Model = row.__class__
            assert row_id is None, "Do not provide row_id when passing a row instance."
            row_id = row[0]
        else:
            Model = Model_or_row

        if row_id is None:
            raise IdNoneError("Cannot DELETE, id=None")
        query = generate_delete_sql(Model)
        cur = self.connection.execute(query, {'id': row_id})
        if cur.rowcount == 0:
            raise MatchNotFoundError(f"Cannot DELETE, no row with id={row_id} in table `{Model.__name__}`")
