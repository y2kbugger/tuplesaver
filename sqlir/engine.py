import logging
import re
from collections.abc import Sequence
from string.templatelib import Template
from typing import Any

import apsw

from .adaptconvert import AdaptingCursor
from .cursorproxy import TypedCursorProxy

# NOTE: engine.py should only know about .model, but not .query
from .model import (
    Row,
    TableRow,
    is_tablerow_model,
)
from .sql import (
    build_create_table_sql,
    build_delete_sql,
    build_insert_sql,
    build_select_sql,
    build_sqlite_schema_query,
    build_update_sql,
)

logger = logging.getLogger(__name__)


class TableSchemaMismatch(Exception):
    def __init__(self, table_name: str, existing_table_schema: str, new_table_schema: str) -> None:
        super().__init__(
            f"Table `{table_name}` already exists but the schema does not match the expected schema.\nExisting schema:\n\t{existing_table_schema}.\nExpected schema:\n\t{new_table_schema}"
        )


class UnpersistedRelationshipError(Exception):
    def __init__(self, model_name: str, field_name: str, row: TableRow) -> None:
        super().__init__(self, f"Cannot save {model_name} with unpersisted {model_name}.{field_name} of row {row}")


class LookupByAdHocModelImpossible(Exception):
    def __init__(self, model_name: str) -> None:
        super().__init__(f"Cannot lookup via adhoc model: `{model_name}`. Only table or alt models can be used for lookups.")


class NoKwargFieldSpecifiedError(ValueError):
    def __init__(self) -> None:
        super().__init__("At least one field must be specified to find a row.")


class InvalidKwargFieldSpecifiedError(ValueError):
    def __init__(self, Model: type[TableRow], kwargs: dict[str, Any]) -> None:
        super().__init__(f"Invalid fields for {Model.__name__}: {', '.join(kwargs.keys())}. Valid fields are: {', '.join(f.name for f in Model.__fields__)}")


class IdNoneError(ValueError):
    pass


class RecordNotFoundError(ValueError):
    def __init__(self, model_name: str, target: Any) -> None:
        super().__init__(f"No row found for {model_name} matching {target}")


class NoRecordToUpdateError(ValueError):
    pass


class NoRecordToDeleteError(ValueError):
    pass


__all_errors__ = [
    TableSchemaMismatch,
    UnpersistedRelationshipError,
    LookupByAdHocModelImpossible,
    NoKwargFieldSpecifiedError,
    InvalidKwargFieldSpecifiedError,
    IdNoneError,
    RecordNotFoundError,
    NoRecordToUpdateError,
    NoRecordToDeleteError,
]


class Engine:
    def __init__(self, connection: apsw.Connection) -> None:
        self.connection = connection
        self.connection.cursor_factory = AdaptingCursor

    def ensure_table_created(self, Model: type[TableRow]) -> None:
        assert is_tablerow_model(Model), f"Model `{Model.__name__}` is not a valid table model."
        table_name = Model.__tablename__

        ddl = build_create_table_sql(Model)

        try:
            self.connection.execute(ddl)
        except apsw.SQLError as e:
            if f"table {table_name} already exists" in str(e):
                # Check existing table, it might be ok
                def _normalize_whitespace(s: str) -> str:
                    return re.sub(r'\s+', ' ', s).strip()

                def _get_sql_for_existing_table(table_name: str) -> str:
                    # TODO: is there a apsw method for this?
                    query = build_sqlite_schema_query(table_name)
                    cursor = self.connection.execute(query)
                    result = cursor.fetchone()
                    cursor.close()
                    assert result is not None, f"Table {table_name} not found in sqlite_master"
                    return result[0]

                existing_table_schema = _normalize_whitespace(_get_sql_for_existing_table(table_name))
                new_table_schema = _normalize_whitespace(ddl)

                if existing_table_schema != new_table_schema:
                    raise TableSchemaMismatch(table_name, existing_table_schema, new_table_schema) from e
            else:
                # error is not about the table already existing
                raise
        except Exception as e:
            raise e

    ##### Reading
    def find[R: Row | TableRow](self, Model: type[R], target: Any = None, params: dict[str, Any] | None = None, /, *, order: str | Template | None = None) -> R:
        """Find a single row by a relational expression. Raises RecordNotFoundError if no row is found.

        If target is missing or doesn't uniquely identify a row, return the
        first row. Combine with `order` to deterministically pick a row.
        """
        compiled_params: dict[str, Any] = {}
        sql = build_select_sql(Model, target, compiled_params, order=order, limit=1)
        if params:
            compiled_params.update(params)

        cur = self._query(Model, sql, compiled_params)
        row = cur.fetchone()
        cur.close()

        if row is None:
            raise RecordNotFoundError(Model.__name__, target)
        return row

    def select[R: Row | TableRow](
        self, Model: type[R], target: Any = None, params: dict[str, Any] | None = None, /, *, order: str | Template | None = None, limit: int | None = None, offset: int | None = None
    ) -> TypedCursorProxy[R]:
        """Select rows by a relational expression, returning a typed cursor.

        The cursor is iterable (yields model instances) and exposes the usual
        apsw methods (`fetchone`, `fetchall`, `fetchmany`). Use it for
        streaming with `for row in engine.select(...)` or materialize all rows
        with `engine.select(...).fetchall()`.

        If target is missing, selects all rows. `params` overrides named
        parameters bound by a t-string target, enabling reuse of a predicate
        with different values.
        """
        compiled_params: dict[str, Any] = {}
        sql = build_select_sql(Model, target, compiled_params, order=order, limit=limit, offset=offset)
        if params:
            compiled_params.update(params)
        return self._query(Model, sql, compiled_params)

    def _query[R: Row | TableRow](self, Model: type[R], sql: str, parameters: Sequence | dict | None = ()) -> TypedCursorProxy[R]:
        cursor = self.connection.execute(sql, parameters)
        return TypedCursorProxy.proxy_cursor_lazy(Model, cursor, self)

    #### Writing
    def insert[R: TableRow](self, row: R) -> R:
        """Insert a record."""
        Model = type(row)

        # Check that all related objects are persisted (have an id) before inserting.
        for f in Model.__fields__:
            if not f.is_fk:
                continue
            related = getattr(row, f.name)
            if related is not None and related.id is None:
                raise UnpersistedRelationshipError(Model.__name__, f.name, row)

        insert = build_insert_sql(Model)

        # `vars(row)` is a single C-level __dict__ read (much cheaper than per-field
        # `getattr`, which routes through TableRow.__getattribute__). Virtual backref
        # fields living in __dict__ are harmless extra keys: apsw ignores dict bindings
        # not referenced by the statement's placeholders.
        cur = self._query(Model, insert, vars(row))
        result = cur.fetchone()
        cur.close()
        assert result is not None  # INSERT always returns a row on success
        return result

    def update(self, Model: type[TableRow], target: Any, params: dict[str, Any] | None = None, /, **patch: Any) -> int:
        """Update records matching the target expression."""
        if target is None:
            return 0

        if not patch:
            raise NoKwargFieldSpecifiedError()

        field_names = {f.name for f in Model.__fields__}
        invalid_kwargs = {k: v for k, v in patch.items() if k not in field_names}
        if invalid_kwargs:
            raise InvalidKwargFieldSpecifiedError(Model, invalid_kwargs)

        compiled_params: dict[str, Any] = {**patch}
        sql = build_update_sql(Model, target, compiled_params, frozenset(patch.keys()))
        if params:
            compiled_params.update(params)

        cur = self._query(Model, sql, compiled_params)
        changes = self.connection.changes()
        cur.close()
        return changes

    def delete(self, Model: type[TableRow], target: Any, params: dict[str, Any] | None = None, /) -> int:
        """Delete records matching the target expression."""
        if target is None:
            return 0

        compiled_params: dict[str, Any] = {}
        query = build_delete_sql(Model, target, compiled_params)
        if params:
            compiled_params.update(params)

        self.connection.execute(query, compiled_params)
        return self.connection.changes()


__all__ = [Engine, *__all_errors__]
