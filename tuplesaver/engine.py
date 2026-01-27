from __future__ import annotations

import logging
import os
import re
from collections.abc import Sequence
from dataclasses import fields, replace
from typing import Any, overload

import apsw

from .adaptconvert import AdaptConvertRegistry
from .cursorproxy import TypedCursorProxy

# NOTE: engine.py should only know about .model, but not .query
from .model import (
    Row,
    TableRow,
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
        super().__init__(f"Invalid fields for {Model.__name__}: {', '.join(kwargs.keys())}. Valid fields are: {', '.join(f.name for f in Model.meta.fields)}")


class UnregisteredFieldTypeError(ValueError):
    def __init__(self, field_type: type | Any) -> None:
        if is_row_model(field_type):
            msg = (
                f"Field Type `{field_type}` is a NamedTuple Row Model, but it has not been registered with the Persister Engine.\n"
                "Use `engine.ensure_table_created({field_type.__name__})` to register it"
            )
        elif field_type is Any:
            msg = "Field Type `Any` is not a valid type for persisting, it can only be used for reading"
        else:
            msg = f"Field Type `{field_type}` has not been registered with an adapter and converter.\n `register_adapt_convert` to register it"

        super().__init__(msg)


class IdNoneError(ValueError):
    pass


class MatchNotFoundError(ValueError):
    pass


__all_errors__ = [
    TableSchemaMismatch,
    UnpersistedRelationshipError,
    LookupByAdHocModelImpossible,
    NoKwargFieldSpecifiedError,
    InvalidKwargFieldSpecifiedError,
    UnregisteredFieldTypeError,
    IdNoneError,
    MatchNotFoundError,
]


class Engine:
    def __init__(self, db_path: str | os.PathLike[str] | apsw.Connection) -> None:
        if isinstance(db_path, apsw.Connection):
            self.connection = db_path
            self.db_path = self.connection.filename
        else:
            self.db_path = db_path
            self.connection: apsw.Connection = apsw.Connection(str(db_path))
            self.connection.execute("PRAGMA journal_mode=WAL")

        self.adapt_convert_registry = AdaptConvertRegistry()
        self.connection.cursor_factory = self.adapt_convert_registry

    def ensure_table_created(self, Model: type[TableRow]) -> None:
        assert is_row_model(Model), f"Model `{Model.__name__}` is not a valid table model."
        meta = Model.meta

        ddl = generate_create_table_ddl(Model)

        for field in meta.fields:
            if field.type == Model:
                continue  # recursive
            if not self.adapt_convert_registry.is_valid_adapttype(field.type):
                raise UnregisteredFieldTypeError(field.type)

        try:
            self.connection.execute(ddl)
        except apsw.SQLError as e:
            assert meta.table_name is not None, "Table name must be defined for the model to create it."
            if f"table {meta.table_name} already exists" in str(e):
                # Check existing table, it might be ok
                def _normalize_whitespace(s: str) -> str:
                    return re.sub(r'\s+', ' ', s).strip()

                def _get_sql_for_existing_table(table_name: str) -> str:
                    # TODO: is there a apsw method for this?
                    query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                    cursor = self.connection.execute(query)
                    result = cursor.fetchone()
                    assert result is not None, f"Table {table_name} not found in sqlite_master"
                    return result[0]

                existing_table_schema = _normalize_whitespace(_get_sql_for_existing_table(meta.table_name))
                new_table_schema = _normalize_whitespace(ddl)

                if existing_table_schema != new_table_schema:
                    raise TableSchemaMismatch(meta.table_name, existing_table_schema, new_table_schema) from e
            else:
                # error is not about the table already existing
                raise
        except Exception as e:
            raise e

        self.adapt_convert_registry.register_adapt_convert(Model, adapt=lambda row: row.id, convert=lambda _id: _id)

    ##### Reading
    def find[R: TableRow](self, Model: type[R], row_id: int | None) -> R:
        """Find a row by its id. This is a special case of find_by."""
        if row_id is None:
            raise IdNoneError("Cannot SELECT, id=None")

        return self.find_by(Model, id=row_id)

    def find_by[R: TableRow](self, Model: type[R], **kwargs: Any) -> R:
        """Find a row by its fields, e.g. `find_by(Model, name="Alice")`"""

        if not kwargs:
            raise NoKwargFieldSpecifiedError()

        if not is_row_model(Model):
            raise LookupByAdHocModelImpossible(Model.__name__)

        meta = Model.meta

        field_names = [f.name for f in meta.fields]
        if not all(k in field_names for k in kwargs):
            raise InvalidKwargFieldSpecifiedError(Model, kwargs)

        # Use cached query generation
        sql = generate_select_by_field_sql(Model, frozenset(kwargs.keys()))

        row = self.query(Model, sql, kwargs).fetchone()

        if row is None:
            kwargs_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
            raise MatchNotFoundError(f"Cannot SELECT, no row with {kwargs_str} in table `{Model.__name__}`")

        return row

    def query[R: Row | TableRow](self, Model: type[R], sql: str, parameters: Sequence | dict = tuple()) -> TypedCursorProxy[R]:
        cursor = self.connection.execute(sql, parameters)
        return TypedCursorProxy.proxy_cursor_lazy(Model, cursor, self)

    #### Writing
    def save[R: TableRow](self, row: R) -> R:
        """insert or update records, based on the presence of an id"""

        # Don't allow saving if a related row is not persisted
        for f in fields(row)[1:]:  # skip id field
            related_row = getattr(row, f.name)
            if is_row_model(related_row.__class__) and related_row.id is None:
                raise UnpersistedRelationshipError(type(row).__name__, f.name, row)

        if row.id is None:
            insert = generate_insert_sql(type(row))
            self.connection.execute(insert, vars(row))
            return replace(row, id=self.connection.last_insert_rowid())
        else:
            update = generate_update_sql(type(row))
            self.connection.execute(update, vars(row))
            if self.connection.changes() == 0:
                raise MatchNotFoundError(f"Cannot UPDATE, no row with id={row.id} in table `{row.__class__.__name__}`")
            return row

    @overload
    def delete(self, Model: type[TableRow], row_id: int | None) -> None: ...

    @overload
    def delete(self, row: TableRow) -> None: ...

    def delete(self, Model_or_row: type[TableRow] | TableRow, row_id: int | None = None) -> None:  # pyright: ignore [reportInconsistentOverload] allow overloads with different parameter names
        if not isinstance(Model_or_row, type):
            row = Model_or_row
            Model = row.__class__
            assert row_id is None, "Do not provide row_id when passing a row instance."
            row_id = row.id
        else:
            Model = Model_or_row

        if row_id is None:
            raise IdNoneError("Cannot DELETE, id=None")
        query = generate_delete_sql(Model)
        self.connection.execute(query, {'id': row_id})
        if self.connection.changes() == 0:
            raise MatchNotFoundError(f"Cannot DELETE, no row with id={row_id} in table `{Model.__name__}`")


__all__ = [Engine, *__all_errors__]
