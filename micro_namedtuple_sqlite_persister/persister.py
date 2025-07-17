from __future__ import annotations

import logging
import re
import sqlite3
from collections.abc import Sequence
from textwrap import dedent
from typing import Any, cast, overload

# NOTE: persister.py should only know about .model, but not .query
from .model import (
    Row,
    get_meta,
    is_registered_fieldtype,
    is_registered_table_model,
    is_row_model,
    register_table_model,
)

logger = logging.getLogger(__name__)


class FieldZeroIdRequired(Exception):
    def __init__(self, model_name: str, field_zero_name: str, field_zero_typehint: Any) -> None:
        super().__init__(
            self,
            f"Field 0 of {model_name} is required to be `id: int | None` but instead is `{field_zero_name}: {field_zero_typehint}`",
        )


class TableSchemaMismatch(Exception):
    def __init__(self, table_name: str, existing_table_schema: str, new_table_schema: str) -> None:
        super().__init__(
            f"Table `{table_name}` already exists but the schema does not match the expected schema.\njExisting schema:\n\t{existing_table_schema}.\nExpected schema:\n\t{new_table_schema}"
        )


class UnregisteredFieldTypeError(Exception):
    def __init__(self, field_type: type) -> None:
        if is_row_model(field_type):
            msg = (
                f"Field Type {field_type} is a NamedTuple Row Model, but it has not been registered with the Persister Engine.\n"
                "Use `engine.ensure_table_created({field_type.__name__})` to register it"
            )
        else:
            msg = f"Field Type {field_type} has not been registered with an adapter and converter.\n `register_adapt_convert` to register it"
        super().__init__(msg)


class UnpersistedRelationshipError(Exception):
    def __init__(self, model_name: str, field_name: str, row: Row) -> None:
        super().__init__(self, f"Cannot save {model_name} with unpersisted {model_name}.{field_name} of row {row}")


class NoKwargFieldSpecifiedError(ValueError):
    def __init__(self) -> None:
        super().__init__("At least one field must be specified to find a row.")


class InvalidKwargFieldSpecifiedError(ValueError):
    def __init__(self, Model: type[Row], kwargs: dict[str, Any]) -> None:
        super().__init__(f"Invalid fields for {Model.__name__}: {', '.join(kwargs.keys())}. Valid fields are: {', '.join(f.name for f in get_meta(Model).fields)}")


class IdNoneError(ValueError):
    pass


class IdNotFoundError(ValueError):
    pass


class Engine:
    def __init__(self, db_path: str, echo_sql: bool = False) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL")
        if echo_sql:
            self.connection.set_trace_callback(print)

    def _get_sql_for_existing_table(self, table_name: str) -> str:
        query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
        cursor = self.connection.execute(query)
        return cursor.fetchone()[0]

    def ensure_table_created(self, Model: type[Row], *, force_recreate: bool = False) -> None:
        meta = get_meta(Model)
        field_zero = meta.fields[0]
        if field_zero.name != "id" or field_zero.full_type != (int | None):
            raise FieldZeroIdRequired(meta.model_name, field_zero.name, field_zero.full_type)

        # Check that all fields are registered
        for field in meta.fields:
            # skipping self reference, it will be registered later
            if field.type is Model:
                continue
            if not is_registered_fieldtype(field.type):
                raise UnregisteredFieldTypeError(field.type)

        query = dedent(f"""
            CREATE TABLE {meta.table_name} (
            {', '.join(f.sql_columndef for f in meta.fields)}
            )""").strip()

        try:
            self.connection.execute(query)
        except sqlite3.OperationalError as e:
            if f"table {meta.table_name} already exists" in str(e):
                # Force Recreate
                if force_recreate:
                    self.connection.execute(f"DROP TABLE {meta.table_name}")
                    self.connection.execute(query)

                # Check existing table, it might be ok
                def normalize_whitespace(s: str) -> str:
                    return re.sub(r'\s+', ' ', s).strip()

                existing_table_schema = normalize_whitespace(self._get_sql_for_existing_table(meta.table_name))
                new_table_schema = normalize_whitespace(query)

                if existing_table_schema != new_table_schema:
                    raise TableSchemaMismatch(meta.table_name, existing_table_schema, new_table_schema) from e
            else:
                # error is not about the table already existing
                raise

        register_table_model(Model)
        sqlite3.register_adapter(Model, lambda row: row[0])  # Register to be able to insert Model instances as foreign keys

    #### Writing
    def save[R: Row](self, row: R, *, deep: bool = False) -> R:
        """insert or update records, based on the presence of an id"""
        if deep:
            # If deep is True, we save all related rows recursively
            row = row._make(self.save(f, deep=deep) if is_registered_table_model(type(f)) else f for f in row)
        else:
            # Don't allow saving if a related row is not persisted
            if any(related_row.id is None for related_row in row[1:] if is_row_model(related_row.__class__)):
                raise UnpersistedRelationshipError(type(row).__name__, "related row", row)

        if row[0] is None:
            cur = self.connection.execute(get_meta(type(row)).insert, row)
            return row._replace(id=cur.lastrowid)
        else:
            query = dedent(f"""
                UPDATE {row.__class__.__name__}
                SET {', '.join(f"{f} = ?" for f in row._fields)}
                WHERE id = ?
                """).strip()
            cur = self.connection.execute(query, (*row, row[0]))
            if cur.rowcount == 0:
                raise IdNotFoundError(f"Cannot UPDATE, no row with id={row[0]} in table `{row.__class__.__name__}`")
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
        query = dedent(f"""
            DELETE FROM {Model.__name__}
            WHERE id = ?
            """).strip()
        cur = self.connection.execute(query, (row_id,))
        if cur.rowcount == 0:
            raise IdNotFoundError(f"Cannot DELETE, no row with id={row_id} in table `{Model.__name__}`")

    ##### Reading
    def find[R: Row](self, Model: type[R], row_id: int | None) -> R:
        if row_id is None:
            raise IdNoneError("Cannot SELECT, id=None")

        row = self.query(Model, get_meta(Model).select_by_id, (row_id,)).fetchone()

        if row is None:
            raise IdNotFoundError(f"Cannot SELECT, no row with id={row_id} in table `{Model.__name__}`")

        return row

    def find_by[R: Row](self, Model: type[R], **kwargs: Any) -> R:
        """Find a row by its fields, e.g. `find_by(Model, name="Alice")`"""
        if not kwargs:
            raise NoKwargFieldSpecifiedError()

        field_names = [f.name for f in get_meta(Model).fields]
        if not all(k in field_names for k in kwargs):
            raise InvalidKwargFieldSpecifiedError(Model, kwargs)

        # Build the WHERE clause
        where_clause = " AND ".join(f"{k} = ?" for k in kwargs)
        select = get_meta(Model).select
        sql = dedent(f"""
            {select}
            WHERE {where_clause}
            """).strip()

        cursor = self.connection.execute(sql, tuple(kwargs.values()))
        row = cursor.fetchone()

        return row

    def query[R: Row](self, Model: type[R], sql: str, parameters: Sequence | dict = tuple()) -> TypedCursorProxy[R]:
        cursor = self.connection.execute(sql, parameters)
        return TypedCursorProxy.proxy_cursor(Model, cursor)


def make_model[R: Row](RootModel: type[R], c: sqlite3.Cursor, root_row: sqlite3.Row) -> R:
    # Non-recursive depth-first stack approach.

    # Stack items: (Model, values, field_index, parent_values, parent_field_index)
    stack: list[tuple[Any, ...]] = [(RootModel, list(root_row), 0, None, None)]

    while stack:
        Model, values, idx, parent_values, parent_idx = stack.pop()
        meta = get_meta(Model)

        while idx < len(meta.fields):
            FieldType = meta.fields[idx].type
            field_value = values[idx]
            if field_value is None:
                # we could assert _nullable here, but we are not in the business of validating data
                # external edits to the database could cause mismatches types in any field, this isn't a special case, but we do need
                # to avoid trying to fetch a model with a id=None.
                pass
            elif is_registered_table_model(FieldType):
                # Sub-model fetch
                InnerModel = FieldType
                inner_meta = get_meta(InnerModel)

                inner_values = list(c.execute(inner_meta.select_by_id, (field_value,)).fetchone())

                # Defer remainder of current model
                stack.append((Model, values, idx + 1, parent_values, parent_idx))
                stack.append((InnerModel, inner_values, 0, values, idx))
                break

            idx += 1
        else:
            # All fields processed; finalize current model
            built = Model._make(values)
            if parent_values is not None:
                parent_values[parent_idx] = built
            else:
                return built

    # Should never reach here if input data is valid
    raise AssertionError("Stack unexpectedly empty before returning the root model.")


class TypedCursorProxy[R: Row](sqlite3.Cursor):
    @staticmethod
    def proxy_cursor(Model: type[R], cursor: sqlite3.Cursor) -> TypedCursorProxy[R]:
        def row_fac(c: sqlite3.Cursor, r: sqlite3.Row) -> R:
            # Disable the row factory to let us handle making the inner models ourselves
            root_row_factory = c.row_factory
            c.row_factory = None

            # so we don't thow away subsequent results in outer cursor
            inner_c = c.connection.cursor()
            m = make_model(Model, inner_c, r)
            inner_c.close()
            c.row_factory = root_row_factory
            return m

        cursor.row_factory = row_fac
        return cast(TypedCursorProxy, cursor)

    def fetchone(self) -> R | None: ...

    def fetchall(self) -> list[R]: ...

    def fetchmany(self, size: int | None = 1) -> list[R]: ...
