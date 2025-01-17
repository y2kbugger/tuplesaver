from __future__ import annotations

import datetime as dt
import inspect
import pickle
import re
import sqlite3
import types
from collections.abc import Callable, Iterable, Sequence
from typing import Any, ForwardRef, NamedTuple, Union, cast, get_args, get_origin, get_type_hints, overload

type Row = NamedTuple

_columntype: dict[type, str] = {}


def reset_to_native_columntypes() -> None:
    native_columntype = {
        str: "TEXT",
        float: "REAL",
        int: "INTEGER",
        bytes: "BLOB",
    }
    _columntype.clear()
    _columntype.update(native_columntype)


reset_to_native_columntypes()


class UnregisteredFieldTypeError(Exception):
    def __init__(self, field_type: type) -> None:
        super().__init__(f"Field Type {field_type} has not been registered with the Persister. Use `register_adapt_convert` to register it")


def unwrap_optional_type(type_hint: Any) -> tuple[bool, Any]:
    """Determine if a given type hint is an Optional type

    Supports the following forms of Optional types:
    UnionType (e.g., int | None)
    Optional  (e.g., Optional[int])
    Union (e.g., Union[int, None])

    Returns
    - A boolean indicating if it is Optional.
    - The underlying type if it is Optional, otherwise the original type.
    """

    # Not any form of Union type
    if not (isinstance(type_hint, types.UnionType) or get_origin(type_hint) is Union):
        return False, type_hint

    args = get_args(type_hint)
    optional = type(None) in args

    underlying_types = tuple(arg for arg in args if arg is not type(None))
    underlying_type = underlying_types[0]
    for t in underlying_types[1:]:
        underlying_type |= t

    return optional, underlying_type


def get_resolved_annotations(Model: Any) -> dict[str, Any]:
    """Resolve ForwardRef type hints by combining all local and global namespaces up the call stack."""
    globalns = getattr(inspect.getmodule(Model), "__dict__", {})
    localns = {}

    for frame in inspect.stack():
        localns.update(frame.frame.f_locals)

    return get_type_hints(Model, globalns=globalns, localns=localns)


def _column_definition(annotation: tuple[str, Any]) -> str:
    field_name, FieldType = annotation

    nullable, FieldType = unwrap_optional_type(FieldType)

    if field_name == "id":
        return "id [INTEGER] PRIMARY KEY NOT NULL"

    columntype = _columntype.get(FieldType)
    if columntype is None:
        raise UnregisteredFieldTypeError(FieldType)

    if nullable:
        nullable_sql = "NULL"
    else:
        nullable_sql = "NOT NULL"

    return f"{field_name} [{columntype}] {nullable_sql}"


def normalize_whitespace(s: str) -> str:
    return re.sub(r'\s+', ' ', s).strip()


class FieldZeroIdRequired(Exception):
    def __init__(self, model_name: str, field_zero_name: str, field_zero_typehint: Any) -> None:
        super().__init__(self, f"Field 0 of {model_name} is required to be `id: int | None` but instead is `{field_zero_name}: {field_zero_typehint}`")


class TableSchemaMismatch(Exception):
    pass


def make_model[R: Row](MMM: type[R], c: sqlite3.Cursor, r: sqlite3.Row) -> R:
    rr = []
    for field_value, (_field_name, FieldType) in zip(r, MMM.__annotations__.items()):
        _nullable, FieldType = unwrap_optional_type(FieldType)
        # ForwardRefs here are only from non-tables TODO: maybe we need to resolve annotations anyway (do view joins need this?)
        if field_value is None:
            rr.append(None)
        elif type(FieldType) is ForwardRef:
            rr.append(field_value)
        elif _columntype.get(FieldType) == f"{FieldType.__name__}_ID":
            InnerModel = FieldType
            inner_r = c.execute(f"SELECT * FROM {InnerModel.__name__} WHERE id = ?", (field_value,)).fetchone()
            rr.append(make_model(InnerModel, c, inner_r))
        else:
            rr.append(field_value)
    return MMM._make(rr)


class TypedCursorProxy[R: Row](sqlite3.Cursor):
    @staticmethod
    def proxy_cursor(Model: type[R], cursor: sqlite3.Cursor) -> TypedCursorProxy:
        def row_fac(c: sqlite3.Cursor, r: sqlite3.Row) -> R:
            # Disable the row factory to let us handle making the inner models ourselves
            root_row_factory = c.row_factory
            c.row_factory = None
            m = make_model(Model, c, r)
            c.row_factory = root_row_factory
            return m

        cursor.row_factory = row_fac
        return cast(TypedCursorProxy, cursor)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.cursor, name)

    def fetchone(self) -> R:
        return self.cursor.fetchone()

    def fetchall(self) -> list[R]:
        return self.cursor.fetchall()

    def fetchmany(self, size: int | None = 1) -> list[R]:
        return self.cursor.fetchmany(size)


class Engine:
    def __init__(self, db_path: str, echo_sql: bool = False) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL")
        if echo_sql:
            self.connection.set_trace_callback(print)

    def _get_sql_for_existing_table(self, Model: type[Row]) -> str:
        query = f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{Model.__name__}'"
        cursor = self.connection.execute(query)
        return normalize_whitespace(cursor.fetchone()[0])

    #### Writing
    def ensure_table_created(self, Model: type[Row], *, force_recreate: bool = False) -> None:
        annotations = get_resolved_annotations(Model)
        Model.__annotations__ = annotations  # TODO: this might not be good, but maybe it is, move to Meta later
        field_zero_name = Model._fields[0]
        field_zero_typehint = annotations[field_zero_name]
        if field_zero_name != "id" or field_zero_typehint != (int | None):
            raise FieldZeroIdRequired(Model.__name__, field_zero_name, field_zero_typehint)

        # TODO: not alot of harm doing this before knowing if the table ends up being created
        # but I wanna do self joins and am afraid of manually cleaning up right now.
        _columntype[Model] = f"{Model.__name__}_ID"  # Register to table to be a possible foreign key
        sqlite3.register_adapter(Model, lambda row: row[0])  # Register to be able to insert Model instances as foreign keys

        query = f"""
            CREATE TABLE {Model.__name__} (
            {', '.join(_column_definition(f) for f in annotations.items())}
            )"""
        try:
            self.connection.execute(query)
        except sqlite3.OperationalError as e:
            if f"table {Model.__name__} already exists" in str(e):
                # Force Recreate
                if force_recreate:
                    self.connection.execute(f"DROP TABLE {Model.__name__}")
                    self.connection.execute(query)

                # Check existing table, it might be ok
                existing_table_schema = self._get_sql_for_existing_table(Model)
                new_table_schema = normalize_whitespace(query)
                if existing_table_schema != new_table_schema:
                    raise TableSchemaMismatch(
                        f"Table `{Model.__name__}` already exists but the schema does not match the expected schema."
                        f"\nExisting schema:\n\t{existing_table_schema}."
                        f"\nExpected schema:\n\t{new_table_schema}"
                    ) from e
            else:
                # error is not about the table already existing
                raise

    def _insert_if_is_model(self, field: Any) -> None:
        if _columntype.get(type(field)) == f"{field.__class__.__name__}_ID":
            return self.insert(field)
        else:
            return field

    def insert[R: Row](self, row: R) -> R:
        # recursively save
        row = row._make(self._insert_if_is_model(f) for f in row)

        query = f"""
            INSERT INTO {row.__class__.__name__} (
            {', '.join(row._fields)}
            ) VALUES (
            {', '.join("?" for _ in range(len(row._fields)))}
            )"""
        cur = self.connection.execute(query, row)
        return row._replace(id=cur.lastrowid)

    def update(self, row: Row) -> None:
        if row[0] is None:
            raise ValueError("Cannot UPDATE, id=None")
        query = f"""
            UPDATE {row.__class__.__name__}
            SET {', '.join(f"{f} = ?" for f in row._fields)}
            WHERE id = ?
            """
        cur = self.connection.execute(query, (*row, row[0]))
        if cur.rowcount == 0:
            raise ValueError(f"Cannot UPDATE, no row with id={row[0]} in table `{row.__class__.__name__}`")

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
            raise ValueError("Cannot DELETE, id=None")
        query = f"""
            DELETE FROM {Model.__name__}
            WHERE id = ?
            """
        cur = self.connection.execute(query, (row_id,))
        if cur.rowcount == 0:
            raise ValueError(f"Cannot DELETE, no row with id={row_id} in table `{Model.__name__}`")

    ##### Reading
    def get[R: Row](self, Model: type[R], row_id: int | None) -> R:
        if row_id is None:
            raise ValueError("Cannot SELECT, id=None")
        sql = f"""
            SELECT {', '.join(Model._fields)}
            FROM {Model.__name__}
            WHERE id = ?
            """
        row = self.query(Model, sql, (row_id,)).fetchone()
        if row is None:
            raise ValueError(f"Cannot SELECT, no row with id={row_id} in table `{Model.__name__}`")

        return Model._make(row)

    def query[R: Row](self, Model: type[R], sql: str, parameters: Sequence | dict = tuple()) -> TypedCursorProxy[R]:
        cursor = self.connection.execute(sql, parameters)
        return TypedCursorProxy.proxy_cursor(Model, cursor)


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType})` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


class AdaptConvertTypeAlreadyRegistered(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or already has an Adapt Convert registered")


## Adapt/Convert
def register_adapt_convert[D](AdaptConvertType: type[D], adapt: Callable[[D], bytes], convert: Callable[[bytes], D], overwrite: bool = False) -> None:
    if type(AdaptConvertType) is not type:
        raise InvalidAdaptConvertType(AdaptConvertType)

    if AdaptConvertType in _columntype and not overwrite:
        raise AdaptConvertTypeAlreadyRegistered(AdaptConvertType)

    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__qualname__}"
    sqlite3.register_adapter(AdaptConvertType, adapt)
    sqlite3.register_converter(field_type_name, convert)
    _columntype[AdaptConvertType] = field_type_name


included_adapt_converters: dict[type, tuple[Callable[[Any], bytes], Callable[[bytes], Any]]] = {
    dt.datetime: (
        lambda datetime: datetime.isoformat().encode(),
        lambda data: dt.datetime.fromisoformat(data.decode()),
    ),
    dt.date: (
        lambda date: date.isoformat().encode(),
        lambda data: dt.date.fromisoformat(data.decode()),
    ),
}

try:
    import pandas as pd

    def adapt_df(obj: pd.DataFrame) -> bytes:
        return pickle.dumps(obj)

    def convert_df(data: bytes) -> pd.DataFrame:
        return pickle.loads(data)

    included_adapt_converters[pd.DataFrame] = (adapt_df, convert_df)
except ImportError:
    pass


def enable_included_adaptconverters(Types: Iterable[type] | None = None) -> None:
    """Enable the included adapt/converters for the given types

    If no types are given, all included adapt/converters will be enabled
    """
    if Types is None:
        Types = included_adapt_converters.keys()

    for Type in Types:
        adapt, convert = included_adapt_converters[Type]
        register_adapt_convert(Type, adapt, convert)
