import datetime as dt
import pickle
import re
import sqlite3
import types
from collections.abc import Callable, Iterable
from typing import Any, NamedTuple, Union, get_args, get_origin, overload

type Row = NamedTuple

_columntype: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}


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
    def __init__(self, Model: type[Row]):
        super().__init__(self, f"Field 0 of {Model.__name__} is required to be `id: int | None` but instead is `{Model._fields[0]}: {Model.__annotations__[Model._fields[0]]}`")


class TableSchemaMismatch(Exception):
    pass


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
        if Model._fields[0] != "id" or Model.__annotations__[Model._fields[0]] != (int | None):
            raise FieldZeroIdRequired(Model)

        query = f"""
            CREATE TABLE {Model.__name__} (
            {', '.join(_column_definition(f) for f in Model.__annotations__.items())}
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

    def insert[R: Row](self, row: R) -> R:
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
        query = f"""
            SELECT {', '.join(Model._fields)}
            FROM {Model.__name__}
            WHERE id = ?
            """
        cursor = self.connection.execute(query, (row_id,))
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Cannot SELECT, no row with id={row_id} in table `{Model.__name__}`")
        return Model._make(row)

    def query[R: Row](self, Model: type[R], sql: str) -> Iterable[R]:
        cursor = self.connection.execute(sql)
        return (Model._make(row) for row in cursor.fetchall())


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType})` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


## Adapt/Convert
def register_adapt_convert[D](AdaptConvertType: type[D], adapt: Callable[[D], bytes], convert: Callable[[bytes], D], overwrite: bool = False) -> None:
    if type(AdaptConvertType) is not type:
        raise InvalidAdaptConvertType(AdaptConvertType)

    if AdaptConvertType in _columntype and not overwrite:
        raise ValueError(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or alread has an Adapt Convert registered")

    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__name__}"
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
