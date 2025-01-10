import datetime as dt
import pickle
import sqlite3
from collections.abc import Callable, Iterable
from typing import Any, NamedTuple

type Row = NamedTuple

_field_type_map: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}


class UnregisteredFieldTypeError(Exception):
    def __init__(self, field_type: type) -> None:
        super().__init__(f"Field Type {field_type} has not been registered with the Persister. Use `register_adapt_convert` to register it")


def _column_definition(annotation: tuple[str, type]) -> str:
    field_name, FieldType = annotation
    if field_name == "id":
        return "id [INTEGER] PRIMARY KEY NOT NULL"
    field_type_name = _field_type_map.get(FieldType)
    if field_type_name is None:
        raise UnregisteredFieldTypeError(FieldType)
    return f"{field_name} [{_field_type_map[FieldType]}] NOT NULL"


class Engine:
    def __init__(self, db_path: str, echo_sql: bool = False) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL")
        if echo_sql:
            self.connection.set_trace_callback(print)

    #### Writing
    def ensure_table_created(self, Model: type[Row]) -> None:
        query = f"""
            CREATE TABLE IF NOT EXISTS {Model.__name__} (
            {', '.join(_column_definition(f) for f in Model.__annotations__.items())}
            )"""
        self.connection.execute(query)

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

    def delete(self, Model: type[Row], row_id: int | None) -> None:
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


## Adapt/Convert
def register_adapt_convert[D](AdaptConvertType: type[D], adapt: Callable[[D], bytes], convert: Callable[[bytes], D], overwrite: bool = False) -> None:
    if AdaptConvertType in _field_type_map and not overwrite:
        raise ValueError(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or alread has an Adapt Convert registered")

    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__name__}"
    sqlite3.register_adapter(AdaptConvertType, adapt)
    sqlite3.register_converter(field_type_name, convert)
    _field_type_map[AdaptConvertType] = field_type_name


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
