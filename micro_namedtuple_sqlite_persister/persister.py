import datetime as dt
import sqlite3
from collections.abc import Callable, Mapping
from typing import NamedTuple, TypeVar

ROW = TypeVar('ROW', bound=NamedTuple)

_field_type_map: Mapping[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
    dt.date: "TEXT",
    dt.datetime: "TEXT",
}


def column_type(field_name: str, FieldType: type) -> str:
    if field_name == "id":
        return "INTEGER PRIMARY KEY"
    return _field_type_map[FieldType]


class Engine:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)

    #### Writing
    def ensure_table_created(self, Model: type[ROW]) -> None:
        preamble = f"CREATE TABLE IF NOT EXISTS {Model.__name__} ("
        colname_types = [(name, column_type(name, FieldType)) for name, FieldType in Model.__annotations__.items()]
        cols = ", ".join([f"{n} {t}" for n, t in colname_types])
        endcap = ");"
        query = f"{preamble} {cols} {endcap}"
        self.connection.execute(query)

    def insert(self, row: ROW) -> ROW:
        preamble = f"INSERT INTO {row.__class__.__name__} ("
        colnames = ", ".join(row._fields)
        mid = ") VALUES ("
        placeholders = ", ".join("?" for _ in range(len(row._fields)))
        endcap = ");"
        query = f"{preamble} {colnames} {mid} {placeholders} {endcap}"
        self.connection.execute(query, row)
        return row

    def update(self, row: ROW) -> ROW:
        raise NotImplementedError

    def delete(self, Model: type[ROW], row_id: int) -> None:
        raise NotImplementedError

    ##### Reading
    def get(self, Model: type[ROW], row_id: int) -> ROW:
        raise NotImplementedError


## Serialization and Deserialization
D = TypeVar('D')
S = TypeVar('S', str, bytes)


def register_serde(serialize: Callable[[D], S], deserialize: Callable[[S], D]) -> None:
    raise NotImplementedError


def enable_included_serdes() -> None:
    def serialize_datetime(datetime: dt.datetime) -> str:
        return datetime.isoformat()

    def deserialize_datetime(data: str) -> dt.datetime:
        return dt.datetime.fromisoformat(data)

    register_serde(serialize_datetime, deserialize_datetime)
