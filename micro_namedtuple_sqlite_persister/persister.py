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


def _column_definition(annotation: tuple[str, type]) -> str:
    field_name, FieldType = annotation
    if field_name == "id":
        return "id INTEGER PRIMARY KEY NOT NULL"
    return f"{field_name} {_field_type_map[FieldType]} NOT NULL"


class Engine:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)

    #### Writing
    def ensure_table_created(self, Model: type[ROW]) -> None:
        query = f"""
            CREATE TABLE IF NOT EXISTS {Model.__name__} (
            {', '.join(_column_definition(f) for f in Model.__annotations__.items())}
            )"""
        self.connection.execute(query)

    def insert(self, row: ROW) -> ROW:
        query = f"""
            INSERT INTO {row.__class__.__name__} (
            {', '.join(row._fields)}
            ) VALUES (
            {', '.join("?" for _ in range(len(row._fields)))}
            )"""
        self.connection.execute(query, row)
        return row

    def update(self, row: ROW) -> ROW:
        raise NotImplementedError

    def delete(self, Model: type[ROW], row_id: int) -> None:
        raise NotImplementedError

    ##### Reading
    def get(self, Model: type[ROW], row_id: int) -> ROW:
        query = f"""
            SELECT {', '.join(Model._fields)}
            FROM {Model.__name__}
            WHERE id = ?
            """
        cursor = self.connection.execute(query, (row_id,))
        return Model._make(cursor.fetchone())


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
