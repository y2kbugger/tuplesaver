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
    def __init__(self, db_path: str, echo_sql: bool = False) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        if echo_sql:
            self.connection.set_trace_callback(print)

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
        cur = self.connection.execute(query, row)
        return row._replace(id=cur.lastrowid)

    def update(self, row: NamedTuple) -> None:
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
        self.connection.commit()

    def delete(self, Model: type[ROW], row_id: int | None) -> None:
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
    def get(self, Model: type[ROW], row_id: int | None) -> ROW:
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
