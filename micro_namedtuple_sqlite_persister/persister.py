import datetime as dt
import sqlite3
from collections.abc import Callable
from typing import NamedTuple, TypeVar

ROW = TypeVar('ROW', bound=NamedTuple)


class Engine:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)

    # writing
    def ensure_table_created(self, model: type[ROW]) -> None:
        raise NotImplementedError

    def insert(self, row: ROW) -> ROW:
        raise NotImplementedError

    def update(self, row: ROW) -> ROW:
        raise NotImplementedError

    def delete(self, model: type[ROW], row_id: int) -> None:
        raise NotImplementedError

    # reading
    def get(self, model: type[ROW], row_id: int) -> ROW:
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
