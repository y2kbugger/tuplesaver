# Provides various useful routines
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import apsw
import apsw.unicode

# NOTE: cursorproxy.py should only know about .model
from .model import Row, get_meta, is_row_model

if TYPE_CHECKING:
    from .engine import Engine


class Lazy[Model]:
    __slots__ = ("_cached", "_engine", "_id", "_model")

    def __init__(self, engine: Engine, model: type[Row], id_: int):
        self._engine = engine
        self._model = model
        self._id = id_
        self._cached = None

    def _obj(self) -> Model:
        if self._cached is None:
            self._cached = self._engine.find(self._model, self._id)
        return cast(Model, self._cached)

    def __hash__(self):
        return hash((self._model, self._id))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return self._id == other
        elif type(other) is self._model:
            return self._id == other[0]
        elif isinstance(other, Lazy):
            return self._model == other._model and self._id == other._id
        return False

    def __repr__(self):
        if self._cached is None:
            return f"<{self.__class__.__name__}[{self._model.__name__}]:{self._id} (pending)>"
        return f"<{self.__class__.__name__}:{self._cached!r}>"


def _make_model_lazy[R: Row](RootModel: type[R], c: apsw.Cursor, root_row: apsw.SQLiteValues, engine: Engine) -> R:
    """Lazy loading of relationships, only fetches sub-models when accessed."""
    # First just create the root RootModel with FKs in related fields
    row = RootModel._make(root_row)

    # Now iterate over the fields and replace any foreign keys with Lazy proxies
    if not is_row_model(RootModel):
        return row  # return early if not a model

    for idx, field in enumerate(get_meta(RootModel).fields):
        if field.type is not None and is_row_model(field.type):
            # Replace with Lazy proxy
            fk_value = root_row[idx]
            assert isinstance(fk_value, int | type(None))
            if fk_value is not None:
                row = row._replace(**{field.name: Lazy(engine, field.type, fk_value)})

    return row  # Return the root model with lazy-loaded relationships


class TypedCursorProxy[R: Row](apsw.Cursor):
    def fetchone(self) -> R | None: ...

    def fetchall(self) -> list[R]: ...  # ty:ignore[invalid-method-override, invalid-return-type]

    @staticmethod
    def proxy_cursor_lazy(Model: type[R], cursor: apsw.Cursor, engine: Engine) -> TypedCursorProxy[R]:
        # Save the existing row_trace (the adapt/convert converter)
        existing_row_trace = cursor.row_trace

        def row_fac_lazy(c: apsw.Cursor, r: apsw.SQLiteValues) -> R:
            # First apply the adapt/convert converters if they exist
            if existing_row_trace is not None:
                r = existing_row_trace(c, r)
            # Then apply the model creation logic
            return _make_model_lazy(Model, c, r, engine)

        cursor.row_trace = row_fac_lazy

        return cast(TypedCursorProxy[R], cursor)
