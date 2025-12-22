# Provides various useful routines
from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import apsw
import apsw.unicode

# NOTE: cursorproxy.py should only know about .model
from .model import Row, get_meta, is_row_model

if TYPE_CHECKING:
    from .engine import Engine


def _make_model_deep[R: Row](RootModel: type[R], c: apsw.Cursor, root_row: apsw.SQLiteValues) -> R:
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
            elif is_row_model(FieldType):
                # Sub-model fetch
                InnerModel = FieldType
                from .sql import generate_select_by_field_sql

                select_by_id = generate_select_by_field_sql(InnerModel, frozenset({"id"}))

                inner_values = list(c.execute(select_by_id, {'id': field_value}).fetchone())  # ty:ignore[invalid-argument-type] Any is actually a tuple coming back

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
    @staticmethod
    def proxy_cursor_deep(Model: type[R], cursor: apsw.Cursor, *, deep: bool = False) -> TypedCursorProxy[R]:
        def row_fac_deep(c: apsw.Cursor, r: apsw.SQLiteValues) -> R:
            # if there is already a type converting cursor, use it
            if hasattr(c, "_row_converter"):
                r = c._row_converter(c, r)  # type: ignore  # noqa: SLF001

            # Disable the row factory to let us handle making the inner models ourselves
            root_row_trace = c.row_trace
            c.row_trace = None

            # so we don't thow away subsequent results in outer cursor
            inner_c = c.connection.cursor()
            m = _make_model_deep(Model, inner_c, r)
            inner_c.close()
            c.row_trace = root_row_trace
            return m

        cursor.row_trace = row_fac_deep

        return cast(TypedCursorProxy[R], cursor)

    @staticmethod
    def proxy_cursor_lazy(Model: type[R], cursor: apsw.Cursor, engine: Engine) -> TypedCursorProxy[R]:
        def row_fac_lazy(c: apsw.Cursor, r: apsw.SQLiteValues) -> R:
            # if there is already a type converting cursor, use it
            if hasattr(c, "_row_converter"):
                r = c._row_converter(c, r)  # type: ignore  # noqa: SLF001

            return _make_model_lazy(Model, c, r, engine)

        cursor.row_trace = row_fac_lazy

        return cast(TypedCursorProxy[R], cursor)

    def fetchone(self) -> R | None: ...

    def fetchall(self) -> list[R]: ...  # ty:ignore[invalid-method-override, invalid-return-type]
