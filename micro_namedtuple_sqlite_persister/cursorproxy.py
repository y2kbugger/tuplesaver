from __future__ import annotations

import sqlite3
from typing import Any, cast

# NOTE: cursor.py should only know about .model
from .model import Row, get_meta, is_registered_table_model


def _make_model_deep[R: Row](RootModel: type[R], c: sqlite3.Cursor, root_row: sqlite3.Row) -> R:
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
        def row_fac_deep(c: sqlite3.Cursor, r: sqlite3.Row) -> R:
            # Disable the row factory to let us handle making the inner models ourselves
            root_row_factory = c.row_factory
            c.row_factory = None

            # so we don't thow away subsequent results in outer cursor
            inner_c = c.connection.cursor()
            m = _make_model_deep(Model, inner_c, r)
            inner_c.close()
            c.row_factory = root_row_factory
            return m

        cursor.row_factory = row_fac_deep
        return cast(TypedCursorProxy[R], cursor)

    def fetchone(self) -> R | None: ...

    def fetchall(self) -> list[R]: ...

    def fetchmany(self, size: int | None = 1) -> list[R]: ...
