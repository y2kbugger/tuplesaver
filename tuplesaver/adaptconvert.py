from __future__ import annotations

import datetime as dt
import json
import logging
import pickle
from collections.abc import Callable, Iterable
from typing import Any

import apsw

from .model import RowMeta, is_row_model, native_columntypes, schematype

logger = logging.getLogger(__name__)


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType}` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


class AdaptConvertTypeAlreadyRegistered(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or already has an Adapt Convert registered")


class UnregisteredFieldTypeError(ValueError):
    def __init__(self, field_type: type | Any) -> None:
        if is_row_model(field_type):
            msg = (
                f"Field Type `{field_type}` is a NamedTuple Row Model, but it has not been registered with the Persister Engine.\n"
                "Use `engine.ensure_table_created({field_type.__name__})` to register it"
            )
        elif field_type is Any:
            msg = "Field Type `Any` is not a valid type for persisting, it can only be used for reading"
        else:
            msg = f"Field Type `{field_type}` has not been registered with an adapter and converter.\n `register_adapt_convert` to register it"

        super().__init__(msg)


class AdaptConvertRegistry:
    """Provides cursors that can convert objects into one of the types supported by SQLite, or back from SQLite"""

    def __init__(self):
        self._adapters: dict[type, Callable] = {}
        self._converters: dict[str, Callable] = {}
        self._model_converters: dict[type, Callable[[apsw.SQLiteValues], tuple]] = {}

    def __call__(self, connection: apsw.Connection) -> AdaptConvertCursor:
        "Returns a new convertor :class:`cursor <apsw.Cursor>` for the `connection`"
        return AdaptConvertRegistry.AdaptConvertCursor(connection, self)

    def is_valid_adapttype(self, AdaptType: type) -> bool:
        return AdaptType in native_columntypes or AdaptType in self._adapters

    def adapt_value(self, value: Any) -> apsw.SQLiteValue:
        "Returns SQLite representation of `value`"
        adapter = self._adapters.get(type(value))
        if adapter:
            return adapter(value)
        # Fallback for Roww models - extract id for FK storage
        if is_row_model(type(value)):
            return value.id
        raise TypeError(f"No adapter registered for type {type(value)}")

    def make_converter_for_model(self, Model: RowMeta) -> Callable[[apsw.SQLiteValues], tuple]:
        """Build and cache an optimized row-converter for *Model*.

        Validates that every field type has a registered adapter, then
        generates (via ``exec``) a tight converter function that maps a
        raw SQLite row-tuple to a Python-typed tuple.  None values are
        passed through without calling converters.  Fields whose
        schematype has no converter are passed through as-is.
        """

        # -- validate field types ------------------------------------------------
        for field in Model.meta.fields:
            if field.type == Model:
                continue  # recursive self-reference
            if field.type is Any:
                continue  # Any is valid for reading, skip validation
            if not self.is_valid_adapttype(field.type):
                raise UnregisteredFieldTypeError(field.type)

        # -- build converter via exec -------------------------------------------
        ns: dict[str, Any] = {}
        parts: list[str] = []
        for i, field in enumerate(Model.meta.fields):
            converter = self._converters.get(field.sql_typename)
            if converter is not None:
                cname = f'_c{i}'
                ns[cname] = converter
                parts.append(f'{cname}(r[{i}]) if r[{i}] is not None else None')
            else:
                parts.append(f'r[{i}]')

        body = ', '.join(parts)
        func_code = f'def _convert(r):\n    return ({body},)'
        exec(func_code, ns)
        converter_func = ns['_convert']

        self._model_converters[Model] = converter_func
        return converter_func

    def get_model_converter(self, Model: RowMeta) -> Callable[[apsw.SQLiteValues], tuple]:
        """Return the cached converter for *Model*, building one if needed."""
        try:
            return self._model_converters[Model]
        except KeyError:
            return self.make_converter_for_model(Model)

    def _convert_binding(self, _: apsw.Cursor, __: int, value: Any) -> apsw.SQLiteValue:
        # TODO: I think we could make this smarter by storing the adapters for a specific Model as a tuple and indexing into it, instead of calling adapt_value each time
        # TODO: also could we put this as a def on the cursor class itself?
        return self.adapt_value(value)

    class AdaptConvertCursor(apsw.Cursor):
        def __init__(self, connection: apsw.Connection, ac_registry: AdaptConvertRegistry):
            super().__init__(connection)
            self.factory = ac_registry
            self.convert_binding = ac_registry._convert_binding  # adapt callback

    def register_adapt_convert[D, V: apsw.SQLiteValue | bytes](self, AdaptConvertType: type[D], adapt: Callable[[D], V], convert: Callable[[V], D]) -> None:
        if not isinstance(AdaptConvertType, type):
            raise InvalidAdaptConvertType(AdaptConvertType)

        self._adapters[AdaptConvertType] = adapt
        self._converters[schematype(AdaptConvertType)] = convert

    def register_pickleable_adapt_convert(self, AdaptConvertType: type, *, overwrite: bool = True) -> None:
        self.register_adapt_convert(
            AdaptConvertType,
            adapt=lambda obj: pickle.dumps(obj),
            convert=lambda data: pickle.loads(data),
        )

    def register_included_adaptconverters(self, Types: Iterable[type]) -> None:
        """Register multiple standard adapt/convert pairs at once"""
        for Type in Types:
            match included_adapt_convert_types.get(Type):
                case None:
                    raise InvalidAdaptConvertType(Type)
                case adapt, convert:
                    self.register_adapt_convert(Type, adapt, convert)


included_adapt_convert_types: dict[type, tuple[Callable, Callable]] = {
    bool: (
        lambda boolean: boolean,
        lambda integer: bool(integer),
    ),
    list: (
        lambda obj: json.dumps(obj),
        lambda string: json.loads(string),
    ),
    dict: (
        lambda obj: json.dumps(obj),
        lambda string: json.loads(string),
    ),
    dt.date: (
        lambda date: date.isoformat(),
        lambda string: dt.date.fromisoformat(string),
    ),
    dt.datetime: (
        lambda datetime: datetime.isoformat(),
        lambda string: dt.datetime.fromisoformat(string),
    ),
}
