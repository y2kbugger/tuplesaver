from __future__ import annotations

import datetime as dt
import json
import logging
import pickle
from collections.abc import Callable, Iterable
from typing import Any

import apsw

from .model import is_row_model, native_columntypes, schematype

logger = logging.getLogger(__name__)


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType}` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


class AdaptConvertTypeAlreadyRegistered(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or already has an Adapt Convert registered")


class AdaptConvertRegistry:
    """Provides cursors that can convert objects into one of the types supported by SQLite, or back from SQLite"""

    def __init__(self):
        self._adapters: dict[type, Callable] = {}
        self._converters: dict[str, Callable] = {}

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

    def convert_value(self, schematype: str, value: apsw.SQLiteValue) -> Any:
        "Returns Python object from schema type and SQLite value"
        converter = self._converters.get(schematype)
        if not converter:
            return value
        return converter(value)

    def _convert_binding(self, _: apsw.Cursor, __: int, value: Any) -> apsw.SQLiteValue:
        # TODO: I think we could make this smarter by storing the adapters for a specific Model as a tuple and indexing into it, instead of calling adapt_value each time
        # TODO: also could we put this as a def on the cursor class itself?
        return self.adapt_value(value)

    class AdaptConvertCursor(apsw.Cursor):
        def __init__(self, connection: apsw.Connection, ac_registry: AdaptConvertRegistry):
            super().__init__(connection)
            self.factory = ac_registry
            self.convert_binding = ac_registry._convert_binding  # adapt callback
            self.row_trace = self._row_converter  # convert callback

        def _row_converter(self, cursor: apsw.Cursor, values: apsw.SQLiteValues) -> tuple[Any, ...]:
            return tuple(self.factory.convert_value(d[1], v) for d, v in zip(cursor.get_description(), values, strict=True))

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
