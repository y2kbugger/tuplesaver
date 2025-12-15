from __future__ import annotations

import datetime as dt
import json
import logging
import pickle
import sqlite3
from collections.abc import Callable, Iterable
from typing import Any

logger = logging.getLogger(__name__)


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType}` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


class AdaptConvertTypeAlreadyRegistered(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or already has an Adapt Convert registered")


adaptconvert_columntypes = {}
adapters = {}
converters = {}


def clear_adapt_convert_registrations() -> None:
    while adaptconvert_columntypes:
        AdaptConvertType, field_type_name = adaptconvert_columntypes.popitem()
        sqlite3.adapters.pop((AdaptConvertType, sqlite3.PrepareProtocol), None)
        sqlite3.converters.pop(field_type_name, None)
        print(f"Cleared adapt/convert for {AdaptConvertType} ({field_type_name})")
    print("Remaining adapters:", sqlite3.adapters)
    print("Remaining converters:", sqlite3.converters)


def register_adapt_convert[D](AdaptConvertType: type[D], adapt: Callable[[D], bytes | str], convert: Callable[[bytes], D], *, overwrite: bool = False) -> None:
    if type(AdaptConvertType) is not type:
        raise InvalidAdaptConvertType(AdaptConvertType)

    if AdaptConvertType in adaptconvert_columntypes and not overwrite:
        raise AdaptConvertTypeAlreadyRegistered(AdaptConvertType)
    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__qualname__}"
    sqlite3.register_adapter(AdaptConvertType, adapt)
    sqlite3.register_converter(field_type_name, convert)
    adaptconvert_columntypes[AdaptConvertType] = field_type_name
    adapters[AdaptConvertType] = adapt
    converters[field_type_name] = convert


def register_pickleable_adapt_convert(AdaptConvertType: type, *, overwrite: bool = True) -> None:
    register_adapt_convert(
        AdaptConvertType,
        adapt=lambda obj: pickle.dumps(obj),
        convert=lambda data: pickle.loads(data),
        overwrite=overwrite,
    )


included_adapt_converters: dict[type, tuple[Callable[[Any], bytes | str], Callable[[bytes], Any]]] = {
    bool: (
        lambda boolean: int(boolean).to_bytes(1),
        lambda data: bool(int.from_bytes(data)),
    ),
    list: (
        lambda obj: json.dumps(obj),
        lambda data: json.loads(data),
    ),
    dict: (
        lambda obj: json.dumps(obj),
        lambda data: json.loads(data),
    ),
    dt.date: (
        lambda date: date.isoformat(),
        lambda data: dt.date.fromisoformat(data.decode()),
    ),
    dt.datetime: (
        lambda datetime: datetime.isoformat(),
        lambda data: dt.datetime.fromisoformat(data.decode()),
    ),
}


def register_standard_adaptconverters(Types: Iterable[type] | None = None, *, overwrite: bool = True) -> None:
    """Enable the standard adapt/converters:
    bool, list, dict, dt.date, dt.datetime

    You can pass a list of types to select which ones to register.
    """
    if Types is None:
        Types = included_adapt_converters.keys()

    for Type in Types:
        adapt, convert = included_adapt_converters[Type]
        register_adapt_convert(Type, adapt, convert, overwrite=overwrite)
