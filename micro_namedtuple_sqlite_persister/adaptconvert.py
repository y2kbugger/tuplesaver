from __future__ import annotations

import datetime as dt
import logging
import pickle
import sqlite3
from collections.abc import Callable, Iterable
from typing import Any

logger = logging.getLogger(__name__)


adaptconvert_columntypes = {}


def clear_adapt_convert_registrations() -> None:
    while adaptconvert_columntypes:
        AdaptConvertType, field_type_name = adaptconvert_columntypes.popitem()
        sqlite3.adapters.pop((AdaptConvertType, sqlite3.PrepareProtocol), None)
        sqlite3.converters.pop(field_type_name, None)


class InvalidAdaptConvertType(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(
            f"AdaptConvertType {AdaptConvertType} is not a valid type for persisting. `{AdaptConvertType})` must be an instance of `type` but instead is `{type(AdaptConvertType)}`"
        )


class AdaptConvertTypeAlreadyRegistered(Exception):
    def __init__(self, AdaptConvertType: type) -> None:
        super().__init__(f"Persistance format for {AdaptConvertType} already exists. It is a native type (int, float, str, bytes) or already has an Adapt Convert registered")


def register_adapt_convert[D](AdaptConvertType: type[D], adapt: Callable[[D], bytes], convert: Callable[[bytes], D], overwrite: bool = False) -> None:
    if type(AdaptConvertType) is not type:
        raise InvalidAdaptConvertType(AdaptConvertType)

    if AdaptConvertType in adaptconvert_columntypes and not overwrite:
        raise AdaptConvertTypeAlreadyRegistered(AdaptConvertType)

    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__qualname__}"
    sqlite3.register_adapter(AdaptConvertType, adapt)
    sqlite3.register_converter(field_type_name, convert)
    adaptconvert_columntypes[AdaptConvertType] = field_type_name


included_adapt_converters: dict[type, tuple[Callable[[Any], bytes], Callable[[bytes], Any]]] = {
    dt.datetime: (
        lambda datetime: datetime.isoformat().encode(),
        lambda data: dt.datetime.fromisoformat(data.decode()),
    ),
    dt.date: (
        lambda date: date.isoformat().encode(),
        lambda data: dt.date.fromisoformat(data.decode()),
    ),
}

try:
    import pandas as pd

    def adapt_df(obj: pd.DataFrame) -> bytes:
        return pickle.dumps(obj)

    def convert_df(data: bytes) -> pd.DataFrame:
        return pickle.loads(data)

    included_adapt_converters[pd.DataFrame] = (adapt_df, convert_df)
except ImportError:
    pass


def enable_included_adaptconverters(Types: Iterable[type] | None = None) -> None:
    """Enable the included adapt/converters for the given types

    If no types are given, all included adapt/converters will be enabled
    """
    if Types is None:
        Types = included_adapt_converters.keys()

    for Type in Types:
        adapt, convert = included_adapt_converters[Type]
        register_adapt_convert(Type, adapt, convert)
