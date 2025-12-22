from __future__ import annotations

import datetime as dt
from typing import Optional

import pytest

from .adaptconvert import (
    InvalidAdaptConvertType,
)
from .engine import Engine
from .RM import Roww


def test_registering_adapt_convert_pair(engine: Engine) -> None:
    class NewType:
        def __init__(self, values: list[str]) -> None:
            self.values = values

    class ModelUnknownType(Roww):
        id: int | None
        name: str
        custom: NewType

    def adapt_newtype(value: NewType) -> bytes:
        return ",".join(value.values).encode()

    def convert_newtype(value: bytes) -> NewType:
        return NewType(value.decode().split(","))

    engine.adapt_convert_registry.register_adapt_convert(NewType, adapt_newtype, convert_newtype)

    ### Table Creation
    engine.ensure_table_created(ModelUnknownType)

    cursor = engine.connection.cursor()
    cursor.execute(f"PRAGMA table_info({ModelUnknownType.__name__});")
    columns = cursor.fetchall()
    assert len(columns) == len(ModelUnknownType._fields)
    assert columns[2][1] == "custom"  # Column Name
    assert columns[2][2] == "tuplesaver.adaptconvert_test.test_registering_adapt_convert_pair.<locals>.NewType"  # Column Type
    assert columns[2][3] == 1  # Not Null
    assert columns[2][5] == 0  # Not Primary Key

    ### Adapt
    row = ModelUnknownType(None, "Alice", NewType(["a", "b", "c"]))
    row = engine.save(row)

    cursor.execute(f"SELECT substr(custom,0) FROM {ModelUnknownType.__name__};")  # substr converting to NewType with converter
    rows = cursor.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == b'a,b,c'  # adapted to a binary format

    ### Convert
    retrieved_row = engine.find(ModelUnknownType, row.id)
    assert type(retrieved_row.custom) is NewType
    assert retrieved_row.custom.values == ["a", "b", "c"]


def test_attempted_registration_of_concrete_obj_raises(engine: Engine) -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    with pytest.raises(InvalidAdaptConvertType):
        engine.adapt_convert_registry.register_adapt_convert("Blah", adapt_newtype, convert_newtype)  # type: ignore


def test_attempted_registration_of_an_union_raises(engine: Engine) -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    with pytest.raises(InvalidAdaptConvertType):
        engine.adapt_convert_registry.register_adapt_convert(Optional[NewType], adapt_newtype, convert_newtype)  # type: ignore


def test_attempted_registration_of_already_registered_type(engine: Engine) -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    def adapt_newtype2(value: NewType) -> bytes:
        return b''

    def convert_newtype2(value: bytes) -> NewType:
        return NewType()

    engine.adapt_convert_registry.register_adapt_convert(NewType, adapt_newtype, convert_newtype)

    # verify the registration
    assert engine.adapt_convert_registry._adapters[NewType] is adapt_newtype
    assert engine.adapt_convert_registry._converters['tuplesaver.adaptconvert_test.test_attempted_registration_of_already_registered_type.<locals>.NewType'] is convert_newtype

    engine.adapt_convert_registry.register_adapt_convert(NewType, adapt_newtype2, convert_newtype2)
    # verify that the registration was overwritten
    assert engine.adapt_convert_registry._adapters[NewType] is adapt_newtype2
    assert engine.adapt_convert_registry._converters['tuplesaver.adaptconvert_test.test_attempted_registration_of_already_registered_type.<locals>.NewType'] is convert_newtype2


def test_can_store_and_retrieve_datetime_as_iso(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        date: dt.datetime

    engine.ensure_table_created(T)
    now = dt.datetime.now()
    row = engine.save(T(None, now))

    returned_row = engine.find(T, row.id)

    assert returned_row.date == now


def test_can_store_and_retrieve_date_as_iso(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        date: dt.date

    engine.ensure_table_created(T)
    today = dt.date.today()
    row = engine.save(T(None, today))

    returned_row = engine.find(T, row.id)

    assert returned_row.date == today


def test_can_store_and_retrieve_bool_as_int(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        flag: bool

    engine.ensure_table_created(T)
    row = engine.save(T(None, True))

    returned_row = engine.find(T, row.id)

    assert returned_row.flag is True

    row = engine.save(T(None, False))

    returned_row = engine.find(T, row.id)

    assert returned_row.flag is False


def test_can_store_and_retrieve_list_as_json(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        names: list

    engine.ensure_table_created(T)
    names = ["Alice", "Bob", "Charlie", 2]
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_can_store_and_retrieve_dict_as_json(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        names: dict

    engine.ensure_table_created(T)
    names = {"Alice": 1, "Bob": 2, "Charlie": 3}
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_raises_on_json_when_nonserializeable(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        dates: list

    engine.ensure_table_created(T)

    with pytest.raises(TypeError, match="Object of type datetime is not JSON serializable"):
        engine.save(T(None, [dt.datetime.now()]))


class PickleableTestType:
    __slots__ = ("value",)

    def __init__(self, value: int):
        self.value = value


def test_can_store_and_retrieve_pickleable_type(engine: Engine) -> None:
    class T(Roww):
        id: int | None
        data: PickleableTestType

    engine.adapt_convert_registry.register_pickleable_adapt_convert(PickleableTestType)
    engine.ensure_table_created(T)

    sentinel_instance = PickleableTestType(42)
    row = engine.save(T(None, sentinel_instance))
    returned_row = engine.find(T, row.id)

    assert returned_row.data.value == sentinel_instance.value
