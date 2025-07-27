from __future__ import annotations

import datetime as dt
import sqlite3
from typing import NamedTuple, Optional

import pytest

from .adaptconvert import (
    AdaptConvertTypeAlreadyRegistered,
    InvalidAdaptConvertType,
    clear_adapt_convert_registrations,
    register_adapt_convert,
    register_pickleable_adapt_convert,
)
from .engine import Engine
from .model import UnregisteredFieldTypeError


def test_registering_adapt_convert_pair(engine: Engine) -> None:
    class NewType:
        def __init__(self, values: list[str]) -> None:
            self.values = values

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        custom: NewType

    def adapt_newtype(value: NewType) -> bytes:
        return ",".join(value.values).encode()

    def convert_newtype(value: bytes) -> NewType:
        return NewType(value.decode().split(","))

    register_adapt_convert(NewType, adapt_newtype, convert_newtype)

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


def test_attempted_registration_of_concrete_obj_raises() -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    with pytest.raises(InvalidAdaptConvertType):
        register_adapt_convert("Blah", adapt_newtype, convert_newtype)  # type: ignore


def test_attempted_registration_of_an_union_raises() -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    with pytest.raises(InvalidAdaptConvertType):
        register_adapt_convert(Optional[NewType], adapt_newtype, convert_newtype)  # type: ignore


def test_attempted_registration_of_already_registered_type() -> None:
    class NewType: ...

    def adapt_newtype(value: NewType) -> bytes:
        return b''

    def convert_newtype(value: bytes) -> NewType:
        return NewType()

    def adapt_newtype2(value: NewType) -> bytes:
        return b''

    def convert_newtype2(value: bytes) -> NewType:
        return NewType()

    register_adapt_convert(NewType, adapt_newtype, convert_newtype)

    # verify the registration
    assert sqlite3.adapters[(NewType, sqlite3.PrepareProtocol)] is adapt_newtype
    assert sqlite3.converters['TUPLESAVER.ADAPTCONVERT_TEST.TEST_ATTEMPTED_REGISTRATION_OF_ALREADY_REGISTERED_TYPE.<LOCALS>.NEWTYPE'] is convert_newtype

    with pytest.raises(AdaptConvertTypeAlreadyRegistered):
        register_adapt_convert(NewType, adapt_newtype, convert_newtype)

    # Ensure that the original registration was not lost or changed
    assert sqlite3.adapters[(NewType, sqlite3.PrepareProtocol)] is adapt_newtype
    assert sqlite3.converters['TUPLESAVER.ADAPTCONVERT_TEST.TEST_ATTEMPTED_REGISTRATION_OF_ALREADY_REGISTERED_TYPE.<LOCALS>.NEWTYPE'] is convert_newtype

    register_adapt_convert(NewType, adapt_newtype2, convert_newtype2, overwrite=True)
    # verify that the registration was overwritten
    assert sqlite3.adapters[(NewType, sqlite3.PrepareProtocol)] is adapt_newtype2
    assert sqlite3.converters['TUPLESAVER.ADAPTCONVERT_TEST.TEST_ATTEMPTED_REGISTRATION_OF_ALREADY_REGISTERED_TYPE.<LOCALS>.NEWTYPE'] is convert_newtype2


def test_adapter_converter_reset_only_affects_what_we_registered() -> None:
    class NewType: ...

    assert (NewType, sqlite3.PrepareProtocol) not in sqlite3.adapters
    assert 'NEWTYPE' not in sqlite3.converters

    sqlite3.register_adapter(NewType, lambda x: 'x')
    sqlite3.register_converter("NewType", lambda x: 'x')

    assert (NewType, sqlite3.PrepareProtocol) in sqlite3.adapters
    assert 'NEWTYPE' in sqlite3.converters

    clear_adapt_convert_registrations()

    assert (NewType, sqlite3.PrepareProtocol) in sqlite3.adapters
    assert 'NEWTYPE' in sqlite3.converters


def test_can_store_and_retrieve_datetime_as_iso(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        date: dt.datetime

    engine.ensure_table_created(T)
    now = dt.datetime.now()
    row = engine.save(T(None, now))

    returned_row = engine.find(T, row.id)

    assert returned_row.date == now


def test_can_store_and_retrieve_date_as_iso(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        date: dt.date

    engine.ensure_table_created(T)
    today = dt.date.today()
    row = engine.save(T(None, today))

    returned_row = engine.find(T, row.id)

    assert returned_row.date == today


def test_can_store_and_retrieve_bool_as_int(engine: Engine) -> None:
    class T(NamedTuple):
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
    class T(NamedTuple):
        id: int | None
        names: list

    engine.ensure_table_created(T)
    names = ["Alice", "Bob", "Charlie", 2]
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_can_store_and_retrieve_dict_as_json(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        names: dict

    engine.ensure_table_created(T)
    names = {"Alice": 1, "Bob": 2, "Charlie": 3}
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_raises_on_json_when_nonserializeable(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        dates: list

    engine.ensure_table_created(T)

    with pytest.raises(TypeError, match="Object of type datetime is not JSON serializable"):
        engine.save(T(None, [dt.datetime.now()]))


def test_that_unregistered_fieldtype_raises(engine: Engine) -> None:
    from array import array

    class T(NamedTuple):
        id: int | None
        data: array

    with pytest.raises(UnregisteredFieldTypeError):
        engine.ensure_table_created(T)


def test_can_store_and_retrieve_pickleable_type(engine: Engine) -> None:
    from array import array

    class T(NamedTuple):
        id: int | None
        data: array

    register_pickleable_adapt_convert(array)
    engine.ensure_table_created(T)

    data = array('i', [1, 2, 3])
    row = engine.save(T(None, data))

    returned_row = engine.find(T, row.id)

    assert returned_row.data.tolist() == data.tolist()
