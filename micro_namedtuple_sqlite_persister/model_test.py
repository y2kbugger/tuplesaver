from __future__ import annotations

from typing import NamedTuple, Optional, Union

import pytest

from .model import clear_modelmeta_registrations, column_definition, get_meta, get_sqltypename, is_registered_row_model, is_row_model, unwrap_optional_type
from .persister import Engine


def test_unwrap_optional_type() -> None:
    # Non-optional hint
    assert unwrap_optional_type(int) == (False, int)

    # Show that any pair optional syntaxs are == equivalent
    assert Union[int, None] == Optional[int]
    assert Union[int, None] == int | None
    assert Optional[int] == int | None
    assert Optional[int] == Union[int, None]
    assert int | None == Union[int, None]
    assert int | None == Optional[int]

    # Simple standard optional hints
    assert unwrap_optional_type(Union[int, None]) == (True, int)
    assert unwrap_optional_type(Optional[int]) == (True, int)
    assert unwrap_optional_type(int | None) == (True, int)

    # Unions including more than one type in addition to None
    assert unwrap_optional_type(Union[int, str, None]) == (True, int | str)
    assert unwrap_optional_type(int | str | None) == (True, int | str)

    # Unions not including None
    U = Union[int, str]
    UT = int | str
    assert U == UT
    assert unwrap_optional_type(U) == (False, int | str)
    assert unwrap_optional_type(UT) == (False, int | str)

    # Types nested within optional
    assert unwrap_optional_type(Union[U, None]) == (True, int | str)
    assert unwrap_optional_type(Optional[U]) == (True, int | str)
    assert unwrap_optional_type((U) | None) == (True, int | str)

    assert unwrap_optional_type(Union[UT, None]) == (True, int | str)
    assert unwrap_optional_type(Optional[UT]) == (True, int | str)
    assert unwrap_optional_type((UT) | None) == (True, int | str)

    # Nest unions are flattened and deduped and thus nested optionals are not preserved
    OU = Optional[Union[int, None]]
    OUT = Optional[int | None]
    assert OU == OUT
    assert unwrap_optional_type(Union[OU, None]) == (True, (int))
    assert unwrap_optional_type(Optional[OU]) == (True, (int))
    assert unwrap_optional_type((OU) | None) == (True, (int))

    assert unwrap_optional_type(Union[OUT, None]) == (True, (int))
    assert unwrap_optional_type(Optional[OUT]) == (True, (int))


def test_is_row_model() -> None:
    assert is_row_model(int) is False
    assert is_row_model(str) is False
    assert is_row_model(float) is False
    assert is_row_model(bytes) is False
    assert is_row_model(None) is False
    assert is_row_model(tuple) is False
    assert is_row_model(Union[int, str]) is False
    assert is_row_model(int | None) is False

    class Model(NamedTuple):
        id: int
        name: str

    assert is_row_model(Model) is True
    assert is_row_model(Model | None) is False  # you have to unwrap it yourself
    assert is_row_model(Model | int) is False  # invalid


def test_is_registered_row_model(engine: Engine) -> None:
    class Model(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(Model) is False

    engine.ensure_table_created(Model)

    assert is_registered_row_model(Model) is True


def test_get_sqltypename() -> None:
    assert get_sqltypename(int) == "INTEGER"
    assert get_sqltypename(str) == "TEXT"
    assert get_sqltypename(float) == "REAL"
    assert get_sqltypename(bytes) == "BLOB"

    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert get_sqltypename(ModelA) == "ModelA_ID"


def test_get_sqltypename_registered_only(engine: Engine) -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert get_sqltypename(ModelA, registered_only=True) is None

    engine.ensure_table_created(ModelA)

    assert get_sqltypename(ModelA, registered_only=True) == "ModelA_ID"


def test_get_meta() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    meta = get_meta(ModelA)
    assert meta is not None
    assert meta.annotations == {"id": int | None, "name": str}
    assert meta.unwrapped_field_types == (int, str)
    assert meta.select == "SELECT id, name FROM ModelA WHERE id = ?"
    assert meta.Model == ModelA


def test_clear_modelmeta_registrations(engine: Engine) -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(ModelA) is False
    _meta = get_meta(ModelA)
    assert is_registered_row_model(ModelA) is True

    clear_modelmeta_registrations()

    assert is_registered_row_model(ModelA) is False


def test_meta_registers_row_model() -> None:
    class Model(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(Model) is False

    _meta = get_meta(Model)

    assert is_registered_row_model(Model) is True


def test_column_definition() -> None:
    assert column_definition(("id", int | None)) == "id [INTEGER] PRIMARY KEY NOT NULL"
    with pytest.raises(TypeError):
        column_definition(("id", int | str))
    with pytest.raises(TypeError):
        column_definition(("id", int))

    assert column_definition(("count", int)) == "count [INTEGER] NOT NULL"
    assert column_definition(("value", float)) == "value [REAL] NOT NULL"
    assert column_definition(("name", str)) == "name [TEXT] NOT NULL"
    assert column_definition(("data", bytes)) == "data [BLOB] NOT NULL"

    assert column_definition(("count", int | None)) == "count [INTEGER] NULL"
    assert column_definition(("value", float | None)) == "value [REAL] NULL"
    assert column_definition(("name", str | None)) == "name [TEXT] NULL"
    assert column_definition(("data", bytes | None)) == "data [BLOB] NULL"

    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert column_definition(("moda", ModelA)) == "moda [ModelA_ID] NOT NULL"
    assert column_definition(("moda", ModelA | None)) == "moda [ModelA_ID] NULL"
