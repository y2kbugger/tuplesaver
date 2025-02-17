from __future__ import annotations

from typing import NamedTuple, Optional, Union

import pytest

from .model import (
    Meta,
    MetaField,
    _sql_columndef,
    _sql_typename,
    _unwrap_optional_type,
    clear_modelmeta_registrations,
    get_meta,
    is_registered_row_model,
    is_registered_table_model,
    is_row_model,
    register_table_model,
)


def test_unwrap_optional_type() -> None:
    # Non-optional hint
    assert _unwrap_optional_type(int) == (False, int)

    # Show that any pair optional syntaxs are == equivalent
    assert Union[int, None] == Optional[int]
    assert Union[int, None] == int | None
    assert Optional[int] == int | None
    assert Optional[int] == Union[int, None]
    assert int | None == Union[int, None]
    assert int | None == Optional[int]

    # Simple standard optional hints
    assert _unwrap_optional_type(Union[int, None]) == (True, int)
    assert _unwrap_optional_type(Optional[int]) == (True, int)
    assert _unwrap_optional_type(int | None) == (True, int)

    # Unions including more than one type in addition to None
    assert _unwrap_optional_type(Union[int, str, None]) == (True, int | str)
    assert _unwrap_optional_type(int | str | None) == (True, int | str)

    # Unions not including None
    U = Union[int, str]
    UT = int | str
    assert U == UT
    assert _unwrap_optional_type(U) == (False, int | str)
    assert _unwrap_optional_type(UT) == (False, int | str)

    # Types nested within optional
    assert _unwrap_optional_type(Union[U, None]) == (True, int | str)
    assert _unwrap_optional_type(Optional[U]) == (True, int | str)
    assert _unwrap_optional_type((U) | None) == (True, int | str)

    assert _unwrap_optional_type(Union[UT, None]) == (True, int | str)
    assert _unwrap_optional_type(Optional[UT]) == (True, int | str)
    assert _unwrap_optional_type((UT) | None) == (True, int | str)

    # Nest unions are flattened and deduped and thus nested optionals are not preserved
    OU = Optional[Union[int, None]]
    OUT = Optional[int | None]
    assert OU == OUT
    assert _unwrap_optional_type(Union[OU, None]) == (True, (int))
    assert _unwrap_optional_type(Optional[OU]) == (True, (int))
    assert _unwrap_optional_type((OU) | None) == (True, (int))

    assert _unwrap_optional_type(Union[OUT, None]) == (True, (int))
    assert _unwrap_optional_type(Optional[OUT]) == (True, (int))


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


def test_is_registered_row_model() -> None:
    class Model(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(Model) is False

    _meta = get_meta(Model)

    assert is_registered_row_model(Model) is True


def test_get_sqltypename() -> None:
    assert _sql_typename(int) == "INTEGER"
    assert _sql_typename(str) == "TEXT"
    assert _sql_typename(float) == "REAL"
    assert _sql_typename(bytes) == "BLOB"

    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert _sql_typename(ModelA) == "ModelA_ID"


def test_get_table_meta() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    register_table_model(ModelA)
    assert get_meta(ModelA) == Meta(
        Model=ModelA,
        model_name="ModelA",
        table_name="ModelA",
        is_table=True,
        select="SELECT id, name FROM ModelA WHERE id = ?",
        fields=(
            MetaField(name="id", type=int, full_type=int | None, nullable=True, is_fk=False, is_pk=True, sql_typename="INTEGER", sql_columndef="id [INTEGER] PRIMARY KEY NOT NULL"),
            MetaField(name="name", type=str, full_type=str, nullable=False, is_fk=False, is_pk=False, sql_typename="TEXT", sql_columndef="name [TEXT] NOT NULL"),
        ),
    )


def test_get_alternateview_meta() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str
        score: float

    register_table_model(ModelA)

    class ModelA_NameOnly(NamedTuple):
        id: int | None
        name: str

    assert get_meta(ModelA_NameOnly) == Meta(
        Model=ModelA_NameOnly,
        model_name="ModelA_NameOnly",
        table_name="ModelA",
        is_table=False,
        select="SELECT id, name FROM ModelA WHERE id = ?",
        fields=(
            MetaField(name="id", type=int, full_type=int | None, nullable=True, is_fk=False, is_pk=True, sql_typename="INTEGER", sql_columndef="id [INTEGER] PRIMARY KEY NOT NULL"),
            MetaField(name="name", type=str, full_type=str, nullable=False, is_fk=False, is_pk=False, sql_typename="TEXT", sql_columndef="name [TEXT] NOT NULL"),
        ),
    )


def test_register_table_model() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    _meta = get_meta(ModelA)

    assert is_registered_row_model(ModelA) is True
    assert is_registered_table_model(ModelA) is False

    register_table_model(ModelA)

    assert is_registered_row_model(ModelA) is True
    assert is_registered_table_model(ModelA) is True


def test_clear_modelmeta_registrations() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert is_registered_row_model(ModelA) is False
    _meta = get_meta(ModelA)
    assert is_registered_row_model(ModelA) is True

    clear_modelmeta_registrations()

    assert is_registered_row_model(ModelA) is False


def test_column_definition() -> None:
    assert _sql_columndef('id', True, int) == "id [INTEGER] PRIMARY KEY NOT NULL"
    with pytest.raises(TypeError):
        _sql_columndef('id', False, int)

    assert _sql_columndef("value", False, float) == "value [REAL] NOT NULL"
    assert _sql_columndef("value", True, float) == "value [REAL] NULL"

    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert _sql_columndef("moda", False, ModelA) == "moda [ModelA_ID] NOT NULL"
    assert _sql_columndef("moda", True, ModelA) == "moda [ModelA_ID] NULL"
