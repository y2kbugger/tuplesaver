from __future__ import annotations

import datetime as dt
from typing import NamedTuple, Optional, Union

import pytest

from .model import (
    FieldZeroIdMalformed,
    FieldZeroIdRequired,
    Meta,
    MetaField,
    _sql_columndef,
    _unwrap_optional_type,
    get_meta,
    is_row_model,
    schematype,
)
from .RM import Roww


def test_unwrap_optional_type() -> None:
    # Non-optional hint
    assert _unwrap_optional_type(int) == (False, int)

    # Show that any pair optional syntaxs are == equivalent
    assert Union[int, None] == Optional[int]  # noqa: UP007, UP045
    assert Union[int, None] == int | None  # noqa: UP007
    assert Optional[int] == int | None  # noqa: UP045
    assert Optional[int] == Union[int, None]  # noqa: UP007, UP045
    assert int | None == Union[int, None]  # noqa: UP007
    assert int | None == Optional[int]  # noqa: UP045

    # Simple standard optional hints
    assert _unwrap_optional_type(Union[int, None]) == (True, int)  # noqa: UP007
    assert _unwrap_optional_type(Optional[int]) == (True, int)  # noqa: UP045
    assert _unwrap_optional_type(int | None) == (True, int)

    # Unions including more than one type in addition to None
    assert _unwrap_optional_type(Union[int, str, None]) == (True, int | str)  # noqa: UP007
    assert _unwrap_optional_type(int | str | None) == (True, int | str)

    # Unions not including None
    U = Union[int, str]  # noqa: UP007
    UT = int | str
    assert U == UT
    assert _unwrap_optional_type(U) == (False, int | str)
    assert _unwrap_optional_type(UT) == (False, int | str)

    # Types nested within optional
    assert _unwrap_optional_type(Union[U, None]) == (True, int | str)  # noqa: UP007
    assert _unwrap_optional_type(Optional[U]) == (True, int | str)  # noqa: UP045
    assert _unwrap_optional_type((U) | None) == (True, int | str)

    assert _unwrap_optional_type(Union[UT, None]) == (True, int | str)  # noqa: UP007
    assert _unwrap_optional_type(Optional[UT]) == (True, int | str)  # noqa: UP045
    assert _unwrap_optional_type((UT) | None) == (True, int | str)

    # Nest unions are flattened and deduped and thus nested optionals are not preserved
    OU = Optional[int | None]  # noqa: UP045
    OUT = Optional[int | None]  # noqa: UP045
    assert OU == OUT
    assert _unwrap_optional_type(Union[OU, None]) == (True, (int))  # noqa: UP007
    assert _unwrap_optional_type(Optional[OU]) == (True, (int))  # noqa: UP045
    assert _unwrap_optional_type((OU) | None) == (True, (int))

    assert _unwrap_optional_type(Union[OUT, None]) == (True, (int))  # noqa: UP007
    assert _unwrap_optional_type(Optional[OUT]) == (True, (int))  # noqa: UP045


def test_is_row_model() -> None:
    assert is_row_model(int) is False
    assert is_row_model(str) is False
    assert is_row_model(float) is False
    assert is_row_model(bytes) is False
    assert is_row_model(None) is False
    assert is_row_model(tuple) is False
    assert is_row_model(int | str) is False
    assert is_row_model(int | None) is False

    class Model(Roww):
        id: int | None
        name: str

    assert is_row_model(Model) is True
    assert is_row_model(Model) is True
    assert is_row_model(Model | None) is False  # you have to unwrap it yourself
    assert is_row_model(Model | int) is False  # invalid

    class NTModel(NamedTuple):
        id: int | None
        name: str

    assert is_row_model(NTModel) is False

    import dataclasses

    @dataclasses.dataclass
    class DCModel:
        id: int | None
        name: str

    assert is_row_model(DCModel) is False


def test_get_sqltypename() -> None:
    assert schematype(int) == "INTEGER"
    assert schematype(str) == "TEXT"
    assert schematype(float) == "REAL"
    assert schematype(bytes) == "BLOB"
    assert schematype(bool) == "builtins.bool"
    assert schematype(list) == "builtins.list"
    assert schematype(dict) == "builtins.dict"
    # this also tests that inheritance hierarchy doesn't disrupt column type resolution
    assert schematype(dt.date) == "datetime.date"
    assert schematype(dt.datetime) == "datetime.datetime"

    # Test related models as fields
    class ModelA(Roww):
        id: int | None
        name: str

    assert schematype(ModelA) == "ModelA_ID"


def test_get_meta__valid_table_model() -> None:
    class ModelA(Roww):
        id: int | None
        name: str

    assert get_meta(ModelA) == Meta(
        Model=ModelA,
        model_name="ModelA",
        table_name="ModelA",
        fields=(
            MetaField(name="id", type=int, full_type=int | None, nullable=True, is_fk=False, is_pk=True, sql_typename="INTEGER", sql_columndef="id [INTEGER] PRIMARY KEY NOT NULL"),
            MetaField(name="name", type=str, full_type=str, nullable=False, is_fk=False, is_pk=False, sql_typename="TEXT", sql_columndef="name [TEXT] NOT NULL"),
        ),
    )


def test_column_definition() -> None:
    assert _sql_columndef('id', True, int) == "id [INTEGER] PRIMARY KEY NOT NULL"
    with pytest.raises(FieldZeroIdMalformed):
        _sql_columndef('id', False, int)

    assert _sql_columndef("value", False, float) == "value [REAL] NOT NULL"
    assert _sql_columndef("value", True, float) == "value [REAL] NULL"

    class ModelA(Roww):
        id: int | None
        name: str

    assert _sql_columndef("moda", False, ModelA) == "moda [ModelA_ID] NOT NULL"
    assert _sql_columndef("moda", True, ModelA) == "moda [ModelA_ID] NULL"


def test_meta__model_missing_id() -> None:
    class TBadID(Roww):
        name: str

    with pytest.raises(FieldZeroIdRequired):
        get_meta(TBadID)


def test_meta__model_malformed_id_raises() -> None:
    class TBadID(Roww):
        id: str | None  # id is not int
        name: str

    with pytest.raises(FieldZeroIdMalformed):
        get_meta(TBadID)


def test_meta__model_id_not_optional() -> None:
    class TBadID(Roww):
        id: int  # id is not optional
        name: str

    with pytest.raises(FieldZeroIdMalformed):
        get_meta(TBadID)


def test_table_meta___related_model() -> None:
    class A(Roww):
        id: int | None
        name: str

    class B(Roww):
        id: int | None
        name: str
        unknown: A

    get_meta(B)


def test_table_meta__related_model_containing_class_declared_first() -> None:
    """This works because we made M._meta a lazy lambda, which initializes only when first accessed."""

    class B(Roww):
        id: int | None
        name: str
        unknown: A

    class A(Roww):
        id: int | None
        name: str

    get_meta(B)


def test_table_meta__related_model_recursive() -> None:
    class A(Roww):
        id: int | None
        name: str
        a: A | None

    get_meta(A)


def test_table_meta__unregistered_field_type__doesnt_raise() -> None:
    class NewType: ...

    class ModelUnknownType(Roww):
        id: int | None
        name: str
        unknown: NewType

    get_meta(ModelUnknownType)
