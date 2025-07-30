from __future__ import annotations

import datetime as dt
from typing import Any, NamedTuple, Optional, Union

import pytest

from .model import (
    FieldZeroIdMalformed,
    FieldZeroIdRequired,
    Meta,
    MetaField,
    UnregisteredFieldTypeError,
    _sql_columndef,
    _sql_typename,
    _unwrap_optional_type,
    clear_modelmeta_registrations,
    get_meta,
    get_table_meta,
    is_registered_row_model,
    is_registered_table_model,
    is_row_model,
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
    assert _sql_typename(bool) == "builtins.bool"
    assert _sql_typename(list) == "builtins.list"
    assert _sql_typename(dict) == "builtins.dict"
    # this also tests that inheritance hierarchy doesn't disrupt column type resolution
    assert _sql_typename(dt.date) == "datetime.date"
    assert _sql_typename(dt.datetime) == "datetime.datetime"

    # Test related models as fields
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert _sql_typename(ModelA) == "ModelA_ID"


def test_get_meta__valid_table_model_but_is_not_yet_registered() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert get_meta(ModelA) == Meta(
        Model=ModelA,
        model_name="ModelA",
        table_name="ModelA",
        is_table=False,
        fields=(
            MetaField(name="id", type=int, full_type=int | None, nullable=True, is_fk=False, is_pk=True, sql_typename="INTEGER", sql_columndef="id [INTEGER] PRIMARY KEY NOT NULL"),
            MetaField(name="name", type=str, full_type=str, nullable=False, is_fk=False, is_pk=False, sql_typename="TEXT", sql_columndef="name [TEXT] NOT NULL"),
        ),
    )


def test_get_meta__table_meta_registered() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    with get_table_meta(ModelA) as _meta:
        pass  # this is where we would normally try to create the table.

    assert get_meta(ModelA) == Meta(
        Model=ModelA,
        model_name="ModelA",
        table_name="ModelA",
        is_table=True,
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

    with get_table_meta(ModelA) as _meta:
        pass  # this is where we would normally try to create the table.

    class ModelA_NameOnly(NamedTuple):
        id: int | None
        name: str

    assert get_meta(ModelA_NameOnly) == Meta(
        Model=ModelA_NameOnly,
        model_name="ModelA_NameOnly",
        table_name="ModelA",
        is_table=False,
        fields=(
            MetaField(name="id", type=int, full_type=int | None, nullable=True, is_fk=False, is_pk=True, sql_typename="INTEGER", sql_columndef="id [INTEGER] PRIMARY KEY NOT NULL"),
            MetaField(name="name", type=str, full_type=str, nullable=False, is_fk=False, is_pk=False, sql_typename="TEXT", sql_columndef="name [TEXT] NOT NULL"),
        ),
    )


def test_get_adhoc_meta() -> None:
    class AdHocModel(NamedTuple):
        score: float

    assert get_meta(AdHocModel) == Meta(
        Model=AdHocModel,
        model_name="AdHocModel",
        table_name=None,
        is_table=False,
        fields=(MetaField(name="score", type=float, full_type=float, nullable=False, is_fk=False, is_pk=False, sql_typename="REAL", sql_columndef="score [REAL] NOT NULL"),),
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

    with get_table_meta(ModelA) as _meta:
        pass  # this is where we would normally try to create the table.

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
    with pytest.raises(FieldZeroIdMalformed):
        _sql_columndef('id', False, int)

    assert _sql_columndef("value", False, float) == "value [REAL] NOT NULL"
    assert _sql_columndef("value", True, float) == "value [REAL] NULL"

    class ModelA(NamedTuple):
        id: int | None
        name: str

    assert _sql_columndef("moda", False, ModelA) == "moda [ModelA_ID] NOT NULL"
    assert _sql_columndef("moda", True, ModelA) == "moda [ModelA_ID] NULL"


def test_meta__model_missing_id() -> None:
    class TBadID(NamedTuple):
        name: str

    get_meta(TBadID)  # this is ok, view models don't need an id


def test_meta__model_malformed_id_raises() -> None:
    class TBadID(NamedTuple):
        id: str | None  # id is not int
        name: str

    with pytest.raises(FieldZeroIdMalformed):
        get_meta(TBadID)  # even though this is a view model, if it has an id, it must be int | None


def test_table_meta__model_missing_id() -> None:
    class TBadID(NamedTuple):
        name: str

    with pytest.raises(FieldZeroIdRequired):  # noqa: SIM117
        with get_table_meta(TBadID) as _meta:
            pass  # this is where we would normally try to create the table.


def test_table_meta__id_isnt_int() -> None:
    class TBadID(NamedTuple):
        id: str | None  # id is not int
        name: str

    with pytest.raises(FieldZeroIdMalformed):  # noqa: SIM117
        with get_table_meta(TBadID) as _meta:
            pass  # this is where we would normally try to create the table.


def test_table_meta__id_isnt_optional() -> None:
    class TBadID(NamedTuple):
        id: int  # id is not optional
        name: str

    with pytest.raises(FieldZeroIdMalformed):  # noqa: SIM117
        with get_table_meta(TBadID) as _meta:
            pass  # this is where we would normally try to create the table.


def test_table_meta__unregistered_related_model() -> None:
    # TODO: one day we could recursively register the unknown model, but for now we just raise an error
    class UnregisteredTableModel(NamedTuple):
        id: int | None
        name: str

    class ModelWithUnregisteredTableModelField(NamedTuple):
        id: int | None
        name: str
        unknown: UnregisteredTableModel

    with pytest.raises(UnregisteredFieldTypeError, match="is a NamedTuple Row Model"):
        get_meta(ModelWithUnregisteredTableModelField)


def test_table_meta__unregistered_field_type() -> None:
    class NewType: ...

    class ModelUnknownType(NamedTuple):
        id: int | None
        name: str
        unknown: NewType

    with pytest.raises(UnregisteredFieldTypeError, match="has not been registered with an adapter and converter"):
        get_meta(ModelUnknownType)


def test_table_meta__failed_table_meta_context__meta_is_not_registered() -> None:
    class ModelA(NamedTuple):
        id: int | None
        name: str

    with pytest.raises(RuntimeError, match="This is a test"):  # noqa: SIM117
        # ensure that get_table_meta indeed bubbles up exceptions by re-raising them.
        with get_table_meta(ModelA) as _meta:
            raise RuntimeError("This is a test to ensure that the meta is not registered if the context fails")

    # After the context, the meta should not be registered
    assert is_registered_row_model(ModelA) is False
    assert is_registered_table_model(ModelA) is False

    # ok, let's try to register it again, successfully this time
    with get_table_meta(ModelA) as _meta:
        pass

    # After the context, the meta should not be registered
    assert is_registered_row_model(ModelA) is True
    assert is_registered_table_model(ModelA) is True


def test_get_meta__any_type__is_permitted() -> None:
    class T(NamedTuple):
        id: int | None
        data: Any

    get_meta(T)


def test_get_meta__optional_any_type__is_prohibited() -> None:
    class T(NamedTuple):
        id: int | None
        data: Any | None

    with pytest.raises(UnregisteredFieldTypeError, match="is not a valid type for persisting"):
        get_meta(T)


def test_get_table_meta__any_type__is_prohibited() -> None:
    class T(NamedTuple):
        id: int | None
        data: Any

    with pytest.raises(UnregisteredFieldTypeError, match="is not a valid type for persisting"):  # noqa: SIM117
        with get_table_meta(T) as _meta:
            pass  # this is where we would normally try to create the table.


def test_get_table_meta__optional_any_type__is_prohibited() -> None:
    class T(NamedTuple):
        id: int | None
        data: Any | None

    with pytest.raises(UnregisteredFieldTypeError, match="is not a valid type for persisting"):  # noqa: SIM117
        with get_table_meta(T) as _meta:
            pass  # this is where we would normally try to create the table.
