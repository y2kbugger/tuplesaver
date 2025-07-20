from __future__ import annotations

import inspect
import logging
import sqlite3
import types
from collections.abc import Iterator
from contextlib import contextmanager
from textwrap import dedent
from typing import Any, NamedTuple, Union, get_args, get_origin, get_type_hints

from .adaptconvert import adaptconvert_columntypes

logger = logging.getLogger(__name__)


type Row = NamedTuple


class ModelDefinitionError(Exception):
    pass


class FieldZeroIdRequired(ModelDefinitionError):
    def __init__(self, model_name: str, field_zero_name: str, field_zero_typehint: Any) -> None:
        super().__init__(
            self,
            f"Field 0 of {model_name} is required to be `id: int | None` but instead is `{field_zero_name}: {field_zero_typehint}`",
        )


class FieldZeroIdMalformed(ModelDefinitionError):
    def __init__(self, field_zero_typehint: Any) -> None:
        super().__init__(
            self,
            f"`id` field is required to be `id: int | None` but instead is `id: {field_zero_typehint}`",
        )


class UnregisteredFieldTypeError(ModelDefinitionError):
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


def is_row_model(cls: object) -> bool:
    """Test at runtime whether an object is a Row, e.g. a NamedTuple model"""
    if not isinstance(cls, type):
        return False

    if not issubclass(cls, tuple):
        return False

    try:
        if object.__getattribute__(cls, '_fields')[0] == 'id':
            return True
        else:
            return False
    except Exception:
        return False


def is_registered_row_model(cls: object) -> bool:
    return cls in _meta


def is_registered_table_model(cls: object) -> bool:
    return cls in _meta and _meta[cls].is_table


def is_registered_fieldtype(cls: object) -> bool:
    return cls in _meta or cls in adaptconvert_columntypes or cls in _native_columntypes


class Meta(NamedTuple):
    Model: type[Row]
    model_name: str
    table_name: str
    is_table: bool
    select: str
    select_by_id: str
    insert: str | None
    fields: tuple[MetaField, ...]


class MetaField(NamedTuple):
    name: str
    type: type
    full_type: Any  # e.g. includes Optional
    nullable: bool
    is_fk: bool
    is_pk: bool
    sql_typename: str
    sql_columndef: str


_meta: dict[type[Row], Meta] = {}


def clear_modelmeta_registrations() -> None:
    _meta.clear()


def get_meta(Model: type[Row]) -> Meta:
    """Simple cached getter for Meta"""
    try:
        return _meta[Model]
    except KeyError:
        _meta[Model] = make_meta(Model)
        return _meta[Model]


@contextmanager
def get_table_meta(Model: type[Row]) -> Iterator[Meta]:
    """Context manager for cache table meta if and only if the manager exits without exception, ie the table is created successfully."""
    meta = make_table_meta(Model)
    try:
        yield meta
    except Exception:
        raise  # propagate
    else:
        _meta[Model] = meta
        sqlite3.register_adapter(Model, lambda row: row[0])  # insert Model as its id when used as FK related field in other tables


def make_meta(Model: type[Row]) -> Meta:
    table_name = Model.__name__.split('_')[0]
    annotations = _get_resolved_annotations(Model)
    fieldnames = Model._fields
    full_types = tuple(annotations.values())
    unwrapped_types = tuple(_unwrap_optional_type(t) for t in full_types)

    fields = tuple(
        MetaField(
            name=fieldname,
            type=FieldType,
            full_type=annotations[fieldname],
            nullable=nullable,
            is_fk=is_row_model(FieldType),
            is_pk=fieldname == "id",
            sql_typename=_sql_typename(FieldType),
            sql_columndef=_sql_columndef(fieldname, nullable, FieldType),
        )
        for fieldname, (nullable, FieldType) in zip(fieldnames, unwrapped_types)
    )

    select = f"SELECT {', '.join(table_name + '.' + f for f in Model._fields)} FROM {table_name}"
    select_by_id = select + "\nWHERE id = ?"

    meta = Meta(
        Model=Model,
        model_name=Model.__name__,
        table_name=table_name,
        is_table=False,
        select=select,
        select_by_id=select_by_id,
        insert=None,
        fields=fields,
    )

    ## Validate Meta

    # Check that all fields are registered
    for field in meta.fields:
        # skipp self-reference, it will be registered once/if it is in _meta
        if field.type is Model:
            continue
        if field.full_type is Any:
            continue  # Any is not a valid type for persisting, but can be used for reading
        if not is_registered_fieldtype(field.type):
            raise UnregisteredFieldTypeError(field.type)

    return meta


def make_table_meta(Model: type[Row]) -> Meta:
    meta = make_meta(Model)

    insert = dedent(f"""
        INSERT INTO {meta.table_name} (
            {', '.join(f.name for f in meta.fields)}
        ) VALUES (
            {', '.join("?" for _ in meta.fields)}
        )""").strip()

    ## Validate Table Meta

    for field in meta.fields:
        if field.full_type is Any:  # Any is not a valid type tables fields
            raise UnregisteredFieldTypeError(field.type)

    # Check that the first field is `id: int | None`
    field_zero = meta.fields[0]
    if field_zero.name != "id" or field_zero.full_type != (int | None):
        raise FieldZeroIdRequired(meta.model_name, field_zero.name, field_zero.full_type)

    meta = meta._replace(
        is_table=True,
        insert=insert,
    )

    return meta


_native_columntypes: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}


def _sql_typename(FieldType: type) -> str:
    if FieldType in _native_columntypes:
        return _native_columntypes[FieldType]
    elif FieldType in adaptconvert_columntypes:
        # TODO: can we use registered_only here, and centralize naming
        return adaptconvert_columntypes[FieldType]
    elif is_row_model(FieldType):
        # a yet unregistered foreign key
        return f"{FieldType.__name__}_ID"
    else:
        return 'UNKNOWN'  # TODO: will be unreachable, when we use it as the source of truth for adaptconvert typenames


def _sql_columndef(field_name: str, nullable: bool, FieldType: type) -> str:
    if field_name == "id":
        if not (FieldType is int and nullable):
            raise FieldZeroIdMalformed(FieldType)
        return "id [INTEGER] PRIMARY KEY NOT NULL"

    if nullable:
        nullable_sql = "NULL"
    else:
        nullable_sql = "NOT NULL"

    columntype = _sql_typename(FieldType)

    return f"{field_name} [{columntype}] {nullable_sql}"


def _unwrap_optional_type(type_hint: Any) -> tuple[bool, Any]:
    """Determine if a given type hint is an Optional type

    Supports the following forms of Optional types:
    UnionType (e.g., int | None)
    Optional  (e.g., Optional[int])
    Union (e.g., Union[int, None])

    Returns
    - A boolean indicating if it is Optional.
    - The underlying type if it is Optional, otherwise the original type.
    """

    # Not any form of Union type
    if not (isinstance(type_hint, types.UnionType) or get_origin(type_hint) is Union):
        return False, type_hint

    args = get_args(type_hint)
    optional = type(None) in args

    underlying_types = tuple(arg for arg in args if arg is not type(None))
    underlying_type = underlying_types[0]
    for t in underlying_types[1:]:
        underlying_type |= t

    return optional, underlying_type


def _get_resolved_annotations(Model: Any) -> dict[str, Any]:
    """Resolve ForwardRef type hints by combining all local and global namespaces up the call stack."""
    globalns = getattr(inspect.getmodule(Model), "__dict__", {})
    localns = {}

    for frame in inspect.stack():
        localns.update(frame.frame.f_locals)

    return get_type_hints(Model, globalns=globalns, localns=localns)
