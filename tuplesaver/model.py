from __future__ import annotations

import inspect
import logging
import types
from typing import Any, NamedTuple, Union, get_args, get_origin, get_type_hints

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


class InvalidTableName(ModelDefinitionError):
    def __init__(self, table_name: str) -> None:
        super().__init__(f"Invalid table name: `{table_name}`. Table names must not contain underscores, these are reserved for alternate models.")


class NotATableModel(ModelDefinitionError):
    def __init__(self, model_name: str) -> None:
        super().__init__(f"Model `{model_name}` is not a valid table model.")


native_columntypes: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}


def is_row_model(cls: object) -> bool:
    """Test at runtime whether an object is a Row, e.g. a NamedTuple model"""
    if hasattr(cls, "__dict__"):
        return "_meta" in cls.__dict__
    else:
        return False


class Meta(NamedTuple):
    Model: type[Row]
    model_name: str
    table_name: str
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


def get_meta(Model: type[Row]) -> Meta:
    if not is_row_model(Model):
        raise NotATableModel(Model.__name__)
    return Model._meta  # type: ignore[attr-defined]  # noqa: SLF001


def make_model_meta(Model: type[Row]) -> Meta:
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
            sql_typename=schematype(FieldType),
            sql_columndef=_sql_columndef(fieldname, nullable, FieldType),
        )
        for fieldname, (nullable, FieldType) in zip(fieldnames, unwrapped_types, strict=False)
    )

    meta = Meta(
        Model=Model,
        model_name=Model.__name__,
        table_name=Model.__name__,  # for now, table name is same as model name
        fields=fields,
    )

    ## Validate Meta

    if "_" in meta.model_name:
        raise InvalidTableName(meta.model_name)

    # Check that the first field is `id: int | None`
    field_zero = meta.fields[0]
    if field_zero.name != "id" or field_zero.full_type != (int | None):
        raise FieldZeroIdRequired(meta.model_name, field_zero.name, field_zero.full_type)

    # monkey-patch Model so any Lazy field is transparently unwrapped
    from .cursorproxy import Lazy

    def _unwrap_lazyproxy_getattr(self: NamedTuple, attr: str):
        value = object.__getattribute__(self, attr)
        if isinstance(value, Lazy):
            return value._obj()  # materialise & return real row  # noqa: SLF001
        return value

    Model.__getattribute__ = _unwrap_lazyproxy_getattr

    return meta


def schematype(FieldType: type) -> str:
    if FieldType in native_columntypes:
        return native_columntypes[FieldType]
    elif is_row_model(FieldType):
        # a yet unregistered foreign key
        return f"{FieldType.__name__}_ID"
    else:
        return f"{FieldType.__module__}.{FieldType.__qualname__}"


def _sql_columndef(field_name: str, nullable: bool, FieldType: type) -> str:
    if field_name == "id":
        if not (FieldType is int and nullable):
            raise FieldZeroIdMalformed(FieldType)
        return "id [INTEGER] PRIMARY KEY NOT NULL"

    if nullable:
        nullable_sql = "NULL"
    else:
        nullable_sql = "NOT NULL"

    columntype = schematype(FieldType)

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
