from __future__ import annotations

import inspect
import logging
import types
from dataclasses import dataclass, field
from typing import Any, ClassVar, NamedTuple, Union, dataclass_transform, get_args, get_origin, get_type_hints

logger = logging.getLogger(__name__)


class LazyMeta:
    """Descriptor that lazily creates and caches the Meta object on first access."""

    def __get__(self, obj: Any, cls: type[TableRow]):
        meta = make_model_meta(cls)
        cls.meta = meta
        return meta


@dataclass_transform()
class RowMeta(type):
    """Metaclass that transforms classes into frozen dataclasses."""

    meta: ClassVar[Meta]

    def __new__(cls, typename: str, bases: tuple[type, ...], ns: dict[str, Any]) -> type:
        new_cls = super().__new__(cls, typename, bases, ns)

        # apply the dataclass decorator if not already applied
        if "__dataclass_fields__" not in new_cls.__dict__:
            new_cls = dataclass(new_cls)

        # Add lazy _meta descriptor, subclasses each get their own Meta instance, thats why we add it here.
        new_cls.meta = LazyMeta()  # type: ignore[attr-defined]

        return new_cls


class Row(metaclass=RowMeta):
    pass


class TableRow(metaclass=RowMeta):
    id: int | None = field(default=None, kw_only=True)


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


native_columntypes: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}


def is_row_model(cls: object) -> bool:
    """Test at runtime whether an object is a Row, e.g. a TableRow model."""
    return isinstance(cls, type) and issubclass(cls, TableRow)


class Meta(NamedTuple):
    Model: type[TableRow]
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


def make_model_meta(Model: type[TableRow]) -> Meta:
    annotations = _get_resolved_annotations(Model)
    fieldnames = Model.__dataclass_fields__.keys()
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

    table_name = getattr(Model, '__tablename__', None) or Model.__name__

    meta = Meta(
        Model=Model,
        model_name=Model.__name__,
        table_name=table_name,
        fields=fields,
    )

    ## Validate Meta

    if "_" in meta.model_name:
        raise InvalidTableName(meta.model_name)

    # monkey-patch Model so any Lazy field is transparently unwrapped
    from .cursorproxy import Lazy

    def _unwrap_lazyproxy_getattr(self: Row, name: str, /) -> Any:
        value = object.__getattribute__(self, name)
        if isinstance(value, Lazy):
            return value._obj()  # materialise & return real row  # noqa: SLF001
        return value

    Model.__getattribute__ = _unwrap_lazyproxy_getattr  # ty:ignore[invalid-assignment]

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

    # Add FK constraint for related models
    if is_row_model(FieldType):
        fk_table = getattr(FieldType, '__tablename__', None) or FieldType.__name__
        fk_clause = f" REFERENCES {fk_table}(id)"
    else:
        fk_clause = ""

    return f"{field_name} [{columntype}] {nullable_sql}{fk_clause}"


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
    """Resolve ForwardRef type hints by combining all local and global namespaces up the call stack.

    Includes inherited annotations from base classes.
    """
    globalns = getattr(inspect.getmodule(Model), "__dict__", {})
    localns = {}

    for frame in inspect.stack():
        localns.update(frame.frame.f_locals)

    # get_type_hints includes inherited annotations
    hints = get_type_hints(Model, globalns=globalns, localns=localns, include_extras=True)

    # Ensure ordering matches dataclass field order
    if hasattr(Model, "__dataclass_fields__"):
        ordered = {}
        for name in Model.__dataclass_fields__:
            if name in hints:
                ordered[name] = hints[name]
        return ordered

    return hints
