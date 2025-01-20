from __future__ import annotations

import inspect
import logging
import types
from typing import Any, NamedTuple, Union, get_args, get_origin, get_type_hints

from .adaptconvert import adaptconvert_columntypes

logger = logging.getLogger(__name__)


type Row = NamedTuple


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


class Meta(NamedTuple):
    annotations: dict[str, Any]
    unwrapped_field_types: tuple[type, ...]
    select: str


_meta: dict[type[Row], Meta] = {}


def clear_modelmeta_registrations() -> None:
    _meta.clear()
    _model_columntypes.clear()


def get_meta(Model: type[Row]) -> Meta:
    try:
        return _meta[Model]
    except KeyError:
        annotations = get_resolved_annotations(Model)
        unwapped_annotations = {k: unwrap_optional_type(v)[1] for k, v in annotations.items()}
        unwrapped_field_types = tuple(unwapped_annotations.values())
        select = f"SELECT {', '.join(Model._fields)} FROM {Model.__name__} WHERE id = ?"
        _meta[Model] = Meta(
            annotations,
            unwrapped_field_types,
            select,
        )
        return _meta[Model]


_native_columntypes: dict[type, str] = {
    str: "TEXT",
    float: "REAL",
    int: "INTEGER",
    bytes: "BLOB",
}
_model_columntypes: dict[type, str] = {}


def get_sqltypename(FieldType: type, *, registered_only: bool = False) -> str | None:
    if FieldType in _native_columntypes:
        return _native_columntypes[FieldType]
    elif FieldType in _model_columntypes:
        return _model_columntypes[FieldType]
    elif FieldType in adaptconvert_columntypes:
        # TODO: can we use registered_only here, and centralize naming
        return adaptconvert_columntypes[FieldType]
    elif is_row_model(FieldType) and not registered_only:
        # a yet unregistered foreign key
        return f"{FieldType.__name__}_ID"


def column_definition(annotation: tuple[str, Any]) -> str:
    field_name, FieldType = annotation

    if field_name == "id":
        if FieldType != int | None:
            raise TypeError("id field must be of type int | None")

        return "id [INTEGER] PRIMARY KEY NOT NULL"

    nullable, FieldType = unwrap_optional_type(FieldType)

    if nullable:
        nullable_sql = "NULL"
    else:
        nullable_sql = "NOT NULL"

    columntype = get_sqltypename(FieldType, registered_only=False)

    return f"{field_name} [{columntype}] {nullable_sql}"


def unwrap_optional_type(type_hint: Any) -> tuple[bool, Any]:
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


def get_resolved_annotations(Model: Any) -> dict[str, Any]:
    """Resolve ForwardRef type hints by combining all local and global namespaces up the call stack."""
    globalns = getattr(inspect.getmodule(Model), "__dict__", {})
    localns = {}

    for frame in inspect.stack():
        localns.update(frame.frame.f_locals)

    return get_type_hints(Model, globalns=globalns, localns=localns)
