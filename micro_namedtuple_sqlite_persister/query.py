from _collections import _tuplegetter  # type: ignore
from collections.abc import Sequence
from typing import Any, NamedTuple, cast


def get_field_idx(field: Any) -> int:
    """Get the index of a field in a NamedTuple

    this is the C-Optimized version of the tuple getter
    the python stub is a property and currently unsupported
    see python/collections/__init__.py

    try:
        from _collections import _tuplegetter
    except ImportError:
        _tuplegetter = lambda index, doc: property(_itemgetter(index), doc=doc)
    """
    # ensure we are using the C-optimized version
    c_optimized_field_type = "<class 'collections._tuplegetter'>"
    our_field_type = str(type(field))
    assert our_field_type == c_optimized_field_type, f'Expected {c_optimized_field_type}, got {our_field_type!r}'
    assert isinstance(field, _tuplegetter), f'Expected {_tuplegetter!r} got {field!r}'

    return field.__reduce__()[1][0]


def get_column_name(Model: type[NamedTuple], idx: int) -> str:
    return f"{Model.__name__}.{Model._fields[idx]}"


def get_table_name(Model: type[NamedTuple]) -> str:
    return Model.__name__


def is_namedtuple_table_model(cls: object) -> bool:
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


class CSV(tuple):
    pass  # command separated values


type Field = _tuplegetter
type Scalar = Field | str | int | float
type Fragment = Sequence[Field | Scalar | CSV | Fragment]


def select(Model: type[NamedTuple], limit: int | None = None) -> Fragment:
    """Create a SELECT statement with optional LIMIT"""
    cols = CSV(Model._fields)
    select = ('SELECT', cols, 'FROM', Model)
    if limit is not None:
        return (*select, "LIMIT", limit)
    else:
        return select


def where(condition: Fragment) -> Fragment:
    """Create a WHERE clause"""
    return ('WHERE', condition)


def eq(left: Scalar, right: Scalar) -> Fragment:
    return ('(', left, '=', right, ')')


def or_(left: Fragment, right: Fragment) -> Fragment:
    return ('(', left, 'OR', right, ')')


def render_query(fg: Field | Scalar | CSV | Fragment, Model: type[NamedTuple] | None = None) -> str:
    if isinstance(fg, str):
        return fg
    elif isinstance(fg, CSV):
        return '\n    ' + ', '.join(fg) + '\n'
    elif isinstance(fg, _tuplegetter):
        assert Model is not None
        return get_column_name(Model, get_field_idx(fg))
    elif is_namedtuple_table_model(fg):
        Model = cast(type[NamedTuple], fg)
        return '\n    ' + Model.__name__
    elif isinstance(fg, tuple):
        return ''.join(render_query(f, Model) for f in fg)
    else:
        raise TypeError(f"Unexpected type: {type(fg)}")
