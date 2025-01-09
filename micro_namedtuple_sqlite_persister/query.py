from _collections import _tuplegetter  # type: ignore
from collections.abc import Sequence
from typing import Any, NamedTuple

from .persister import Row


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


# fmt: off
class SELECT(tuple): pass  # SELECT statement  # noqa: E701
class CSV(tuple): pass  # command separated values  # noqa: E701
class WHERE(tuple): pass  # WHERE clause  # noqa: E701
class LIMIT(tuple): pass  # LIMIT clause  # noqa: E701
class CMP(tuple): pass  # comparison  # noqa: E701
class LOGIC(tuple): pass  # logical operator # noqa: E701
# fmt: on

type Field = _tuplegetter
type Scalar = Field | str | int | float
type Frag = Field | Scalar | CSV
type Fragment = Sequence[Frag | Fragment] | Frag


def _select(Model: type[Row], *, where: Fragment | None = None, limit: int | None = None) -> Fragment:
    """Create a SELECT statement"""
    cols = CSV(Model._fields)
    select = ('SELECT', cols, 'FROM', Model)
    if where is not None:
        select = (*select, WHERE(('WHERE', where)))
    if limit is not None:
        select = (*select, LIMIT(("LIMIT", limit)))
    return SELECT(select)


def render(M: type[Row], fg: Fragment) -> str:
    match fg:
        case SELECT((select, cols, frm, model, *rest)):  # Deconstruct SELECT statement
            return f"{select} {render(M, cols)}\n{frm} {render(M, model)}" + (f"\n{'\n'.join(render(M, f) for f in rest)}" if rest else "")
        case CSV(fields):  # Deconstruct CSV into fields
            return ', '.join(fields)
        case CMP((left, op, right)):  # Deconstruct CMP into left, op, and right
            return f"({render(M, left)} {op} {render(M, right)})"
        case LOGIC((left, operator, right)):  # Deconstruct LOGIC into left and right
            return f"({render(M, left)} {operator} {render(M, right)})"
        case WHERE((keyword, condition)):  # Deconstruct WHERE into keyword and condition
            return f"{keyword} {render(M, condition)}"
        case LIMIT((keyword, value)):  # Deconstruct LIMIT into keyword and value
            return f"{keyword} {value}"
        case _tuplegetter():
            assert M is not None
            return get_column_name(M, get_field_idx(fg))
        case _ if is_namedtuple_table_model(fg):
            return M.__name__
        case tuple():  # Match any tuple and deconstruct into elements
            return ' '.join(render(M, f) for f in fg)
        case int() | float():
            return str(fg)
        case str():
            return fg
        case _:
            raise TypeError(f"Unexpected type: {type(fg)}")


def select[R: Row](Model: type[R], *, where: Fragment | None = None, limit: int | None = None) -> tuple[type[R], str]:
    """Produce a rendered SELECT query"""
    select_fragment = _select(Model, where=where, limit=limit)
    sql = render(Model, select_fragment)
    return Model, sql


def eq(left: Scalar, right: Scalar) -> Fragment:
    return CMP((left, '=', right))


def gt(left: Scalar, right: Scalar) -> Fragment:
    """Greater than comparison"""
    return CMP((left, '>', right))


def lt(left: Scalar, right: Scalar) -> Fragment:
    """Less than comparison"""
    return CMP((left, '<', right))


def gte(left: Scalar, right: Scalar) -> Fragment:
    """Greater than or equal comparison"""
    return CMP((left, '>=', right))


def lte(left: Scalar, right: Scalar) -> Fragment:
    """Less than or equal comparison"""
    return CMP((left, '<=', right))


def ne(left: Scalar, right: Scalar) -> Fragment:
    """Not equal comparison"""
    return CMP((left, '!=', right))


def or_(left: Fragment, right: Fragment) -> Fragment:
    return LOGIC((left, 'OR', right))


def and_(left: Fragment, right: Fragment) -> Fragment:
    return LOGIC((left, 'AND', right))
