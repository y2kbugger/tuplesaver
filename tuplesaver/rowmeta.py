from __future__ import annotations

from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    NamedTuple,
    Protocol,
    cast,
    get_type_hints,
)

if TYPE_CHECKING:
    _make_nmtuple: Callable[..., type[NamedTuple]]
    _prohibited: frozenset[str]
    _special: frozenset[str]
else:
    # lean on std for now, just copy how named tuples are created
    from typing import _make_nmtuple, _prohibited, _special

__all__ = ["Column", "Roww"]


class Column(NamedTuple):
    name: str
    coltype: type


class FieldDescriptor:
    def __init__(self, column: Column, index: int):
        self.column = column
        self.index = index

    def __get__(self, instance: NamedTuple | None, owner: type):
        if instance is None:
            return self.column
        else:
            return instance[self.index]


class RowLike(Protocol):
    _field_defaults: ClassVar[dict[str, Any]]
    _fields: ClassVar[tuple[str, ...]]
    # __orig_bases__ sometimes exists on <3.12, but not consistently
    # So we only add it to the stub on 3.12+.
    __orig_bases__: ClassVar[tuple[Any, ...]]
    __columns__: dict[str, Column]


class RowMeta(type):
    """Like NamedTupleMeta but for Row

    Drop support for generics, and maybe some other edges
    """

    def __new__(cls, typename: str, bases: tuple[type], ns: dict[str, Any]) -> type:
        assert bases == (_Roww,), f"You can only subclass Roww, got {bases!r}"
        bases = (tuple,)
        types = ns.get("__annotations__", {})
        default_names = []
        for field_name in types:
            # TODO: custom logic for allowing id as first default field
            if field_name in ns:
                default_names.append(field_name)
            elif default_names:
                raise TypeError(f"Non-default namedtuple field {field_name} cannot follow default field{'s' if len(default_names) > 1 else ''} {', '.join(default_names)}")

        nm_tpl = _make_nmtuple(
            typename,
            types.items(),
            defaults=[ns[n] for n in default_names],
            module=ns["__module__"],
        )

        nm_tpl.__bases__ = bases

        # update from user namespace without overriding special namedtuple attributes
        for key, val in ns.items():
            if key in _prohibited:
                raise AttributeError("Cannot overwrite Row attribute " + key)
            elif key not in _special:
                if key not in nm_tpl._fields:
                    setattr(nm_tpl, key, val)
                try:
                    set_name = type(val).__set_name__
                except AttributeError:
                    pass
                else:
                    try:
                        set_name(val, nm_tpl, key)
                    except BaseException as e:
                        e.add_note(f"Error calling __set_name__ on {type(val).__name__!r} instance {key!r} in {typename!r}")
                        raise

        type_hints = get_type_hints(nm_tpl)
        nm_tpl = cast(type[RowLike], nm_tpl)
        nm_tpl.__columns__ = {}
        for i, (field_name, field_type) in enumerate(type_hints.items()):
            column = Column(name=field_name, coltype=field_type)
            nm_tpl.__columns__[field_name] = column
            setattr(nm_tpl, field_name, FieldDescriptor(column, i))

        return nm_tpl


### the old body of this was for deprecated "function based passing"
def Roww(typename: str, **kwargs) -> type: ...  # pyright: ignore[reportRedeclaration]


Roww: type[NamedTuple] = cast(type[NamedTuple], Roww)
_Roww = type.__new__(RowMeta, "Roww", (), {})


def _row_mro_entries(bases: tuple):
    assert Roww in bases
    return (_Roww,)


Roww.__mro_entries__ = _row_mro_entries  # type: ignore
