from __future__ import annotations

from collections.abc import Callable
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    NamedTuple,
    Protocol,
    cast,
)

if TYPE_CHECKING:
    _make_nmtuple: Callable[..., type[NamedTuple]]
    _prohibited: frozenset[str]
    _special: frozenset[str]
else:
    # lean on std for now, just copy how named tuples are created
    from typing import _make_nmtuple, _prohibited, _special

__all__ = ["Column", "RowLike", "Roww"]


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
    """Like NamedTupleMeta but for our Row

    We basically reimplement the logic of NamedTupleMeta here to create our own
    metaclass that creates NamedTuple subclasses with our desired behavior. e.g.  Drop support for generics, and maybe some other edges

    The whole point is to allow us to do the same thing as in NTM.py, except for the user, they just subclass Roww instead of NamedTuple with a special metaclass.
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

        # type_hints = get_type_hints(cls)
        # nm_tpl = cast(type[RowLike], nm_tpl)
        # nm_tpl.__columns__ = {}  # this columns thing is just a placeholder for actual _meta, when we combine later
        # for i, (field_name, field_type) in enumerate(type_hints.items()):
        #     column = Column(name=field_name, coltype=field_type)
        #     nm_tpl.__columns__[field_name] = column
        #     # setattr(nm_tpl, field_name, FieldDescriptor(column, i)) # lets assume we dont want custom descriptor behavior for now

        class LazyMeta:
            def __get__(self, obj: Any, cls: type):
                from tuplesaver.model import make_model_meta

                meta = make_model_meta(cls)
                cls._meta = meta
                return meta

        nm_tpl._meta = LazyMeta()  # type: ignore[attr-defined]  # noqa: SLF001

        return nm_tpl


# we call it Roww to avoid clashing with the Row in model.py at least for now.


### the old body of this (pulled from NamedTuple implementation) was for deprecated "function based passing"
def RowwMaker(typename: str, **kwargs) -> type: ...


Roww: type[NamedTuple] = cast(type[NamedTuple], RowwMaker)
_Roww = type.__new__(RowMeta, "Roww", (), {})


def _row_mro_entries(bases: tuple):
    assert Roww in bases
    return (_Roww,)


Roww.__mro_entries__ = _row_mro_entries  # type: ignore
