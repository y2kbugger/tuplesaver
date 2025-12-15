"""A metaclass that enables NamedTuple classes to have class-level access to Column definitions.

I think this was an early stage experiement that isn't relied on yet
"""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    NamedTuple,
    get_type_hints,
)

if TYPE_CHECKING:
    NamedTupleMeta = type(tuple)
else:
    from typing import NamedTupleMeta

print(NamedTupleMeta, type(NamedTupleMeta))
print(NamedTupleMeta.__mro__)

print(NamedTuple, type(NamedTuple))


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


class RowMeta(NamedTupleMeta):
    __columns__: dict[str, Column]

    def __new__(cls, name: str, bases: tuple, namespace: dict):
        ncls = super().__new__(cls, name, bases, namespace)

        type_hints = get_type_hints(ncls)
        ncls.__columns__ = {}
        for i, (field_name, field_type) in enumerate(type_hints.items()):
            column = Column(name=field_name, coltype=field_type)
            ncls.__columns__[field_name] = column
            setattr(ncls, field_name, FieldDescriptor(column, i))

        return ncls
