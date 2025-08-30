from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    NamedTuple,
    assert_type,
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


class MyRow(NamedTuple, metaclass=RowMeta):
    id: int
    name: str
    active: bool


class MyRelatedRow(NamedTuple, metaclass=RowMeta):
    id: int
    mr: MyRow


print("=== Testing class-level Column access ===")
print(f"MY_ROW.id: {MyRow.id}")
print(f"MY_ROW.name: {MyRow.name}")
print(f"MY_ROW.active: {MyRow.active}")
assert MyRow.id == Column("id", int)
assert MyRow.name == Column("name", str)
assert MyRow.active == Column("active", bool)

print("\n=== Testing instance-level field access ===")
mr = MyRow(id=1, name="Toyota", active=True)
print(f"mr.id: {mr.id}")
print(f"mr.name: {mr.name}")
print(f"mr.active: {mr.active}")
assert mr.id == 1
assert mr.name == "Toyota"
assert mr.active is True

print("\n=== Testing related row access ===")
related = MyRelatedRow(id=1, mr=mr)
print(f"related.id: {related.id}")
print(f"related.mr: {related.mr}")
print(f"related.mr.name: {related.mr.name}")
assert related.id == 1
assert related.mr == mr
assert related.mr.name == "Toyota"


print("Testing matrix of related and metaclassed NT Fields")


class MyRowB(NamedTuple):
    id: int
    name: str
    active: bool


class MyRowA(NamedTuple, metaclass=RowMeta):
    id: int
    name: str
    active: bool


class MyRelatedRow1(NamedTuple):
    id: int
    a: MyRowA
    b: MyRowB


class MyRelatedRow2(NamedTuple, metaclass=RowMeta):
    id: int
    a: MyRowA
    b: MyRowB


aa = MyRowA(1, "Alice", True)
bb = MyRowB(2, "Bob", False)
rr = MyRelatedRow1(1, a=aa, b=bb)


assert_type(rr, MyRelatedRow1)
assert_type(rr.a, MyRowA)
assert_type(rr.b, MyRowB)

# These are actually controversial, really those are not the types of .a and .b
# actually .a and .b are descriptors that return those types when in an instance
assert_type(MyRelatedRow1.a, MyRowA)
assert_type(MyRelatedRow1.b, MyRowB)

assert_type(MyRelatedRow2.a, MyRowA)
assert_type(MyRelatedRow2.b, MyRowB)

print("All tests passed! 🎉")
