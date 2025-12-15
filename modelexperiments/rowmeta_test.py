"""This test suite is used to test and compare the behavior of two approaches to
defining a custom metaclass for our model type. If we don't want to require the user
to use the metaclass keyword, then we need to provide our own base class that uses
a custom metaclass. This is what RM.py does with Roww. but since it is also trying to
be a NamedTuple, it has to reimplement a lot of the logic of NamedTupleMeta.

That method is cool tho, because we can also add other customizations in the same way.
The downside is that the code is complex.
"""

from typing import NamedTuple, assert_type

import pytest
from NTM import Column, RowMeta

from tuplesaver.model import Row as MyRowwwT

# ruff: noqa: ERA001
# from tuplesaver.RM import Column, Roww as MyRowwwT


# class MyRow(Roww):
class MyRow(NamedTuple, metaclass=RowMeta):
    id: int
    name: str
    active: bool


# class MyRelatedRow(Roww):
class MyRelatedRow(NamedTuple, metaclass=RowMeta):
    id: int
    mr: MyRow


def test_class_level_column_access() -> None:
    assert MyRow.id == Column("id", int)
    assert MyRow.name == Column("name", str)
    assert MyRow.active == Column("active", bool)


def test_instance_level_field_access() -> None:
    mr = MyRow(id=1, name="Toyota", active=True)

    assert isinstance(mr, MyRow)
    assert isinstance(mr, tuple)

    assert mr.id == 1
    assert mr.name == "Toyota"
    assert mr.active is True


def test_instance_level_field_types() -> None:
    mr = MyRow(id=1, name="Toyota", active=True)

    assert_type(mr, MyRow)
    assert_type(mr.id, int)
    assert_type(mr.name, str)
    assert_type(mr.active, bool)


def test_model_type_can_be_used_as_typehint() -> None:
    def func(r: MyRowwwT) -> int: ...


def test_related_row_access() -> None:
    mr = MyRow(id=1, name="Toyota", active=True)
    related = MyRelatedRow(id=1, mr=mr)

    assert related.id == 1
    assert related.mr == mr
    assert related.mr.name == "Toyota"


def test_namedtuple_behavior() -> None:
    mr = MyRow(id=1, name="Toyota", active=True)

    assert mr[0] == 1
    assert mr[1] == "Toyota"
    assert mr[2] is True

    assert hasattr(mr, '_fields')
    assert mr._fields == ('id', 'name', 'active')


def test_column_metadata_is_correctly_set() -> None:
    columns = getattr(MyRow, '__columns__', {})

    assert 'id' in columns
    assert 'name' in columns
    assert 'active' in columns

    assert columns['id'] == Column('id', int)
    assert columns['name'] == Column('name', str)
    assert columns['active'] == Column('active', bool)


def test_default_field_values_work() -> None:
    # class RowWithDefaults(Roww):
    class RowWithDefaults(NamedTuple, metaclass=RowMeta):
        id: int
        name: str
        active: bool = True

    row1 = RowWithDefaults(id=1, name="Test", active=False)
    assert row1.active is False

    row2 = RowWithDefaults(id=2, name="Test2")
    assert row2.active is True


def test_non_default_fields_cannot_follow_default_fields() -> None:
    with pytest.raises(TypeError, match="Non-default namedtuple field"):
        # class BadRow(Roww):
        class BadRow(NamedTuple, metaclass=RowMeta):
            id: int
            active: bool = True
            name: str  # type: ignore
