import datetime as dt
from typing import NamedTuple

import pytest

from .persister import Engine


def test_can_store_and_retrieve_list_as_json(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        names: list

    engine.ensure_table_created(T)
    names = ["Alice", "Bob", "Charlie", 2]
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_can_store_and_retrieve_dict_as_json(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        names: dict

    engine.ensure_table_created(T)
    names = {"Alice": 1, "Bob": 2, "Charlie": 3}
    row = engine.save(T(None, names))

    returned_row = engine.find(T, row.id)

    assert returned_row.names == names


def test_raises_on_json_when_nonserializeable(engine: Engine) -> None:
    class T(NamedTuple):
        id: int | None
        dates: list

    engine.ensure_table_created(T)

    with pytest.raises(TypeError, match="Object of type datetime is not JSON serializable"):
        engine.save(T(None, [dt.datetime.now()]))
