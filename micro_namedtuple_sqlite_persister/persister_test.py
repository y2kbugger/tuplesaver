import pytest

from .persister import Engine


@pytest.fixture
def engine() -> Engine:
    return Engine(":memory:")


def test_get_connection(engine: Engine) -> None:
    from sqlite3 import Connection

    assert isinstance(engine.connection, Connection)
