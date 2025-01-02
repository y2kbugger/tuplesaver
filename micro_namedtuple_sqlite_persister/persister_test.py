from .persister import Engine


def test_create_engine() -> None:
    engine = Engine(":memory:")
    assert isinstance(engine, Engine)


def test_get_connection() -> None:
    from sqlite3 import Connection

    engine = Engine(":memory:")
    connection = engine.connection
    assert isinstance(connection, Connection)
