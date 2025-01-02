from .persister import Engine


def test_pass() -> None:
    pass


def test_create_engine() -> None:
    engine = Engine(":memory:")
    assert isinstance(engine, Engine)
