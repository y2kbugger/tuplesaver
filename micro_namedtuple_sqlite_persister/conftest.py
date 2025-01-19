from collections.abc import Iterable

import pytest

from micro_namedtuple_sqlite_persister.adaptconvert import enable_included_adaptconverters

from .adaptconvert import clear_adapt_convert_registrations
from .persister import Engine


@pytest.fixture
def engine() -> Engine:
    return Engine(":memory:", echo_sql=True)


@pytest.fixture(autouse=True)
def init_and_reset_adapterconverters() -> Iterable[None]:
    enable_included_adaptconverters()
    yield
    clear_adapt_convert_registrations()
