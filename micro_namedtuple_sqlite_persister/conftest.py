import sqlite3
from collections.abc import Iterable

import pytest

from .adaptconvert import clear_adapt_convert_registrations, enable_included_adaptconverters
from .persister import Engine


@pytest.fixture
def engine() -> Engine:
    return Engine(":memory:", echo_sql=True)


@pytest.fixture(autouse=True)
def init_and_reset_adapterconverters() -> Iterable[None]:
    enable_included_adaptconverters()
    yield
    # clean up registrations and well as just our registrations to sqlite3 itself
    clear_adapt_convert_registrations()

    # clean up sqlite itself harder just in case we make a mistake
    sqlite3.adapters.clear()
    sqlite3.converters.clear()
