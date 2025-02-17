from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable

import pytest
from pytest_benchmark.plugin import BenchmarkFixture

from .adaptconvert import clear_adapt_convert_registrations, enable_included_adaptconverters
from .model import clear_modelmeta_registrations
from .persister import Engine


@pytest.fixture
def engine() -> Iterable[Engine]:
    engine = Engine(":memory:", echo_sql=True)
    yield engine
    engine.connection.close()


@pytest.fixture(autouse=True)
def init_and_cleanup_registrations() -> Iterable[None]:
    enable_included_adaptconverters()

    yield

    # clean up our adaptconvert registrations and well as just our registrations to sqlite3 itself
    clear_adapt_convert_registrations()
    # clean up sqlite itself harder just in case we make a mistake
    sqlite3.adapters.clear()
    sqlite3.converters.clear()
    # clean up our model registrations
    clear_modelmeta_registrations()


@pytest.fixture
def benchmark(benchmark: BenchmarkFixture) -> Iterable[BenchmarkFixture]:
    """Overwrite the benchmark fixture to set affinity to a specific core"""

    old = os.sched_getaffinity(0)
    # set affinity to 6,7 e.g physical hyperthreaded physical core 3, this is system specific
    # TODO: make this configurable
    os.sched_setaffinity(0, {6, 7})
    os.sched_yield()

    yield benchmark

    os.sched_setaffinity(0, old)
