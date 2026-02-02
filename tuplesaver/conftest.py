from __future__ import annotations

import os
import sys
from collections.abc import Iterable, Mapping, Sequence

import apsw
import pytest
from pytest_benchmark.plugin import BenchmarkFixture

from .adaptconvert import included_adapt_convert_types
from .engine import Engine


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "scenario(name): specify migrate test scenario folder")


@pytest.fixture
def engine() -> Iterable[Engine]:
    engine = Engine(":memory:")
    engine.adapt_convert_registry.register_included_adaptconverters(included_adapt_convert_types)
    yield engine
    engine.connection.close()


class SqlLog:
    def __init__(self):
        self.entrys = []
        self.block = ""

    def exec_trace(self, cursor: apsw.Cursor, sql: str, values: Sequence | Mapping | None, /) -> bool:
        self.log(cursor.expanded_sql)
        return True  # continue normal execution

    def log(self, entry: str) -> None:
        print(entry)  # Echo to stdout, shown on test failure
        self.entrys.append(entry)
        self.block += entry + "\n"

    def clear(self) -> None:
        self.entrys.clear()
        self.block = ""

    def __contains__(self, entry: str) -> bool:
        return entry in self.block


@pytest.fixture
def sql_log(engine: Engine) -> Iterable[SqlLog]:
    sql_log = SqlLog()
    engine.connection.exec_trace = sql_log.exec_trace
    yield sql_log


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


@pytest.fixture
def limit_stack_depth() -> Iterable[None]:
    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(55)
    yield
    sys.setrecursionlimit(old_limit)
