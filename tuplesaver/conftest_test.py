from __future__ import annotations

import sqlite3
from typing import NamedTuple

import pytest

from .adaptconvert import (
    adaptconvert_columntypes,
    register_adapt_convert,
)
from .engine import Engine
from .model import UnregisteredFieldTypeError


class TestInitAndResetFixures:
    class NewType: ...

    class ModelQ(NamedTuple):
        id: int | None
        name: str
        custom: TestInitAndResetFixures.NewType

    @pytest.mark.parametrize("_", ["ping", "pong"])
    def test_adaptconvert_by_inspecting_sqlite(self, _: str) -> None:
        assert sqlite3.adapters.get((self.NewType, sqlite3.PrepareProtocol)) is None
        register_adapt_convert(self.NewType, lambda x: b'', lambda x: self.NewType())
        assert sqlite3.adapters.get((self.NewType, sqlite3.PrepareProtocol)) is not None

    @pytest.mark.parametrize("_", ["ping", "pong"])
    def test_adaptconvert_by_inspecting_adapt_convert_registry(self, _: str) -> None:
        assert self.NewType not in adaptconvert_columntypes
        register_adapt_convert(self.NewType, lambda x: b'', lambda x: self.NewType())
        assert self.NewType in adaptconvert_columntypes

    @pytest.mark.parametrize("_", ["ping", "pong"])
    def test_adaptconvert_by_inference_while_trying_to_create_tables(self, engine: Engine, _: str) -> None:
        with pytest.raises(UnregisteredFieldTypeError):
            engine.ensure_table_created(self.ModelQ)

        register_adapt_convert(self.NewType, lambda x: b'', lambda x: self.NewType())

        engine.ensure_table_created(self.ModelQ)

    class ModelC(NamedTuple):
        id: int | None
        value: float

    class ModelD(NamedTuple):
        id: int | None
        name: str
        modelc: TestInitAndResetFixures.ModelC

    @pytest.mark.parametrize("_", ["ping", "pong"])
    def test_modelregistration_by_inference_while_trying_to_create_tables(self, engine: Engine, _: str) -> None:
        with pytest.raises(UnregisteredFieldTypeError):
            engine.ensure_table_created(self.ModelD)

        engine.ensure_table_created(self.ModelC)

        engine.ensure_table_created(self.ModelD)
