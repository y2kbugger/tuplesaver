from __future__ import annotations

import sqlite3

import pytest

from .adaptconvert import (
    adaptconvert_columntypes,
    register_adapt_convert,
)
from .engine import Engine
from .model import UnregisteredFieldTypeError
from .RM import Roww


class TestInitAndResetFixures:
    class NewType: ...

    class ModelQ(Roww):
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
    def test_adaptconvert_by_inference_while_trying_to_create_tables_global_model(self, engine: Engine, _: str) -> None:
        if _ == "pong":
            pytest.xfail(reason="Currently, meta is cached between tests, and so the pong never fails. Need to reset meta between tests")

        with pytest.raises(UnregisteredFieldTypeError):
            engine.ensure_table_created(self.ModelQ)

        register_adapt_convert(self.NewType, lambda x: b'', lambda x: self.NewType())

        engine.ensure_table_created(self.ModelQ)
        # 1 / 0

    @pytest.mark.parametrize("_", ["ping", "pong"])
    def test_adaptconvert_by_inference_while_trying_to_create_tables_local_model(self, engine: Engine, _: str) -> None:
        class ModelW(Roww):
            id: int | None
            name: str
            custom: TestInitAndResetFixures.NewType

        with pytest.raises(UnregisteredFieldTypeError):
            engine.ensure_table_created(ModelW)

        register_adapt_convert(self.NewType, lambda x: b'', lambda x: self.NewType())

        engine.ensure_table_created(ModelW)
        # 1 / 0
