"""SQLite schema migration management for TupleSaver models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .engine import Engine
from .model import TableRow
from .sql import generate_create_table_ddl


class State(Enum):
    ERROR = "error"  # Blocking issues (duplicate numbers, gaps)
    DIVERGED = "diverged"  # Script on disk differs from what was applied
    PENDING = "pending"  # Scripts ready to apply
    DRIFT = "drift"  # DB doesn't match models, need to generate
    CURRENT = "current"  # Fully in sync


@dataclass
class TableSchema:
    """Schema comparison for a single model/table."""

    table_name: str
    expected_sql: str  # DDL from model
    actual_sql: str | None  # DDL from DB, None if table doesn't exist

    @property
    def exists(self) -> bool:
        return self.actual_sql is not None

    @property
    def is_current(self) -> bool:
        return self.expected_sql == self.actual_sql


@dataclass
class CheckResult:
    pending: list[str] = field(default_factory=list)  # scripts on disk not yet applied
    applied: list[str] = field(default_factory=list)  # scripts recorded in _migrations table
    divergent: list[str] = field(default_factory=list)  # disk content != recorded content
    errors: list[str] = field(default_factory=list)  # blockers
    schema: dict[str, TableSchema] = field(default_factory=dict)  # table_name -> schema comparison

    @property
    def has_schema_drift(self) -> bool:
        return any(not s.is_current for s in self.schema.values())

    @property
    def state(self) -> State:
        """Primary state for decision-making (first match wins)."""
        if self.errors:
            return State.ERROR
        if self.divergent:
            return State.DIVERGED
        if self.pending:
            return State.PENDING
        if self.has_schema_drift:
            return State.DRIFT
        return State.CURRENT

    def status(self) -> str:
        """Human-readable summary, like `git status`."""
        lines = []
        if self.errors:
            lines.append(f"Errors: {', '.join(self.errors)}")
        if self.divergent:
            lines.append(f"Diverged: {', '.join(self.divergent)}")
        if self.pending:
            lines.append(f"Pending: {', '.join(self.pending)}")
        if self.has_schema_drift:
            missing_tables = [name for name, s in self.schema.items() if not s.exists]
            changed_tables = [name for name, s in self.schema.items() if s.exists and not s.is_current]
            if missing_tables:
                lines.append(f"Tables to create: {', '.join(missing_tables)}")
            if changed_tables:
                lines.append(f"Tables with schema changes: {', '.join(changed_tables)}")
        if not lines:
            lines.append("Current: schema is up to date")
        return "\n".join(lines)


class Migrate:
    def __init__(self, engine: Engine, models: list[type[TableRow]]) -> None:
        self.engine = engine
        self.models = models
        assert engine.db_path is not None, "Engine must have a db_path"
        self.db_path = Path(engine.db_path)

    def _get_table_sql(self, table_name: str) -> str | None:
        """Get CREATE TABLE sql from sqlite_master, or None if not exists."""
        cursor = self.engine.connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def _compute_table_schema(self, model: type[TableRow]) -> TableSchema:
        """Compute schema comparison for a single model."""
        table_name = model.meta.table_name
        expected_sql = generate_create_table_ddl(model)
        actual_sql = self._get_table_sql(table_name)
        return TableSchema(
            table_name=table_name,
            expected_sql=expected_sql,
            actual_sql=actual_sql,
        )

    def check(self) -> CheckResult:
        """Read-only checks. No side effects."""
        schema = {m.meta.table_name: self._compute_table_schema(m) for m in self.models}
        return CheckResult(schema=schema)
