"""SQLite schema migration management for TupleSaver models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .engine import Engine
from .model import TableRow


class State(Enum):
    ERROR = "error"  # Blocking issues (duplicate numbers, gaps)
    DIVERGED = "diverged"  # Script on disk differs from what was applied
    PENDING = "pending"  # Scripts ready to apply
    DRIFT = "drift"  # DB doesn't match models, need to generate
    CURRENT = "current"  # Fully in sync


@dataclass
class ModelDiff:
    """Schema differences between models and actual DB."""

    tables_to_create: list[str] = field(default_factory=list)
    tables_to_drop: list[str] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not self.tables_to_create and not self.tables_to_drop


@dataclass
class CheckResult:
    pending: list[str] = field(default_factory=list)  # scripts on disk not yet applied
    applied: list[str] = field(default_factory=list)  # scripts recorded in _migrations table
    divergent: list[str] = field(default_factory=list)  # disk content != recorded content
    errors: list[str] = field(default_factory=list)  # blockers
    model_diff: ModelDiff = field(default_factory=ModelDiff)

    @property
    def state(self) -> State:
        """Primary state for decision-making (first match wins)."""
        if self.errors:
            return State.ERROR
        if self.divergent:
            return State.DIVERGED
        if self.pending:
            return State.PENDING
        if not self.model_diff.is_empty:
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
        if not self.model_diff.is_empty:
            if self.model_diff.tables_to_create:
                lines.append(f"Tables to create: {', '.join(self.model_diff.tables_to_create)}")
            if self.model_diff.tables_to_drop:
                lines.append(f"Tables to drop: {', '.join(self.model_diff.tables_to_drop)}")
        if not lines:
            lines.append("Current: schema is up to date")
        return "\n".join(lines)


class Migrate:
    def __init__(self, engine: Engine, models: list[type[TableRow]]) -> None:
        self.engine = engine
        self.models = models
        self.db_path = Path(engine.db_path) if engine.db_path else None

    def check(self) -> CheckResult:
        """Read-only checks. No side effects."""
        return CheckResult()
