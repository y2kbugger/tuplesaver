"""SQLite schema migration management for TupleSaver models."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

from .engine import Engine
from .model import Row, TableRow
from .sql import generate_create_table_ddl


class SqliteMaster(Row):
    __tablename__ = "sqlite_master"
    sql: str
    type: str
    name: str


class Migration(TableRow):
    """History of applied Migration scripts, PK id is the migration number."""

    __tablename__ = "_migrations"

    filename: str
    script: str
    started_at: str
    finished_at: str


class State(Enum):
    ERROR = "error"  # Blocking issues (duplicate numbers, gaps)
    CONFLICTED = "conflicted"  # Script on disk differs from ref DB (production)
    DIVERGED = "diverged"  # Script on disk differs from working DB (not ref)
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
    divergent: list[str] = field(default_factory=list)  # disk content != working DB recorded content
    divergent_missing: list[str] = field(default_factory=list)  # applied in working DB but file missing
    ref_pending: list[str] = field(default_factory=list)  # scripts on disk not yet applied to ref DB
    conflicted: list[str] = field(default_factory=list)  # disk content != ref DB recorded content
    conflicted_missing: list[str] = field(default_factory=list)  # applied in ref DB but file missing
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
        if self.conflicted or self.conflicted_missing:
            return State.CONFLICTED
        if self.divergent or self.divergent_missing:
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
            lines.append("Errors:\n" + "\n".join(f"  {e}" for e in self.errors))
        if self.conflicted:
            lines.append("Conflicted (changed from ref):\n" + "\n".join(f"  {d}" for d in self.conflicted))
        if self.conflicted_missing:
            lines.append("Conflicted (missing from ref):\n" + "\n".join(f"  {d}" for d in self.conflicted_missing))
        if self.divergent:
            lines.append("Diverged (changed):\n" + "\n".join(f"  {d}" for d in self.divergent))
        if self.divergent_missing:
            lines.append("Diverged (missing):\n" + "\n".join(f"  {d}" for d in self.divergent_missing))
        if self.pending:
            lines.append("Pending:\n" + "\n".join(f"  {p}" for p in self.pending))
        if self.ref_pending:
            lines.append("Pending (not yet in ref/production):\n" + "\n".join(f"  {p}" for p in self.ref_pending))
        if self.has_schema_drift:
            missing = [name for name, s in self.schema.items() if not s.exists]
            changed = [name for name, s in self.schema.items() if s.exists and not s.is_current]
            if missing:
                lines.append("Tables to create:\n" + "\n".join(f"  {t}" for t in missing))
            if changed:
                lines.append("Tables with schema changes:\n" + "\n".join(f"  {t}" for t in changed))
        if not lines:
            lines.append("Current: schema is up to date")
        return "\n".join(lines)


class Migrate:
    def __init__(self, db_path: str | Path, models: list[type[TableRow]]) -> None:
        self.db_path = Path(db_path)
        self.engine = Engine(str(self.db_path))
        self.models = models

    def _get_table_sql(self, table_name: str) -> str | None:
        """Get CREATE TABLE sql from sqlite_master, or None if not exists."""
        sm = self.engine.find_by(SqliteMaster, type="table", name=table_name)
        return sm.sql if sm else None

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

    def _ensure_migrations_table(self) -> None:
        """Create _migrations table if it doesn't exist (dogfoods ensure_table_created)."""
        self.engine.ensure_table_created(Migration)

    def _get_applied_migrations(self) -> dict[int, tuple[str, str]]:
        """Get applied migrations as {id: (filename, script)} from _migrations table."""
        self._ensure_migrations_table()
        cur = self.engine.select(Migration)
        return {row.id: (row.filename, row.script) for row in cur.fetchall()}  # type: ignore[dict-item-type]

    def _get_ref_applied_migrations(self) -> dict[int, tuple[str, str]]:
        """Get applied migrations from the .ref DB as {id: (filename, script)}.

        Returns empty dict if .ref doesn't exist or has no _migrations table.
        """
        if not self.ref_path.exists():
            return {}

        ref_engine = Engine(str(self.ref_path))
        if not ref_engine.find_by(SqliteMaster, type="table", name="_migrations"):
            return {}

        cur = ref_engine.select(Migration)
        return {row.id: (row.filename, row.script) for row in cur.fetchall()}  # type: ignore[dict-item-type]

    def check(self) -> CheckResult:
        """Read-only checks. No side effects."""
        schema = {m.meta.table_name: self._compute_table_schema(m) for m in self.models}

        # Get migration files and applied migrations
        files = self._get_migration_files()
        applied = self._get_applied_migrations()
        ref_applied = self._get_ref_applied_migrations()
        ref_pending = []

        # Build set of file numbers for quick lookup
        file_numbers = {number for number, _name, _path in files}

        # scripts on disk that are not yet applied in working DB
        pending = []
        # migration scripts differing from the working DB
        divergent = []
        divergent_missing = []
        # migration scripts differing from the ref DB (i.e. production)
        conflicted = []
        conflicted_missing = []

        for number, _name, path in files:
            current_script = path.read_text().replace("\r\n", "\n")

            if number not in applied:
                pending.append(path.name)
            else:
                # Check if file content matches what was applied in working DB
                _recorded_filename, recorded_script = applied[number]
                if current_script != recorded_script:
                    divergent.append(path.name)

            # Check against ref DB (independent of working DB comparison)
            if number not in ref_applied:
                ref_pending.append(path.name)
            else:
                _ref_filename, ref_script = ref_applied[number]
                if current_script != ref_script:
                    conflicted.append(path.name)

        # Check for applied migrations with missing files (working DB)
        for number, (recorded_filename, _recorded_script) in applied.items():
            if number not in file_numbers:
                divergent_missing.append(recorded_filename)

        # Check for applied migrations with missing files (ref DB)
        for number, (ref_filename, _ref_script) in ref_applied.items():
            if number not in file_numbers:
                conflicted_missing.append(ref_filename)

        return CheckResult(
            schema=schema,
            pending=pending,
            ref_pending=ref_pending,
            divergent=divergent,
            divergent_missing=divergent_missing,
            conflicted=conflicted,
            conflicted_missing=conflicted_missing,
        )

    @property
    def migrations_dir(self) -> Path:
        """Return the migrations directory path (e.g., mydb.sqlite.migrations/)."""
        return self.db_path.parent / f"{self.db_path.name}.migrations"

    def _get_migration_files(self) -> list[tuple[int, str, Path]]:
        """Get all migration files as (number, name, path) tuples, sorted by number."""
        migrations_dir = self.migrations_dir
        if not migrations_dir.exists():
            return []

        results = []
        for f in migrations_dir.glob("*.sql"):
            match = re.match(r"^(\d+)\.(.+)\.sql$", f.name)
            if match:
                results.append((int(match.group(1)), match.group(2), f))

        return sorted(results, key=lambda x: x[0])

    def _get_next_migration_number(self) -> int:
        """Determine the next migration number based on existing files."""
        files = self._get_migration_files()
        if not files:
            return 1
        return files[-1][0] + 1

    def _generate_migration_name(self, schema: dict[str, TableSchema]) -> str:
        """Generate a descriptive name for the migration based on changes."""
        missing = [name for name, s in schema.items() if not s.exists]
        changed = [name for name, s in schema.items() if s.exists and not s.is_current]

        parts = []
        if missing:
            parts.append("create_" + "_".join(missing).lower())
        if changed:
            parts.append("alter_" + "_".join(changed).lower())

        return "_".join(parts) if parts else "migration"

    def generate(self) -> Path | None:
        """Auto-generate a migration script based on schema drift.

        Only allowed in DRIFT state. Returns the path to the generated file,
        or None if nothing to generate.
        """
        result = self.check()
        if result.state != State.DRIFT:
            raise RuntimeError(f"generate() only allowed in DRIFT state, current state is {result.state.value}")

        # Build migration SQL
        sql_parts = []

        for table_name, table_schema in result.schema.items():
            if not table_schema.exists:
                # Table doesn't exist - create it
                sql_parts.append(f"{table_schema.expected_sql};")
            elif not table_schema.is_current:
                # Table exists but schema differs - drop and recreate
                sql_parts.append(f"DROP TABLE {table_name};")
                sql_parts.append(f"{table_schema.expected_sql};")

        if not sql_parts:
            return None

        # Create migrations directory if needed
        migrations_dir = self.migrations_dir
        migrations_dir.mkdir(exist_ok=True)

        # Generate filename
        number = self._get_next_migration_number()
        name = self._generate_migration_name(result.schema)
        filename = f"{number:03d}.{name}.sql"
        filepath = migrations_dir / filename

        # Write the migration file
        sql_content = "\n\n".join(sql_parts)
        filepath.write_text(sql_content + "\n")

        return filepath

    def apply(self, filename: str) -> None:
        """Run one migration script.

        Only allowed in PENDING state. Executes the script SQL and records
        it in the _migrations table.
        """
        result = self.check()
        if result.state != State.PENDING:
            raise RuntimeError(f"apply() only allowed in PENDING state, current state is {result.state.value}")

        if filename not in result.pending:
            raise ValueError(f"Migration '{filename}' is not pending")

        # Parse filename to get number
        match = re.match(r"^(\d+)\.(.+)\.sql$", filename)
        if not match:
            raise ValueError(f"Invalid migration filename format: {filename}")

        number = int(match.group(1))
        filepath = self.migrations_dir / filename
        script = filepath.read_text()

        # Normalize newlines
        script = script.replace("\r\n", "\n")

        started_at = datetime.now(UTC).isoformat()

        # Execute the migration script (APSW handles multiple statements)
        self.engine.connection.execute(script)

        finished_at = datetime.now(UTC).isoformat()

        # Record in _migrations table (dogfoods engine.save with force_insert)
        self._ensure_migrations_table()
        migration = Migration(filename=filename, script=script, started_at=started_at, finished_at=finished_at, id=number)
        self.engine.save(migration, force_insert=True)

    @property
    def ref_path(self) -> Path:
        """Return the reference DB path (e.g., mydb.sqlite.ref)."""
        return self.db_path.parent / f"{self.db_path.name}.ref"

    def save_ref(self) -> None:
        """Save a reference snapshot of the working DB using SQLite backup API."""
        import apsw

        dest = apsw.Connection(str(self.ref_path))
        with dest.backup("main", self.engine.connection, "main") as backup:
            backup.step(-1)  # copy all pages in one step
        dest.close()

    def restore_db(self) -> None:
        """Restore working DB from .ref using SQLite backup API.

        If .ref exists, copies it over the working DB via backup.
        If .ref is missing, restore to greenfield (empty DB).

        This fixes DIVERGED state (scripts differ from working DB but match ref).
        After restore_db, scripts that were applied in working DB become pending again.
        """
        import apsw

        if self.ref_path.exists():
            source = apsw.Connection(str(self.ref_path), flags=apsw.SQLITE_OPEN_READONLY)
        else:
            # No ref → empty in-memory DB as source (greenfield)
            source = apsw.Connection(":memory:")

        with self.engine.connection.backup("main", source, "main") as backup:
            backup.step(-1)  # copy all pages in one step

        source.close()
        self.engine.connection.execute("PRAGMA journal_mode=WAL")

    def restore_scripts(self) -> None:
        """Restore migration script files from the .ref DB's _migrations table.

        Only allowed in CONFLICTED state. Overwrites files on disk with the
        script content recorded in the ref DB, and recreates missing files.

        This fixes CONFLICTED state (scripts differ from ref / production).
        """
        result = self.check()
        if result.state != State.CONFLICTED:
            raise RuntimeError(f"restore_scripts() only allowed in CONFLICTED state, current state is {result.state.value}")

        ref_applied = self._get_ref_applied_migrations()

        # Ensure migrations directory exists
        self.migrations_dir.mkdir(exist_ok=True)

        # Restore scripts from ref DB, overwriting or creating files as needed
        for _n, (ref_filename, ref_script) in ref_applied.items():
            if ref_filename in result.conflicted + result.conflicted_missing:
                filepath = self.migrations_dir / ref_filename
                filepath.write_text(ref_script)
