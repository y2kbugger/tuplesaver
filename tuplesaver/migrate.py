"""SQLite schema migration management for TupleSaver models."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

import apsw
import colors as clr

from .engine import Engine
from .model import Row, TableRow
from .sql import generate_create_table_ddl


def _parse_migration_number(filename: str) -> int | None:
    """Extract migration number from filename like '001.name.sql'."""
    match = re.match(r"^(\d+)\.", filename)
    return int(match.group(1)) if match else None


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
    MISMATCH = "mismatch"  # DB doesn't match models, need to generate
    CURRENT = "current"  # Fully in sync


@dataclass
class TableSchema:
    """Schema comparison for a single model/table."""

    table_name: str
    model_name: str
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
    all_filenames: list[str] = field(default_factory=list)  # all known migration filenames

    @property
    def has_schema_mismatch(self) -> bool:
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
        if self.has_schema_mismatch:
            return State.MISMATCH
        return State.CURRENT

    def status_lines(self) -> list[tuple[bool, str, str, str, str]]:
        """Compute compact status lines.

        Returns list of ``(show, ref, local, model, name)`` tuples.

        * **show** - ``True`` when any indicator is set (row visible in output).
        * **ref** - ``"P"`` pending, ``"C"`` conflicted, ``" "`` up-to-date.
        * **local** - ``"P"`` pending, ``"D"`` diverged, ``" "`` up-to-date.
        * **model** - ``"M"`` modified, ``"U"`` untracked, ``" "`` up-to-date.
        * **name** - migration filename *or* model name (never mixed).

        Migrations come first (sorted by filename), then models (sorted by name).
        """
        lines: list[tuple[bool, str, str, str, str]] = []

        pending_nums = {_parse_migration_number(f) for f in self.pending}
        divergent_nums = {_parse_migration_number(f) for f in self.divergent} | {_parse_migration_number(f) for f in self.divergent_missing}
        ref_pending_nums = {_parse_migration_number(f) for f in self.ref_pending}
        conflicted_nums = {_parse_migration_number(f) for f in self.conflicted} | {_parse_migration_number(f) for f in self.conflicted_missing}

        for filename in sorted(set(self.all_filenames)):
            num = _parse_migration_number(filename)
            ref = "C" if num in conflicted_nums else ("P" if num in ref_pending_nums else " ")
            local = "D" if num in divergent_nums else ("P" if num in pending_nums else " ")
            model = " "
            show = ref != " " or local != " "
            lines.append((show, ref, local, model, filename))

        for table_name in sorted(self.schema):
            ts = self.schema[table_name]
            if not ts.exists:
                model = "U"
            elif not ts.is_current:
                model = "M"
            else:
                model = " "
            show = model != " "
            lines.append((show, " ", " ", model, ts.model_name))

        return lines

    def status(self) -> str:
        """Human-readable compact status."""
        return format_status(self)


def format_status(result: CheckResult) -> str:
    """Render *result* as a compact, ``git status``-style string.

    Columns: ``Ref  Local  Model | Name``

    Uses ``ansicolors`` for coloured indicators: green (ref),
    yellow (local), red (model).
    """
    lines = result.status_lines()
    visible = [(ref, local, model, name) for show, ref, local, model, name in lines if show]

    if not visible and not result.errors:
        return make_status_summary(result)

    parts: list[str] = []

    parts.append(make_status_summary(result))

    if result.errors:
        for e in result.errors:
            parts.append(f"{clr.red('E')} {e}")

    for ref, local, model, name in visible:
        ref_s = clr.green(ref) if ref != " " else ref
        local_s = clr.yellow(local) if local != " " else local
        model_s = clr.red(model) if model != " " else model
        parts.append(f"{ref_s}{local_s}{model_s} {name}")

    return "\n".join(parts)


def make_status_summary(result: CheckResult) -> str:
    """Build the single-line state summary header (e.g. ``PENDING: 1 migration Pending …``)."""

    def _pl(n: int, word: str) -> str:
        return f"{n} {word}{'s' if n != 1 else ''}"

    state = result.state
    label = state.value.upper()

    if state == State.ERROR:
        detail = _pl(len(result.errors), "error")
    elif state == State.CONFLICTED:
        n = len(result.conflicted) + len(result.conflicted_missing)
        detail = f"{_pl(n, 'script')} {clr.green('Conflicted')} with production reference, restore scripts from prod to resolve"
    elif state == State.DIVERGED:
        n = len(result.divergent) + len(result.divergent_missing)
        detail = f"{_pl(n, 'script')} {clr.yellow('Diverged')} from devlocal DB, rollback devlocal to the production reference to resolve"
    elif state == State.MISMATCH:
        untracked = sum(1 for s in result.schema.values() if not s.exists)
        mismatched = sum(1 for s in result.schema.values() if s.exists and not s.is_current)
        summary_parts: list[str] = []
        if untracked:
            summary_parts.append(f"{_pl(untracked, 'model')} {clr.red('Untracked')}")
        if mismatched:
            summary_parts.append(f"{_pl(mismatched, 'model')} {clr.red('Mismatched')}")
        detail = ", ".join(summary_parts) + ", generate migrations to resolve"
    elif state == State.PENDING:
        summary_parts = []
        local_n = len(result.pending)
        ref_n = len(result.ref_pending)
        if local_n:
            summary_parts.append(f"{_pl(local_n, 'migration')} {clr.yellow('Pending')} on devlocal DB")
        if ref_n:
            summary_parts.append(f"{ref_n} {clr.green('Pending')} production deployment")
        detail = ", ".join(summary_parts)
    elif state == State.CURRENT:
        ref_n = len(result.ref_pending)
        if ref_n:
            detail = f"{ref_n} {clr.green('Pending')} production deployment"
        else:
            detail = "schema is up to date"
    else:
        detail = ""

    return f"{label}: {detail}"


def _backup_with_retry(backup: object, *, retries: int = 8, delay: float = 0.1) -> None:
    """Drive a single apsw backup to completion, retrying on BusyError.

    Calls ``step(-1)`` to copy all pages in one shot.  If the source DB
    is momentarily locked (e.g. WAL checkpoint in progress), we sleep
    briefly and retry up to *retries* times before re-raising.
    """
    import apsw

    for attempt in range(retries):
        try:
            backup.step(-1)  # type: ignore[union-attr]
            return
        except apsw.BusyError:
            if attempt + 1 == retries:
                raise
            time.sleep(delay * (attempt + 1))


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
        model_name = model.meta.model_name
        expected_sql = generate_create_table_ddl(model)
        actual_sql = self._get_table_sql(table_name)
        return TableSchema(
            table_name=table_name,
            model_name=model_name,
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

    def _validate_migration_files(self, files: list[tuple[int, str, Path]]) -> list[str]:
        """Validate migration files in the migrations directory.

        Checks:
        - Non-.sql files are silently ignored
        - .sql files must have exactly two periods (NNN.name.sql)
        - The prefix before the first period must be an integer
        - No duplicate migration numbers
        - No gaps in migration sequence

        Returns a list of error strings (empty if all valid).
        """
        errors: list[str] = []
        migrations_dir = self.migrations_dir

        if migrations_dir.exists():
            for f in migrations_dir.iterdir():
                if not f.is_file():
                    continue
                # Ignore non-.sql files silently
                if not f.name.endswith(".sql"):
                    continue
                # .sql files must have exactly two periods
                parts = f.name.split(".")
                if len(parts) != 3:
                    errors.append(f"Invalid migration filename '{f.name}' — expected format NNN.name.sql (exactly two periods)")
                    continue
                # The prefix must be an integer
                try:
                    int(parts[0])
                except ValueError:
                    errors.append(f"Invalid migration filename '{f.name}' — prefix '{parts[0]}' is not an integer")

        if files:
            # Check for duplicate numbers
            number_to_files: dict[int, list[str]] = {}
            for number, _name, path in files:
                number_to_files.setdefault(number, []).append(path.name)
            for number, filenames in sorted(number_to_files.items()):
                if len(filenames) > 1:
                    errors.append(f"Duplicate migration number {number}: {', '.join(sorted(filenames))} — rename or remove files so each number is unique")

            # Check for gaps (numbers must be 1..max with no gaps)
            unique_numbers = sorted(number_to_files.keys())
            expected = list(range(1, unique_numbers[-1] + 1))
            missing_numbers = sorted(set(expected) - set(unique_numbers))
            if missing_numbers:
                have = ', '.join(str(n) for n in unique_numbers)
                need = ', '.join(str(n) for n in missing_numbers)
                errors.append(f"Gap in migration sequence: have [{have}], missing [{need}] — renumber files to be sequential starting from 1 with no gaps")

        return errors

    def check(self) -> CheckResult:
        """Read-only checks. No side effects."""
        schema = {m.meta.table_name: self._compute_table_schema(m) for m in self.models}

        # Get migration files and applied migrations
        files = self._get_migration_files()
        applied = self._get_applied_migrations()
        ref_applied = self._get_ref_applied_migrations()
        ref_pending = []

        # Validate migration files
        errors = self._validate_migration_files(files)

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

        # Build canonical filename per migration number.
        # Priority (highest wins): refdb > on-disk file > localdb.
        canonical: dict[int, str] = {}
        for number, (recorded_filename, _) in applied.items():
            canonical[number] = recorded_filename
        for number, _name, path in files:
            canonical[number] = path.name
        for number, (ref_filename, _) in ref_applied.items():
            canonical[number] = ref_filename
        all_filenames_set = set(canonical.values())

        return CheckResult(
            schema=schema,
            pending=pending,
            ref_pending=ref_pending,
            divergent=divergent,
            divergent_missing=divergent_missing,
            conflicted=conflicted,
            conflicted_missing=conflicted_missing,
            errors=errors,
            all_filenames=sorted(all_filenames_set),
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
        """Auto-generate a migration script based on schema mismatch.

        Only allowed in MISMATCH state. Returns the path to the generated file,
        or None if nothing to generate.
        """
        result = self.check()
        if result.state != State.MISMATCH:
            raise RuntimeError(f"generate() only allowed in MISMATCH state, current state is {result.state.value}")

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

    def apply(self, filename: str, *, retries: int = 8, retry_delay: float = 0.1) -> None:
        """Run one migration script inside an IMMEDIATE transaction.

        Only allowed in PENDING state. Executes the script SQL and records
        it in the _migrations table atomically — if anything fails, the
        transaction is rolled back so no partial application is left behind.

        Retries on ``apsw.BusyError`` (DB locked) up to *retries* times
        with exponential backoff starting at *retry_delay* seconds.
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

        self._ensure_migrations_table()

        conn = self.engine.connection
        prev_mode = conn.transaction_mode
        conn.transaction_mode = "IMMEDIATE"
        try:
            for attempt in range(retries):
                try:
                    started_at = datetime.now(UTC).isoformat()

                    with conn:
                        # Execute the migration script (APSW handles multiple statements)
                        conn.execute(script)

                        finished_at = datetime.now(UTC).isoformat()

                        # Record in _migrations table (dogfoods engine.save with force_insert)
                        migration = Migration(
                            filename=filename,
                            script=script,
                            started_at=started_at,
                            finished_at=finished_at,
                            id=number,
                        )
                        self.engine.save(migration, force_insert=True)
                    return  # committed
                except apsw.BusyError:
                    if attempt + 1 == retries:
                        raise
                    time.sleep(retry_delay * (attempt + 1))
        finally:
            conn.transaction_mode = prev_mode

    @property
    def ref_path(self) -> Path:
        """Return the reference DB path (e.g., mydb.sqlite.ref)."""
        return self.db_path.parent / f"{self.db_path.name}.ref"

    def save_ref(self) -> None:
        """Save a reference snapshot of the working DB using SQLite backup API."""
        import apsw

        dest = apsw.Connection(str(self.ref_path))
        with dest.backup("main", self.engine.connection, "main") as backup:
            _backup_with_retry(backup)
        dest.close()

    def restore_db(self, path: Path | None = None) -> None:
        """Restore working DB using SQLite backup API.

        If *path* is given, restore from that specific file (e.g. a backup).
        Otherwise fall back to .ref; if .ref is also missing, restore to
        greenfield (empty DB).

        This fixes DIVERGED state (scripts differ from working DB but match ref).
        After restore_db, scripts that were applied in working DB become pending again.
        """
        import apsw

        if path is not None:
            if not path.exists():
                raise FileNotFoundError(f"Backup not found: {path}")
            source = apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY)
        elif self.ref_path.exists():
            source = apsw.Connection(str(self.ref_path), flags=apsw.SQLITE_OPEN_READONLY)
        else:
            # No ref → empty in-memory DB as source (greenfield)
            source = apsw.Connection(":memory:")

        with self.engine.connection.backup("main", source, "main") as backup:
            _backup_with_retry(backup)

        source.close()

    @property
    def backup_dir(self) -> Path:
        """Return the backup directory path (e.g., mydb.sqlite.bak/)."""
        return self.db_path.parent / f"{self.db_path.name}.bak"

    def backup(self) -> Path:
        """Create a timestamped backup of the working DB using SQLite backup API.

        Backup is stored in ``<db>.bak/<timestamp>.<highest_migration_num>.<db_name>``
        e.g.  ``mydb.sqlite.bak/2026-02-10T14-30-05.123456.003.mydb.sqlite``

        The filename sorts lexically by time, encodes the highest applied
        migration number for quick identification, and avoids characters
        (like ``:``) that are illegal on Windows.

        Returns the path to the created backup file.
        """
        import apsw

        self.backup_dir.mkdir(exist_ok=True)

        # Determine highest applied migration number
        applied = self._get_applied_migrations()
        highest = max(applied.keys()) if applied else 0

        # Build filename: timestamp.NNN.dbname
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S.%f")  # µs precision
        filename = f"{ts}.{highest:03d}.{self.db_path.name}"
        dest_path = self.backup_dir / filename

        if dest_path.exists():
            raise FileExistsError(f"Backup already exists: {dest_path}")

        dest = apsw.Connection(str(dest_path))
        with dest.backup("main", self.engine.connection, "main") as b:
            _backup_with_retry(b)
        dest.close()

        return dest_path

    def list_backups(self) -> list[Path]:
        """List available backup files, sorted by name (most recent first)."""
        if not self.backup_dir.exists():
            return []
        return sorted((f for f in self.backup_dir.iterdir() if f.is_file()), reverse=True)

    def restore_scripts(self) -> None:
        """Restore migration script files from the .ref DB's _migrations table.

        Overwrites files on disk with the script content recorded in the ref DB,
        and recreates missing files. Does not delete any files that are not in
        the ref DB.

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
