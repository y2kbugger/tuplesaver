"""
Migrate test scenarios.

Each scenario folder in migrate_scenarios/ contains:
- An initial .sqlite file (or missing for fresh DB)
- A .migrations/ folder with scripts (or missing)
- Optional m.py with models (loaded automatically if present)

Test Fixtures copy the scenario to a temp dir, then return a Migrate object.
Use @pytest.mark.scenario("name") to pick the scenario.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

from .migrate import Migrate, State
from .model import TableRow

SCENARIOS_DIR = Path(__file__).parent / "migrate_scenarios"


def load_models_from_file(m_path: Path) -> list[type[TableRow]]:
    """Load all TableRow subclasses from a module file."""
    spec = importlib.util.spec_from_file_location("m", m_path)
    if spec is None or spec.loader is None:
        return []
    module = importlib.util.module_from_spec(spec)
    sys.modules["m"] = module
    spec.loader.exec_module(module)

    models = []
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and issubclass(obj, TableRow) and obj is not TableRow:
            models.append(obj)
    return models


@pytest.fixture
def migrate(request: pytest.FixtureRequest, tmp_path: Path) -> Migrate:
    """Copy scenario folder to tmp, return Migrate object with models from m.py."""
    marker = request.node.get_closest_marker("scenario")
    if not marker:
        raise ValueError("Test must use @pytest.mark.scenario('name')")

    name = marker.args[0]
    src = SCENARIOS_DIR / name
    if not src.exists():
        raise ValueError(f"Scenario not found: {src}")

    dst = tmp_path / name
    shutil.copytree(src, dst)

    # Load models from m.py if it exists
    m_path = dst / "m.py"
    models = load_models_from_file(m_path) if m_path.exists() else []

    db_path = dst / "db.sqlite"
    return Migrate(db_path, models=models)


@pytest.mark.scenario("empty_db")
def test_migrate__empty_db__engine_works(migrate: Migrate):
    """Engine is functional and db is in temp dir."""
    db_path = Path(migrate.engine.db_path)
    assert db_path.stem == "db"
    assert db_path.suffix == ".sqlite"
    assert db_path.parent.name == "empty_db"
    assert "pytest" in str(db_path)  # is in tmp dir
    assert db_path.exists()
    migrate.engine.connection.execute("SELECT 1")


@pytest.mark.scenario("empty_db")
def test_migrate__empty_db_no_models__is_current(migrate: Migrate):
    """Empty db + no models = CURRENT."""
    result = migrate.check()
    assert result.state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__fresh_db_with_model__is_drift(migrate: Migrate):
    """Fresh db + models = DRIFT, schema shows table needs to be created."""
    result = migrate.check()
    assert result.state == State.DRIFT
    assert "User" in result.schema
    user_schema = result.schema["User"]
    assert not user_schema.exists
    assert user_schema.actual_sql is None
    assert "CREATE TABLE User" in user_schema.expected_sql


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__generate__creates_migration_file(migrate: Migrate):
    """generate() creates a migration script in the migrations folder."""
    result = migrate.check()
    assert result.state == State.DRIFT

    # Generate migration
    filepath = migrate.generate()

    # Verify file was created
    assert filepath is not None
    assert filepath.exists()
    assert filepath.parent == migrate.migrations_dir
    assert filepath.name == "001.create_user.sql"

    # Verify content
    content = filepath.read_text()
    assert "CREATE TABLE User" in content
    assert content.endswith(";\n")


@pytest.mark.scenario("empty_db")
def test_migrate__generate__raises_when_not_drift(migrate: Migrate):
    """generate() raises error when not in DRIFT state."""
    result = migrate.check()
    assert result.state == State.CURRENT

    with pytest.raises(RuntimeError, match="DRIFT"):
        migrate.generate()


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__generate_then_check__is_pending(migrate: Migrate):
    """After generate(), check() returns PENDING state with the new script."""
    migrate.generate()

    result = migrate.check()
    assert result.state == State.PENDING
    assert len(result.pending) == 1
    assert result.pending[0] == "001.create_user.sql"


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__apply__executes_migration(migrate: Migrate):
    """apply() executes the migration and creates the table."""
    migrate.generate()
    result = migrate.check()

    migrate.apply(result.pending[0])

    # Table should now exist
    cursor = migrate.engine.connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='User'")
    assert cursor.fetchone() is not None


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__apply__records_in_migrations_table(migrate: Migrate):
    """apply() records the migration in _migrations table."""
    migrate.generate()
    result = migrate.check()

    migrate.apply(result.pending[0])

    # Check _migrations table
    cursor = migrate.engine.connection.execute("SELECT id, filename, script FROM _migrations WHERE id = 1")
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == 1
    assert row[1] == "001.create_user.sql"
    assert "CREATE TABLE User" in row[2]


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__apply__then_check_is_current(migrate: Migrate):
    """After applying all migrations, check() returns CURRENT."""
    migrate.generate()
    result = migrate.check()

    migrate.apply(result.pending[0])

    result = migrate.check()
    assert result.state == State.CURRENT
    assert len(result.pending) == 0


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__apply__raises_when_not_pending(migrate: Migrate):
    """apply() raises error when not in PENDING state."""
    # Still in DRIFT state, no migration generated
    result = migrate.check()
    assert result.state == State.DRIFT

    with pytest.raises(RuntimeError, match="PENDING"):
        migrate.apply("001.create_user.sql")


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__diverged__edit_after_apply(migrate: Migrate):
    """Editing a migration file after apply causes DIVERGED state."""
    # Setup: generate and apply migration
    migrate.generate()
    result = migrate.check()
    migrate.apply(result.pending[0])

    # Verify we're CURRENT
    result = migrate.check()
    assert result.state == State.CURRENT

    # Edit the migration file
    script_path = migrate.migrations_dir / "001.create_user.sql"
    original = script_path.read_text()
    script_path.write_text(original + "\n-- edited after apply\n")

    # Now check should detect divergence
    result = migrate.check()
    assert result.state == State.DIVERGED
    assert "001.create_user.sql" in result.divergent
    assert len(result.divergent) == 1


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__diverged__missing_applied_script(migrate: Migrate):
    """Deleting a migration file after apply causes DIVERGED state."""
    # Setup: generate and apply migration
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # Delete the migration file
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.unlink()

    # Now check should detect divergence
    result = migrate.check()
    assert result.state == State.DIVERGED
    assert "001.create_user.sql" in result.divergent_missing
    assert len(result.divergent_missing) == 1


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__generate__raises_when_diverged(migrate: Migrate):
    """generate() raises error when in DIVERGED state."""
    # Setup: generate, apply, then edit to cause divergence
    migrate.generate()
    result = migrate.check()
    migrate.apply(result.pending[0])

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")

    result = migrate.check()
    assert result.state == State.DIVERGED

    with pytest.raises(RuntimeError, match="DRIFT"):
        migrate.generate()


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__apply__raises_when_diverged(migrate: Migrate):
    """apply() raises error when in DIVERGED state."""
    # Setup: generate, apply, then edit to cause divergence
    migrate.generate()
    result = migrate.check()
    migrate.apply(result.pending[0])

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")

    result = migrate.check()
    assert result.state == State.DIVERGED

    with pytest.raises(RuntimeError, match="PENDING"):
        migrate.apply("001.create_user.sql")


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__model_changed_need_new_migration(migrate: Migrate):
    """Full workflow: model change → generate → apply, then change model again."""
    # 1. Initial state: DRIFT (model exists but table doesn't)
    result = migrate.check()
    assert result.state == State.DRIFT

    # 2. Generate first migration
    migrate.generate()
    result = migrate.check()
    assert result.state == State.PENDING
    assert result.pending == ["001.create_user.sql"]

    # 3. Apply first migration
    migrate.apply(result.pending[0])
    result = migrate.check()
    assert result.state == State.CURRENT

    # 4. Simulate model change: add a new field to User
    # Create an updated User model with an avatar field
    class User(TableRow):
        name: str
        email: str
        avatar: str  # new field

    migrate.models = [User]

    # 5. Check detects schema drift
    result = migrate.check()
    assert result.state == State.DRIFT
    assert "User" in result.schema
    assert not result.schema["User"].is_current

    # 6. Generate second migration
    filepath = migrate.generate()
    assert filepath is not None
    assert filepath.name == "002.alter_user.sql"

    # 7. Apply second migration
    result = migrate.check()
    assert result.state == State.PENDING
    assert result.pending == ["002.alter_user.sql"]
    migrate.apply(result.pending[0])

    # 8. Back to CURRENT
    result = migrate.check()
    assert result.state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__iterate_then_consolidate(migrate: Migrate):
    """Option B: Consolidate multiple migrations via restore.

    After iterating with multiple migrations delete them, restore to .ref, and regenerate a single migration.

    This simulates the reference implementation of a devserver integration.
    """
    # Setup: create .ref as empty DB (simulates production baseline)
    migrate.save_ref()

    # 1. First iteration: create table
    migrate.generate()  # 001.create_user.sql
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # 2. Second iteration: add avatar field
    class UserV2(TableRow):
        name: str
        email: str
        avatar: str

    UserV2.__name__ = "User"
    migrate.models = [UserV2]

    migrate.generate()  # 002.alter_user.sql
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # 3. Third iteration: add bio field
    class UserV3(TableRow):
        name: str
        email: str
        avatar: str
        bio: str

    UserV3.__name__ = "User"
    migrate.models = [UserV3]

    migrate.generate()  # 003.alter_user.sql
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # Verify we have 3 migration files
    files = list(migrate.migrations_dir.glob("*.sql"))
    assert len(files) == 3

    # 4. Consolidate: delete all migration files
    for f in files:
        f.unlink()

    assert migrate.check().state == State.DIVERGED

    # 5. Restore to .ref (empty DB)
    migrate.restore_db()

    # 6. Regenerate single migration with all changes
    result = migrate.check()
    assert result.state == State.DRIFT

    filepath = migrate.generate()
    assert filepath is not None
    assert filepath.name == "001.create_user.sql"  # starts fresh at 001

    # 7. Apply the consolidated migration
    result = migrate.check()
    assert result.state == State.PENDING
    assert len(result.pending) == 1

    migrate.apply(result.pending[0])

    # 8. Verify CURRENT with final schema
    result = migrate.check()
    assert result.state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__missing_ref__equivalent_to_empty_db(migrate: Migrate):
    """Restoring with no .ref file is equivalent to starting from an empty db."""
    # Ensure no .ref exists
    assert not migrate.ref_path.exists()

    # Setup: generate and apply migration so DB has state
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # Edit migration file to cause diverged state
    script_path = next(migrate.migrations_dir.glob("001.*.sql"))
    script_path.write_text(script_path.read_text() + "\n-- edited\n")
    assert migrate.check().state == State.DIVERGED

    # Restore without .ref → should act like empty db
    migrate.restore_db()

    # After restore, DB should be empty: no tables, no _migrations
    result = migrate.check()
    assert result.state == State.PENDING  # edited migration is ready to be reapplied
    assert not result.applied  # no applied migrations
    assert len(result.pending) == 1  # one pending migration
    assert "User" in result.schema
    assert not result.schema["User"].exists  # table doesn't exist


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__save_ref__creates_ref_via_backup(migrate: Migrate):
    """save_ref() creates a .ref file that captures current DB state."""
    # Generate and apply so DB has a table + _migrations row
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    assert not migrate.ref_path.exists()
    migrate.save_ref()
    assert migrate.ref_path.exists()

    # Verify by restoring and checking: round-trip proves the ref captured state
    # (add a second model so the DB diverges from ref first)
    class Post(TableRow):
        title: str
        body: str

    migrate.models = [migrate.models[0], Post]
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT
    assert migrate.check().schema["Post"].exists

    migrate.restore_db()
    result = migrate.check()
    # Post table gone, User table still there — ref was faithfully captured
    assert not result.schema["Post"].exists
    assert result.schema["User"].exists


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__restore__returns_to_ref_state(migrate: Migrate):
    """Restore brings the DB back to the exact state captured in .ref.

    Workflow:
      1. Generate + apply User migration → CURRENT, make .ref
      2. Add Post model, generate + apply → CURRENT with both tables
      3. Restore → DB state matches step 1 snapshot
    """
    # -- Step 1: one model (User), generate, apply, make ref -------------------
    migrate.generate()  # 001.create_user.sql
    migrate.apply(migrate.check().pending[0])

    ref_check = migrate.check()
    assert ref_check.state == State.CURRENT
    assert ref_check.schema["User"].exists
    assert ref_check.schema["User"].is_current

    migrate.save_ref()

    # -- Step 2: add a second model (Post), generate, apply --------------------
    class Post(TableRow):
        title: str
        body: str

    migrate.models = [migrate.models[0], Post]

    migrate.generate()  # 002.create_post.sql
    migrate.apply(migrate.check().pending[0])

    after_post = migrate.check()
    assert after_post.state == State.CURRENT
    assert after_post.schema["Post"].exists
    assert after_post.schema["Post"].is_current

    # -- Step 3: restore -------------------------------------------------------
    migrate.restore_db()

    restored = migrate.check()

    # User table still present and unchanged
    assert restored.schema["User"].exists
    assert restored.schema["User"].is_current

    # Post table is gone
    assert not restored.schema["Post"].exists

    # 002 migration file is on disk but not yet applied → PENDING
    assert restored.state == State.PENDING
    assert "002.create_post.sql" in restored.pending


# --- CONFLICTED state tests ---


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__conflicted__edit_after_apply_with_ref(migrate: Migrate):
    """Editing a script that's recorded in ref DB → CONFLICTED (not just DIVERGED).

    Workflow:
      1. Generate + apply → CURRENT
      2. save_ref() → ref captures the applied script
      3. Edit script file on disk
      4. check() → CONFLICTED (file ≠ ref), also DIVERGED (file ≠ working)
         but CONFLICTED wins priority
    """
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    migrate.save_ref()

    # Edit the migration file
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- conflicting edit\n")

    result = migrate.check()
    assert result.state == State.CONFLICTED
    assert "001.create_user.sql" in result.conflicted
    # Also diverged from working DB
    assert "001.create_user.sql" in result.divergent


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__conflicted__missing_file_with_ref(migrate: Migrate):
    """Deleting a script that's recorded in ref DB → CONFLICTED."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    # Delete the migration file
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.unlink()

    result = migrate.check()
    assert result.state == State.CONFLICTED
    assert "001.create_user.sql" in result.conflicted_missing
    assert "001.create_user.sql" in result.divergent_missing


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__diverged_not_conflicted__no_ref(migrate: Migrate):
    """Editing a script with no .ref → DIVERGED (never CONFLICTED)."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    # No save_ref() — ref doesn't exist
    assert not migrate.ref_path.exists()

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")

    result = migrate.check()
    assert result.state == State.DIVERGED
    assert "001.create_user.sql" in result.divergent
    assert not result.conflicted
    assert not result.conflicted_missing


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__diverged_not_conflicted__ref_has_no_record(migrate: Migrate):
    """Script diverges from working DB, but ref has no record of it → DIVERGED only.

    Workflow:
      1. save_ref() with empty DB (ref has no _migrations)
      2. Generate + apply → CURRENT
      3. Edit file → DIVERGED (ref doesn't know about this script)
    """
    migrate.save_ref()  # ref is empty, no migrations

    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")

    result = migrate.check()
    assert result.state == State.DIVERGED
    assert "001.create_user.sql" in result.divergent
    assert not result.conflicted


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__restore_scripts__fixes_conflicted(migrate: Migrate):
    """restore_scripts() overwrites files from ref, resolving CONFLICTED.

    Workflow:
      1. Generate + apply → CURRENT, save_ref
      2. Edit file → CONFLICTED
      3. restore_scripts() → file restored from ref
      4. check() → CURRENT (file matches both working and ref)
    """
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    script_path = migrate.migrations_dir / "001.create_user.sql"
    original_content = script_path.read_text()
    script_path.write_text(original_content + "\n-- conflicting edit\n")

    assert migrate.check().state == State.CONFLICTED

    migrate.restore_scripts()

    result = migrate.check()
    assert result.state == State.CURRENT
    assert not result.conflicted
    assert not result.divergent

    # File content restored to original
    assert script_path.read_text() == original_content


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__restore_scripts__recreates_missing_file(migrate: Migrate):
    """restore_scripts() recreates missing files from ref DB."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    script_path = migrate.migrations_dir / "001.create_user.sql"
    original_content = script_path.read_text()
    script_path.unlink()

    assert migrate.check().state == State.CONFLICTED

    migrate.restore_scripts()

    result = migrate.check()
    assert result.state == State.CURRENT
    assert script_path.exists()
    assert script_path.read_text() == original_content


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__restore_scripts__raises_when_not_conflicted(migrate: Migrate):
    """restore_scripts() raises error when not in CONFLICTED state."""
    result = migrate.check()
    assert result.state != State.CONFLICTED

    with pytest.raises(RuntimeError, match="CONFLICTED"):
        migrate.restore_scripts()


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__conflicted_then_restore_db_still_conflicted(migrate: Migrate):
    """restore_db doesn't fix CONFLICTED — only restore_scripts does.

    Workflow:
      1. Generate + apply → CURRENT, save_ref
      2. Edit file → CONFLICTED
      3. restore_db() → DB reset to ref, but file still edited
      4. check() → still CONFLICTED (file ≠ ref)
    """
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- conflicting edit\n")

    assert migrate.check().state == State.CONFLICTED

    migrate.restore_db()

    # Still CONFLICTED because file still differs from ref
    result = migrate.check()
    assert result.state == State.CONFLICTED
    assert "001.create_user.sql" in result.conflicted


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__conflicting_migration_from_other_dev(migrate: Migrate):
    """Simulate another dev pushing a migration with the same number we generated locally.

    Workflow:
      1. Both devs start from same baseline (001 applied, ref saved)
      2. We generate 002.create_post.sql locally
      3. Other dev pushes 002.add_index.sql to prod (arrives via git pull + ref refresh)
      4. Now two 002.*.sql files on disk — number collision
      5. We renumber ours to 003.create_post.sql
      6. check() → PENDING (both 002 and 003 need applying to working DB)
      7. Apply both → CURRENT
    """
    import apsw

    # 1. Shared baseline: 001 applied, ref saved
    migrate.generate()  # 001.create_user.sql
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT
    migrate.save_ref()

    # 2. We add a model and generate our local migration
    class Post(TableRow):
        title: str
        body: str

    migrate.models = [migrate.models[0], Post]
    migrate.generate()  # 002.create_post.sql

    result = migrate.check()
    assert result.state == State.PENDING
    assert "002.create_post.sql" in result.pending

    # 3a. Ref DB refreshed from prod — other dev's migration 002 now recorded
    #     (this happens before git pull, so we don't have their script file yet)
    their_script = "CREATE INDEX idx_user_email ON User(email);\n"
    ref_conn = apsw.Connection(str(migrate.ref_path))
    ref_conn.execute(
        "INSERT INTO _migrations (id, filename, script, started_at, finished_at) VALUES (2, '002.add_index.sql', ?, '2026-02-07T00:00:00', '2026-02-07T00:00:01')",
        (their_script,),
    )
    ref_conn.close()

    # Our 002 file doesn't match ref's 002 → CONFLICTED
    result = migrate.check()
    assert result.state == State.CONFLICTED
    assert "002.create_post.sql" in result.conflicted

    # 3b. Git pull arrives — other dev's script file lands on disk
    (migrate.migrations_dir / "002.add_index.sql").write_text(their_script)

    # 4. Now we have TWO 002.*.sql files — number collision
    files_002 = list(migrate.migrations_dir.glob("002.*.sql"))
    assert len(files_002) == 2

    # State after pull but before fixing — duplicate number is an ERROR
    result = migrate.check()
    # Our 002.create_post.sql still conflicts with ref's 002
    # Their 002.add_index.sql matches ref's 002
    assert "002.create_post.sql" in result.conflicted
    assert "002.add_index.sql" not in result.conflicted
    assert result.state == State.ERROR  # duplicate migration numbers block all other states
    assert any("Duplicate migration number 2" in e for e in result.errors)

    # 5. Renumber our script: 002 → 003
    (migrate.migrations_dir / "002.create_post.sql").rename(migrate.migrations_dir / "003.create_post.sql")

    # 6. check() → PENDING (002 and 003 both need applying to working DB)
    result = migrate.check()
    assert result.state == State.PENDING
    assert "002.add_index.sql" in result.pending
    assert "003.create_post.sql" in result.pending
    # 002 is not ref_pending (it's in ref), but 003 is
    assert "002.add_index.sql" not in result.ref_pending
    assert "003.create_post.sql" in result.ref_pending

    # 7. Apply both in order → CURRENT
    migrate.apply("002.add_index.sql")
    migrate.apply("003.create_post.sql")

    result = migrate.check()
    assert result.state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__ref_pending__no_ref_reports_all(migrate: Migrate):
    """With no .ref, every script on disk is ref_pending."""
    assert not migrate.ref_path.exists()

    migrate.generate()
    result = migrate.check()
    assert result.ref_pending == ["001.create_user.sql"]

    migrate.apply(result.pending[0])

    # Still ref_pending even after applied to working DB
    result = migrate.check()
    assert result.ref_pending == ["001.create_user.sql"]


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__ref_pending__with_ref_tracks_unapplied(migrate: Migrate):
    """With .ref, only scripts not recorded in ref are ref_pending."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()  # ref now has 001

    # 001 is in ref → not ref_pending
    result = migrate.check()
    assert result.ref_pending == []

    # Add second model and generate
    class Post(TableRow):
        title: str
        body: str

    migrate.models = [migrate.models[0], Post]
    migrate.generate()
    migrate.apply(migrate.check().pending[0])

    # 002 not in ref → ref_pending
    result = migrate.check()
    assert result.ref_pending == ["002.create_post.sql"]
    assert "001.create_user.sql" not in result.ref_pending


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__restore_scripts__preserves_unrelated_files(migrate: Migrate):
    """restore_scripts() only touches conflicted files, leaving WIP scripts alone.

    Workflow:
      1. Generate + apply 001 → CURRENT, save_ref
      2. Add Post model, generate 002 (our WIP, not in ref)
      3. Edit 001 on disk → CONFLICTED
      4. restore_scripts() restores 001 but leaves 002 untouched
    """
    migrate.generate()  # 001.create_user.sql
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    # Generate a second WIP migration (ref doesn't know about it)
    class Post(TableRow):
        title: str
        body: str

    migrate.models = [migrate.models[0], Post]
    migrate.generate()  # 002.create_post.sql

    wip_path = migrate.migrations_dir / "002.create_post.sql"
    wip_content = wip_path.read_text()
    assert wip_path.exists()

    # Edit 001 to cause CONFLICTED
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- conflict\n")

    assert migrate.check().state == State.CONFLICTED

    migrate.restore_scripts()

    # 002 WIP file is completely untouched
    assert wip_path.exists()
    assert wip_path.read_text() == wip_content
    assert migrate.check().state == State.PENDING

    # Only two files in migrations dir — nothing added or removed
    all_files = sorted(f.name for f in migrate.migrations_dir.glob("*.sql"))
    assert all_files == ["001.create_user.sql", "002.create_post.sql"]


# --- ERROR state tests ---


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__duplicate_migration_numbers(migrate: Migrate):
    """Two files with the same migration number → ERROR state."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.create_user.sql").write_text("CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);\n")
    (migrate.migrations_dir / "001.create_post.sql").write_text("CREATE TABLE Post (id INTEGER PRIMARY KEY, title TEXT NOT NULL);\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert len(result.errors) == 1
    assert "Duplicate migration number 1" in result.errors[0]
    assert "001.create_post.sql" in result.errors[0]
    assert "001.create_user.sql" in result.errors[0]
    assert "rename or remove" in result.errors[0]


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__gap_in_migration_numbers(migrate: Migrate):
    """Files with a gap in numbering → ERROR state."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.create_user.sql").write_text("CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);\n")
    (migrate.migrations_dir / "003.create_post.sql").write_text("CREATE TABLE Post (id INTEGER PRIMARY KEY, title TEXT NOT NULL);\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("missing [2]" in e for e in result.errors)
    assert any("renumber" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__migration_not_starting_at_one(migrate: Migrate):
    """Single file not starting at 1 → ERROR state (gap: missing 1)."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "002.create_user.sql").write_text("CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL);\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("missing [1]" in e for e in result.errors)
    assert any("renumber" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__multiple_gaps(migrate: Migrate):
    """Files with multiple gaps report all missing numbers."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.first.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "004.fourth.sql").write_text("SELECT 4;\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("missing [2, 3]" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__duplicate_and_gap(migrate: Migrate):
    """Duplicate numbers AND gaps both reported."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.a.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.b.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "003.c.sql").write_text("SELECT 3;\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("Duplicate migration number 1" in e for e in result.errors)
    assert any("missing [2]" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__generate_raises_when_error(migrate: Migrate):
    """generate() raises when in ERROR state."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.a.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.b.sql").write_text("SELECT 1;\n")

    assert migrate.check().state == State.ERROR

    with pytest.raises(RuntimeError, match="DRIFT"):
        migrate.generate()


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__apply_raises_when_error(migrate: Migrate):
    """apply() raises when in ERROR state."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.a.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.b.sql").write_text("SELECT 1;\n")

    assert migrate.check().state == State.ERROR

    with pytest.raises(RuntimeError, match="PENDING"):
        migrate.apply("001.a.sql")


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__status_shows_errors(migrate: Migrate):
    """status() includes error messages."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.a.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.b.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    status = result.status()
    assert "Errors (fix manually before proceeding):" in status
    assert "Duplicate migration number 1" in status
    assert "rename or remove" in status


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__no_files_no_error(migrate: Migrate):
    """No migration files at all → no error (DRIFT because model has no table)."""
    result = migrate.check()
    assert result.state == State.DRIFT
    assert not result.errors


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__sequential_files_no_error(migrate: Migrate):
    """Files numbered 1, 2, 3 sequentially → no error."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.first.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "002.second.sql").write_text("SELECT 2;\n")
    (migrate.migrations_dir / "003.third.sql").write_text("SELECT 3;\n")

    result = migrate.check()
    assert not result.errors
    assert result.state != State.ERROR


# --- Filename validation tests ---


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__sql_file_with_one_period(migrate: Migrate):
    """A .sql file with only one period (e.g., 'create_user.sql') → ERROR."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "create_user.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("create_user.sql" in e and "exactly two periods" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__sql_file_with_three_periods(migrate: Migrate):
    """A .sql file with three periods (e.g., '001.create.user.sql') → ERROR."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.create.user.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("001.create.user.sql" in e and "exactly two periods" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__sql_file_non_integer_prefix(migrate: Migrate):
    """A .sql file with non-integer prefix (e.g., 'abc.name.sql') → ERROR."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "abc.create_user.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("abc.create_user.sql" in e and "not an integer" in e for e in result.errors)


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__non_sql_files_ignored(migrate: Migrate):
    """Non-.sql files (e.g., README.md, .gitkeep) are silently ignored."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "README.md").write_text("Migration notes\n")
    (migrate.migrations_dir / ".gitkeep").write_text("")
    (migrate.migrations_dir / "001.create_user.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    assert not result.errors


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__error__mixed_valid_and_invalid_filenames(migrate: Migrate):
    """Valid .sql files alongside invalid ones: only invalid ones produce errors."""
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.create_user.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "bad.sql").write_text("SELECT 2;\n")
    (migrate.migrations_dir / "notes.txt").write_text("ignore me\n")

    result = migrate.check()
    assert result.state == State.ERROR
    assert any("bad.sql" in e and "exactly two periods" in e for e in result.errors)
    # The valid file and the .txt should not appear in errors
    assert not any("001.create_user.sql" in e for e in result.errors)
    assert not any("notes.txt" in e for e in result.errors)


# ── backup tests ──────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__backup__creates_backup_file(migrate: Migrate):
    """backup() creates a timestamped copy in .bak/ directory."""
    # Setup: generate and apply migration so DB has state
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    assert migrate.check().state == State.CURRENT

    backup_path = migrate.backup()

    assert backup_path.exists()
    assert backup_path.parent == migrate.backup_dir
    # Filename pattern: YYYY-MM-DDThh-mm-ss.NNN.dbname
    assert backup_path.name.endswith(".db.sqlite")
    assert ".001." in backup_path.name
    # No colons (Windows-safe)
    assert ":" not in backup_path.name


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__backup__no_migrations_applied(migrate: Migrate):
    """backup() with no applied migrations uses 000 as migration number."""
    backup_path = migrate.backup()

    assert backup_path.exists()
    assert ".000." in backup_path.name


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__backup__is_valid_sqlite(migrate: Migrate):
    """The backup file is a usable SQLite database with the same data."""
    import apsw

    migrate.generate()
    migrate.apply(migrate.check().pending[0])

    backup_path = migrate.backup()

    # Open backup and verify it has the User table with _migrations
    conn = apsw.Connection(str(backup_path), flags=apsw.SQLITE_OPEN_READONLY)
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    assert "User" in tables
    assert "_migrations" in tables
    rows = list(conn.execute("SELECT id, filename FROM _migrations"))
    assert len(rows) == 1
    assert rows[0][0] == 1
    conn.close()


@pytest.mark.scenario("fresh_db_with_model")
def test_migrate__backup__multiple_sort_lexically(migrate: Migrate):
    """Multiple backups sort lexically by filename (timestamp prefix)."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])

    p1 = migrate.backup()
    p2 = migrate.backup()

    assert p1.name < p2.name
