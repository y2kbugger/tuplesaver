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

from .engine import Engine
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

    engine = Engine(str(dst / "db.sqlite"))
    return Migrate(engine, models=models)


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
    cursor = migrate.engine.connection.execute("SELECT number, filename, script FROM _migrations WHERE number = 1")
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
    import shutil

    shutil.copy2(migrate.db_path, migrate.ref_path)

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
    migrate.restore()

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
