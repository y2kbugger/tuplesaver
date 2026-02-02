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
