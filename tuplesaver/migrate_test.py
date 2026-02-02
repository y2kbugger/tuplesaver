"""
Migrate test scenarios.

Each scenario folder in migrate_scenarios/ contains:
- An initial .sqlite file (or missing for fresh DB)
- A .migrations/ folder with scripts (or missing)

Test Fixtures copy the scenario to a temp dir, then return a Migrate object.
Use @pytest.mark.scenario("name") to pick the scenario.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from .engine import Engine
from .migrate import Migrate, State

SCENARIOS_DIR = Path(__file__).parent / "migrate_scenarios"


@pytest.fixture
def migrate(request: pytest.FixtureRequest, tmp_path: Path) -> Migrate:
    """Copy scenario folder to tmp, return Migrate object with empty models."""
    marker = request.node.get_closest_marker("scenario")
    if not marker:
        raise ValueError("Test must use @pytest.mark.scenario('name')")

    name = marker.args[0]
    src = SCENARIOS_DIR / name
    if not src.exists():
        raise ValueError(f"Scenario not found: {src}")

    dst = tmp_path / name
    shutil.copytree(src, dst)

    engine = Engine(str(dst / "db.sqlite"))
    return Migrate(engine, models=[])


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
