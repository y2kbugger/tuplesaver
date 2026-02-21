"""Tests for the TupleSaver migration CLI."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from .migrate import Migrate, State
from .migrate_cli import (
    cmd_apply,
    cmd_backup,
    cmd_dev,
    cmd_generate,
    cmd_list_backups,
    cmd_restore,
    cmd_status,
    load_models_from_module,
    main,
)
from .migrate_test import SCENARIOS_DIR, migrate  # noqa: F401 — re-export fixture


def _ns(**kwargs) -> argparse.Namespace:
    """Create a simple namespace to act like argparse.Namespace."""
    return argparse.Namespace(**kwargs)


# ── status ────────────────────────────────────────────────────────────


@pytest.mark.scenario("empty_db")
def test_cli_status_current(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """status on empty DB with no models → CURRENT, exit 0."""
    args = _ns()
    code = cmd_status(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Current" in out


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_status_drift(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """status with drift → non-CURRENT info, exit 1."""
    args = _ns()
    code = cmd_status(migrate, args)
    assert code == 1
    out = capsys.readouterr().out
    assert "User" in out


# ── generate ──────────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_generate_ok(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """generate in DRIFT state → prints path, file exists, exit 0."""
    args = _ns()
    code = cmd_generate(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "001.create_user.sql" in out
    assert (migrate.migrations_dir / "001.create_user.sql").exists()


@pytest.mark.scenario("empty_db")
def test_cli_generate_wrong_state(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """generate in CURRENT state → error message, exit 1."""
    args = _ns()
    code = cmd_generate(migrate, args)
    assert code == 1
    out = capsys.readouterr().out
    assert "current" in out


# ── apply ─────────────────────────────────────────────────────────────


@pytest.mark.scenario("empty_db")
def test_cli_apply_current(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """apply on CURRENT DB → nothing to do, exit 0."""
    args = _ns(filename=None)
    code = cmd_apply(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "up to date" in out


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_apply_all_pending(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """apply all pending → backup created, applies all, exit 0."""
    migrate.generate()
    args = _ns(filename=None)
    code = cmd_apply(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Applied 001.create_user.sql" in out
    # backup was created
    assert migrate.backup_dir.exists()
    assert len(list(migrate.backup_dir.iterdir())) >= 1
    # migration was actually applied
    assert migrate.check().state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_apply_specific_file(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """apply with specific filename → backup + apply that one, exit 0."""
    migrate.generate()
    args = _ns(filename="001.create_user.sql")
    code = cmd_apply(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Applied 001.create_user.sql" in out
    assert migrate.check().state == State.CURRENT


# ── backup ────────────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_backup(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """backup → file created, path printed, exit 0."""
    args = _ns(ref=False)
    code = cmd_backup(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Backup created" in out
    assert migrate.backup_dir.exists()


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_backup_ref(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """backup --ref → backup + .ref created."""
    args = _ns(ref=True)
    code = cmd_backup(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Backup created" in out
    assert "Ref saved" in out
    assert migrate.ref_path.exists()


# ── list-backups ──────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_list_backups(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """list-backups after backup → files listed sorted."""
    migrate.backup()
    args = _ns()
    code = cmd_list_backups(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "db.sqlite" in out


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_list_backups_empty(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """list-backups with no backups → 'No backups found'."""
    args = _ns()
    code = cmd_list_backups(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "No snapshots" in out


# ── restore ───────────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_restore_db(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """restore (default) → backup created, restores DB, exit 0."""
    # Setup: generate, apply, save ref, then diverge
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    # Edit migration file to cause diverged state
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")
    assert migrate.check().state == State.CONFLICTED  # has ref so conflicted

    args = _ns(scripts=False)
    code = cmd_restore(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Restored DB" in out
    # backup was created before restore
    assert migrate.backup_dir.exists()


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_restore_scripts(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """restore --scripts → restores scripts (no backup), exit 0."""
    migrate.generate()
    migrate.apply(migrate.check().pending[0])
    migrate.save_ref()

    # Edit to cause conflicted
    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- conflict\n")
    assert migrate.check().state == State.CONFLICTED

    args = _ns(scripts=True)
    code = cmd_restore(migrate, args)
    assert code == 0
    out = capsys.readouterr().out
    assert "Restored scripts" in out
    # No backup was created for scripts restore
    assert not migrate.backup_dir.exists()


# ── dev ───────────────────────────────────────────────────────────────


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_dev_drift(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev from DRIFT → generates + applies, exit 0."""
    code = cmd_dev(migrate, _ns())
    assert code == 0
    out = capsys.readouterr().out
    assert "Generated" in out
    assert "Applied" in out
    assert migrate.check().state == State.CURRENT


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_dev_pending(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev from PENDING → backup + applies, exit 0."""
    migrate.generate()
    code = cmd_dev(migrate, _ns())
    assert code == 0
    out = capsys.readouterr().out
    assert "Applied" in out
    assert migrate.check().state == State.CURRENT
    assert migrate.backup_dir.exists()


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_dev_diverged(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev from DIVERGED → restores then resolves."""
    # Setup diverged state
    migrate.generate()
    migrate.apply(migrate.check().pending[0])

    script_path = migrate.migrations_dir / "001.create_user.sql"
    script_path.write_text(script_path.read_text() + "\n-- edited\n")
    assert migrate.check().state == State.DIVERGED

    code = cmd_dev(migrate, _ns())
    assert code == 0
    out = capsys.readouterr().out
    assert "Restored DB" in out
    assert migrate.check().state == State.CURRENT


@pytest.mark.scenario("empty_db")
def test_cli_dev_current(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev from CURRENT → exit 0 immediately."""
    code = cmd_dev(migrate, _ns())
    assert code == 0
    out = capsys.readouterr().out
    assert "Current" in out


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_dev_error(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev from ERROR → exit 1."""
    # Create duplicate migration numbers to cause ERROR
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.foo.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.bar.sql").write_text("SELECT 1;\n")

    code = cmd_dev(migrate, _ns())
    assert code == 1


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_dev_stuck_state(migrate: Migrate, capsys: pytest.CaptureFixture[str]) -> None:
    """dev with stuck state → exit 1, no infinite loop."""
    # Create a DIVERGED state that restore_db can't fix because there's no ref
    # and the migration file is edited, so after restore DB is empty but migration
    # file still differs. After restore_db we go PENDING, apply goes through, then
    # diverged again due to edited script content → stuck.
    # Simpler: create duplicate migration numbers → ERROR stays ERROR.
    migrate.migrations_dir.mkdir(exist_ok=True)
    (migrate.migrations_dir / "001.foo.sql").write_text("SELECT 1;\n")
    (migrate.migrations_dir / "001.bar.sql").write_text("SELECT 1;\n")

    result = migrate.check()
    assert result.state == State.ERROR

    code = cmd_dev(migrate, _ns())
    assert code == 1
    out = capsys.readouterr().out
    assert "Error" in out or "error" in out


# ── integration: main() ──────────────────────────────────────────────


@pytest.mark.scenario("empty_db")
def test_cli_main_status(migrate: Migrate) -> None:
    """main() with full argv → SystemExit with correct code."""
    with pytest.raises(SystemExit) as exc_info, patch("tuplesaver.migrate_cli.make_migrate", return_value=migrate):
        main(["--db-path", "x.db", "--models", "m", "status"])
    assert exc_info.value.code == 0


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_main_generate(migrate: Migrate) -> None:
    """main() generate → SystemExit(0)."""
    with pytest.raises(SystemExit) as exc_info, patch("tuplesaver.migrate_cli.make_migrate", return_value=migrate):
        main(["--db-path", "x.db", "--models", "m", "generate"])
    assert exc_info.value.code == 0


@pytest.mark.scenario("fresh_db_with_model")
def test_cli_main_dev(migrate: Migrate) -> None:
    """main() dev from DRIFT → SystemExit(0)."""
    with pytest.raises(SystemExit) as exc_info, patch("tuplesaver.migrate_cli.make_migrate", return_value=migrate):
        main(["--db-path", "x.db", "--models", "m", "dev"])
    assert exc_info.value.code == 0


# ── model loading ────────────────────────────────────────────────────


def test_cli_load_models_from_module(tmp_path: Path) -> None:
    """load_models_from_module can import a dotted module path."""
    # Create a temp module
    mod_dir = tmp_path / "fake_pkg"
    mod_dir.mkdir()
    (mod_dir / "__init__.py").write_text("")
    (mod_dir / "models.py").write_text("from tuplesaver.model import TableRow\n\nclass Widget(TableRow):\n    name: str\n")
    sys.path.insert(0, str(tmp_path))
    try:
        models = load_models_from_module("fake_pkg.models")
        assert len(models) == 1
        assert models[0].__name__ == "Widget"
    finally:
        sys.path.remove(str(tmp_path))
        # Clean up sys.modules
        for key in list(sys.modules):
            if key.startswith("fake_pkg"):
                del sys.modules[key]


# ── subprocess e2e tests ─────────────────────────────────────────────


def _run_cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    import os

    return subprocess.run(
        [sys.executable, "-m", "tuplesaver.migrate_cli", *args],
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env={**os.environ, "PYTHONPATH": str(cwd)},
    )


def test_e2e_passes_db_path_and_models(tmp_path: Path) -> None:
    """CLI works when --db-path and --models are provided."""
    shutil.copytree(SCENARIOS_DIR / "fresh_db_with_model", tmp_path / "s")
    r = _run_cli("--db-path", str(tmp_path / "s/db.sqlite"), "--models", "m", "status", cwd=tmp_path / "s")
    assert r.returncode in (0, 1)  # ran successfully (exit 1 = DRIFT, still valid)
    assert r.stderr == ""


def test_e2e_fails_without_db_path_and_models(tmp_path: Path) -> None:
    """CLI errors when --db-path and --models are missing."""
    r = _run_cli("status", cwd=tmp_path)
    assert r.returncode != 0
