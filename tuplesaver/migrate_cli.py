"""CLI front end for TupleSaver migration system."""

from __future__ import annotations

import argparse
import importlib
import sys
import tomllib
from pathlib import Path

from .migrate import Migrate, State
from .model import TableRow


def load_models_from_module(module_path: str) -> list[type[TableRow]]:
    """Import a module by dotted path and collect all TableRow subclasses."""
    module = importlib.import_module(module_path)
    models: list[type[TableRow]] = []
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and issubclass(obj, TableRow) and obj is not TableRow:
            models.append(obj)
    return models


def load_config() -> dict[str, str]:
    """Load [tool.tuplesaver] from ./pyproject.toml if it exists."""
    pyproject = Path("pyproject.toml")
    if not pyproject.exists():
        return {}
    with pyproject.open("rb") as f:
        data = tomllib.load(f)
    return data.get("tool", {}).get("tuplesaver", {})


def resolve_args(args: argparse.Namespace) -> tuple[str, str]:
    """Resolve db_path and models from CLI args + pyproject.toml fallback.

    Returns (db_path, models) strings. Raises SystemExit on missing values.
    """
    config = load_config()

    db_path = args.db_path or config.get("db_path")
    models = args.models_module or config.get("models_module")

    if not db_path:
        print("Error: --db-path is required (or set db_path in [tool.tuplesaver])")
        sys.exit(1)
    if not models:
        print("Error: --models-module is required (or set models_module in [tool.tuplesaver])")
        sys.exit(1)

    assert isinstance(db_path, str) and isinstance(models, str)
    return db_path, models


def make_migrate(args: argparse.Namespace) -> Migrate:
    """Build a Migrate object from resolved CLI args."""
    db_path, models_spec = resolve_args(args)
    models = load_models_from_module(models_spec)
    return Migrate(db_path, models=models)


def cmd_status(migrate: Migrate, args: argparse.Namespace) -> int:
    """Show migration state."""
    result = migrate.check()
    print(result.status())
    return 0 if result.state == State.CURRENT else 1


def cmd_generate(migrate: Migrate, args: argparse.Namespace) -> int:
    """Generate migration script from schema drift."""
    result = migrate.check()
    if result.state != State.DRIFT:
        print(f"Cannot generate: state is {result.state.value}, expected drift")
        return 1
    path = migrate.generate()
    print(f"Generated {path}")
    return 0


def cmd_apply(migrate: Migrate, args: argparse.Namespace) -> int:
    """Apply pending migrations."""
    result = migrate.check()
    if result.state == State.CURRENT:
        print("Nothing to apply: already up to date")
        return 0
    if result.state != State.PENDING:
        print(f"Cannot apply: state is {result.state.value}, expected pending")
        return 1

    filename = args.filename
    if filename:
        if filename not in result.pending:
            print(f"Migration '{filename}' is not pending")
            return 1
        to_apply = [filename]
    else:
        to_apply = list(result.pending)

    for script in to_apply:
        migrate.backup()
        migrate.apply(script)
        print(f"Applied {script}")

    return 0


def cmd_backup(migrate: Migrate, args: argparse.Namespace) -> int:
    """Create backup, optionally save ref."""
    path = migrate.backup()
    print(f"Backup created: {path}")
    if args.ref:
        migrate.save_ref()
        print(f"Ref saved: {migrate.ref_path}")
    return 0


def cmd_list_backups(migrate: Migrate, args: argparse.Namespace) -> int:
    """List available backups."""
    backups = migrate.list_backups()
    if not backups:
        print("No snapshots found")
    else:
        for b in backups:
            print(b.name)

    # Include ref if it exists
    if migrate.ref_path.exists():
        print(f"ref: {migrate.ref_path.name}")

    return 0


def cmd_restore(migrate: Migrate, args: argparse.Namespace) -> int:
    """Restore DB or scripts."""
    if args.scripts:
        result = migrate.check()
        if result.state != State.CONFLICTED:
            print(f"Cannot restore scripts: state is {result.state.value}, expected conflicted")
            return 1
        migrate.restore_scripts()
        print("Restored scripts from ref DB.")
        return 0

    # DB restore
    migrate.backup()
    migrate.restore_db()
    print("Restored DB from ref.")
    return 0


def _dev_step(migrate: Migrate, *, prev_state: State | None = None) -> int:
    """Recursive dev auto-resolve state machine."""
    result = migrate.check()

    if result.state == prev_state:
        print(f"Still {result.state.value} after fix attempt. Manual intervention needed.")
        print(result.status())
        return 1

    match result.state:
        case State.CURRENT:
            print(result.status())
            return 0
        case State.ERROR:
            print(result.status())
            return 1
        case State.CONFLICTED:
            print(result.status())
            migrate.restore_scripts()
            print("Restored scripts from ref DB.")
            return _dev_step(migrate, prev_state=result.state)
        case State.DIVERGED:
            print(result.status())
            migrate.backup()
            migrate.restore_db()
            print("Restored DB from ref.")
            return _dev_step(migrate, prev_state=result.state)
        case State.PENDING:
            migrate.backup()
            for script in result.pending:
                migrate.apply(script)
                print(f"Applied {script}")
            return _dev_step(migrate, prev_state=result.state)
        case State.DRIFT:
            path = migrate.generate()
            print(f"Generated {path}")
            return _dev_step(migrate, prev_state=result.state)


def cmd_dev(migrate: Migrate, args: argparse.Namespace) -> int:
    """Auto-resolve to CURRENT."""
    return _dev_step(migrate)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(prog="tuplesaver-migrate", description="TupleSaver migration CLI")
    parser.add_argument("--db-path", dest="db_path", help="Path to working SQLite DB")
    parser.add_argument("--models-module", dest="models_module", help="Dotted module path containing TableRow models")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show migration state")
    sub.add_parser("generate", help="Generate migration script from schema drift")

    apply_p = sub.add_parser("apply", help="Apply pending migrations")
    apply_p.add_argument("filename", nargs="?", default=None, help="Specific migration file to apply")

    backup_p = sub.add_parser("backup", help="Create backup")
    backup_p.add_argument("--ref", action="store_true", help="Also save ref snapshot")

    sub.add_parser("list-backups", help="List available backups")

    restore_p = sub.add_parser("restore", help="Restore DB or scripts from ref")
    restore_p.add_argument("--scripts", action="store_true", help="Restore scripts instead of DB")

    sub.add_parser("dev", help="Auto-resolve to CURRENT")

    return parser


COMMANDS = {
    "status": cmd_status,
    "generate": cmd_generate,
    "apply": cmd_apply,
    "backup": cmd_backup,
    "list-backups": cmd_list_backups,
    "restore": cmd_restore,
    "dev": cmd_dev,
}


def main(argv: list[str] | None = None) -> None:
    """Entry point for the CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)
    migrate = make_migrate(args)
    handler = COMMANDS[args.command]
    code = handler(migrate, args)
    raise SystemExit(code)


if __name__ == "__main__":
    main()
