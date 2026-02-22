"""CLI front end for TupleSaver migration system."""

from __future__ import annotations

import argparse
import importlib
import sys
import tomllib
from pathlib import Path

from .migrate import Migrate, State, format_status
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
    print(format_status(result))
    return 0 if result.state == State.CURRENT else 1


def cmd_generate(migrate: Migrate, args: argparse.Namespace) -> int:
    """Generate migration script from schema mismatch."""
    result = migrate.check()
    if result.state == State.CURRENT:
        print("Nothing to generate — schema already matches the DB")
        return 0
    if result.state != State.MISMATCH:
        print(
            f"Can't generate: DB is out of sync with migration scripts ({result.state.value}).\n"
            "Resolve or apply existing migrations first so that script generation has a clean baseline to compare against."
        )
        return 1
    path = migrate.generate()
    print(f"Generated {path}")
    return 0


def cmd_apply(migrate: Migrate, args: argparse.Namespace) -> int:
    """Apply pending migrations."""
    result = migrate.check()
    if result.state == State.CURRENT:
        print("Nothing to apply — already up to date")
        return 0
    if result.state != State.PENDING:
        print(f"Can't apply: migrations aren't in a clean state ({result.state.value}).\nResolve conflicts or sync the DB with existing scripts before applying.")
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


def _list_backups(migrate: Migrate) -> None:
    """Print available backups to stdout."""
    backups = migrate.list_backups()
    if not backups:
        print("No snapshots found")
    else:
        for i, b in enumerate(backups, 1):
            print(f"  {i}. {b.name}")
    if migrate.ref_path.exists():
        print(f"  ref: {migrate.ref_path.name}")


def cmd_restore(migrate: Migrate, args: argparse.Namespace) -> int:
    """Restore DB or scripts."""
    if args.scripts:
        migrate.restore_scripts()
        print("Restored scripts from ref DB.")
        return 0

    # Interactive mode: list backups and prompt for selection
    if args.interactive:
        backups = migrate.list_backups()
        if not backups:
            print("No snapshots available to restore.")
            return 1
        _list_backups(migrate)
        try:
            choice = input("Select backup number (or 'q' to cancel): ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nCancelled.")
            return 1
        if choice.lower() == "q":
            print("Cancelled.")
            return 1
        try:
            idx = int(choice)
        except ValueError:
            print(f"Invalid selection: {choice}")
            return 1
        if idx < 1 or idx > len(backups):
            print(f"Selection out of range: {idx}")
            return 1
        selected = backups[idx - 1]
        migrate.backup()
        migrate.restore_db(selected)
        print(f"Restored DB from backup: {selected.name}")
        return 0

    # Restore specific backup by name
    if args.backup:
        backup_path = migrate.backup_dir / args.backup
        if not backup_path.exists():
            print(f"Backup not found: {args.backup}")
            _list_backups(migrate)
            return 1
        migrate.backup()
        migrate.restore_db(backup_path)
        print(f"Restored DB from backup: {args.backup}")
        return 0

    # Default: DB restore from ref
    migrate.backup()
    migrate.restore_db()
    print("Restored DB from ref.")
    return 0


def _dev_step(migrate: Migrate, *, prev_state: State | None = None) -> int:
    """Recursive dev auto-resolve state machine."""
    result = migrate.check()
    if prev_state is not None:
        print("----------------------", flush=True)
    print(format_status(result), flush=True)

    if result.state == prev_state:
        print(f"Still {result.state.value} after fix attempt. Manual intervention needed.")
        return 1

    match result.state:
        case State.CURRENT:
            return 0
        case State.ERROR:
            return 1
        case State.CONFLICTED:
            migrate.restore_scripts()
            print("Restored scripts from ref DB.")
            return _dev_step(migrate, prev_state=result.state)
        case State.DIVERGED:
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
        case State.MISMATCH:
            path = migrate.generate()
            print(f"Generated {path}")
            return _dev_step(migrate, prev_state=result.state)


def cmd_dev(migrate: Migrate, args: argparse.Namespace) -> int:
    """Auto-resolve to CURRENT."""
    return _dev_step(migrate)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="tuplesaver-migrate",
        description="TupleSaver migration CLI",
        epilog=(
            "pyproject.toml config:\n"
            "  Options can also be set in [tool.tuplesaver]:\n"
            "\n"
            "    [tool.tuplesaver]\n"
            '    db_path = "path/to/db.sqlite"\n'
            '    models_module = "myapp.models"\n'
            "\n"
            "  CLI flags take precedence over pyproject.toml values."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db-path", dest="db_path", help="Path to working SQLite DB")
    parser.add_argument("--models-module", dest="models_module", help="Dotted module path containing TableRow models")

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show migration state")
    sub.add_parser("generate", help="Generate migration script from schema mismatch")

    apply_p = sub.add_parser("apply", help="Apply pending migrations")
    apply_p.add_argument("filename", nargs="?", default=None, help="Specific migration file to apply")

    backup_p = sub.add_parser("backup", help="Create backup")
    backup_p.add_argument("--ref", action="store_true", help="Also save ref snapshot")

    restore_p = sub.add_parser("restore", help="Restore DB or scripts from ref/backup")
    restore_mode = restore_p.add_mutually_exclusive_group()
    restore_mode.add_argument("--scripts", action="store_true", help="Restore scripts instead of DB")
    restore_mode.add_argument("-i", "--interactive", action="store_true", help="Interactively select a backup to restore")
    restore_mode.add_argument("-b", "--backup", default=None, help="Specific backup filename to restore")

    sub.add_parser("dev", help="Auto-resolve to CURRENT")

    return parser


COMMANDS = {
    "status": cmd_status,
    "generate": cmd_generate,
    "apply": cmd_apply,
    "backup": cmd_backup,
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
