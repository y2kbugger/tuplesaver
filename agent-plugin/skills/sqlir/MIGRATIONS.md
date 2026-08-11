<!-- AUTO-GENERATED — DO NOT EDIT. Regenerate with: python scripts/sync_plugin.py Source: MIGRATIONS.md -->

# Migrations

Manage development and application of SQLite schema migrations.

- Ensure schema matches `TableRow` models
- Ensure migrations apply cleanly to production
- Triage migration conflicts between devs
- Auto-generate obvious migrations

Migrations run **out of band**, from the CLI — applications should never
generate or apply migrations at startup.

## File Layout

```
mydb.sqlite                    # working DB (refresh from production)
mydb.sqlite.ref                # reference DB (production snapshot, immutable)
mydb.sqlite.migrations/
    001.create_users.sql
    002.add_email_column.sql
mydb.sqlite.bak/
    2026-11-19T14-30-05.123456.000.mydb.sqlite
    2026-11-20T09-15-42.456789.001.mydb.sqlite
```

Backup filenames are `<utc-timestamp>.<migration_level>.<db_name>`. The
`<migration_level>` is the highest migration number **already applied at the
moment the snapshot was taken** — the schema state captured *inside* the
backup. Since a backup is taken right before each `apply()`, this is the level
*before* the next migration runs (`.000` = empty DB, `.001` = migration 001
applied, …). Restoring a `.NNN` backup rewinds the DB to that level.

## States

Priority: ERROR > CONFLICTED > DIVERGED > PENDING > MISMATCH > CURRENT.

| State | Meaning | Fix |
|-------|---------|-----|
| `CURRENT` | Schema, scripts, and DB all agree | — |
| `MISMATCH` | Models differ from DB, no script yet | `generate()` → PENDING |
| `PENDING` | Unapplied migration scripts exist | `apply()` → MISMATCH or CURRENT |
| `DIVERGED` | Scripts differ from working DB (no ref) | `restore_db()` → PENDING |
| `CONFLICTED` | Scripts differ from ref DB | `restore_scripts()` → CURRENT or DIVERGED |
| `ERROR` | Bad migration files (gaps, dupes) | Manual fix |

### State Transitions Diagram

```
 start ────┐
           v
         ┌──────────┐
         │  check() │<──────────────────────────────yes─────┐
         └───┬──────┘                                       │
             │                                            state changed?─────no─────>done
   ┌──────┬──┴───┬─────────┬───────────┬──────────┐             ^
   v      v      v         v           v          v             │
 ERROR  CONFL  DIVERG    PENDING    MISMATCH   CURRENT          │
   │      │      │         │           │          │             │
   v      v      v         v           v          v             │
 exit 1  restore restore  backup &  generate   exit 0           │
         scripts  db      apply all                             │
           │      │         │                                   │
           └──────┴─────────┴──────────recurse──────────────────┘
```

Migrations have both a Python API and a CLI, which share the same underlying
logic and state management. The CLI is just a thin wrapper around the API, so
any action you can do in the CLI can also be done programmatically, and vice
versa.

## CLI

Entry point: `sqlir-migrate` (or `python -m sqlir.migrate_cli`)

> The source of truth for CLI usage is the CLI itself: run
> `sqlir-migrate --help` and `sqlir-migrate <COMMAND> --help` for the
> authoritative list of subcommands and flags. It is not duplicated here.

Global flags (required, with `pyproject.toml` fallback):

```
--db-path PATH           Path to working DB
--models-module MODULE   Dotted module path, e.g. myapp.models
```

Set permanently in `./pyproject.toml`:

```toml
[tool.sqlir]
db_path = "data/mydb.sqlite"
models_module = "myapp.models"
```
