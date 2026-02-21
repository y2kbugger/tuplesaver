# TupleSaver Migration System

Manage development and application of SQLite schema migrations.

- Ensure schema matches TupleSaver models
- Ensure migrations apply cleanly to production
- Triage migration conflicts between devs
- Auto-generate obvious migrations

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

## States

Priority: ERROR > CONFLICTED > DIVERGED > PENDING > DRIFT > CURRENT.

| State | Meaning | Fix |
|-------|---------|-----|
| `CURRENT` | Schema, scripts, and DB all agree | — |
| `DRIFT` | Models differ from DB, no script yet | `generate()` → PENDING |
| `PENDING` | Unapplied migration scripts exist | `apply()` → DRIFT or CURRENT |
| `DIVERGED` | Scripts differ from working DB (no ref) | `restore_db()` → PENDING |
| `CONFLICTED` | Scripts differ from ref DB | `restore_scripts()` → CURRENT or DIVERGED |
| `ERROR` | Bad migration files (gaps, dupes) | Manual fix |

## Python API

`Migrate(db_path: str | Path, models: list[type[TableRow]])`

| Method | State Gate | Effect |
|--------|-----------|--------|
| `check() → CheckResult` | any | Read-only state assessment |
| `generate() → Path` | DRIFT | Write migration SQL from model diff |
| `apply(filename)` | PENDING | Execute script, record in `_migrations` |
| `backup() → Path` | any | Timestamped copy via SQLite backup API |
| `save_ref()` | any | Snapshot working DB → `.ref` |
| `restore_db()` | DIVERGED+ | Restore working DB from `.ref` |
| `restore_scripts()` | CONFLICTED | Overwrite scripts from ref `_migrations` |
| `list_backups() → list[Path]` | any | Sorted backup files |

`restore_db()` is the standard dev workflow for iterating on scripts against a production-like baseline. The `.ref` should be refreshed regularly from production.

`restore_scripts()` overwrites local files with script content recorded in the ref DB's `_migrations` table, resolving conflicts where local edits diverge from production/shared reference.

## CLI

Entry point: `tuplesaver-migrate` (or `python -m tuplesaver.migrate_cli`)

### Global flags (required, with pyproject.toml fallback)

```
--db-path PATH      Path to working DB
--models-module MODULE   Dotted module path, e.g. myapp.models
```

Fallback: `[tool.tuplesaver]` in `./pyproject.toml`:
```toml
[tool.tuplesaver]
db_path = "data/mydb.sqlite"
models_module = "myapp.models"
```

### Commands

| Command | Flags | Description |
|---------|-------|-------------|
| `status` | | Print state and details |
| `generate` | | Generate migration from drift |
| `apply` | `[filename]` | Apply pending migrations (auto-backup) |
| `backup` | `--ref` | Create backup; `--ref` also saves ref |
| `list-backups` | | List available backups |
| `restore` | `--scripts` | Restore DB (default) or scripts from ref |
| `dev` | | Auto-resolve to CURRENT |

Exit codes: `0` = success/CURRENT, `1` = error or non-CURRENT.

No prompts. Always optimistic — takes action, prints what happened. Auto-backup before any DB mutation (`apply`, `restore`, `dev`).

### Dev State Machine

Recursive with loop breaker: if state unchanged after a fix attempt, exit 1.

```
         ┌──────────┐
         │  check() │◄──────────────────┐
         └────┬─────┘                   │
              │                    state changed?
   ┌──────┬──┴───┬─────────┬──────────┬──────────┐
   ▼      ▼      ▼         ▼          ▼          ▼
 ERROR  CONFL  DIVERG    PENDING     DRIFT     CURRENT
   │      │      │         │           │          │
 exit 1  restore restore  backup &  generate   exit 0
         scripts  db      apply all
           │      │         │          │
           └──────┴─────────┴──────────┘
                    recurse
```

## TODO

- [x] Check against no models
- [x] Generate migration scripts from schema check
- [x] Iterate-on-uncommitted-migration workflow
- [x] Pretty status output
- [x] Accept db_path instead of Engine; manage connections internally (WAL mode)
- [x] Detect/triage conflicts: DIVERGED (file ≠ working DB) vs CONFLICTED (file ≠ ref DB)
- [x] restore_scripts() from ref `_migrations` table
- [x] Error states (sequential, gapless, unique migration numbers)
- [x] Backup with timestamp + highest migration number
- [x] CLI with near-parity to Python API
- [ ] Generate ALTER instead of DROP/CREATE
- [ ] Generate SELECT-INTO for general alters
- [ ] Delete-and-regenerate (remove scripts not in ref with same prefix)
- [ ] Schema/application version pragma

2026-02-11
- Auto-resolve to CURRENT !! bad wording, need to jbe clearerer
- option summary for subcommands from --help
- restore specific db backup
- rename DRIFT to MISMATCH
