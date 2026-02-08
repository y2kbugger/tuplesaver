# TupleSaver Migration System Design for SQLite

Manange devlopment and application of sqlite schema migration.

1. Ensure schema matches tuplesaver models
2. Ensure that migrations apply cleanly to prod
3. Help triage migration conflicts
4. Automatically generate obvious migrations


## File Layout

```
mydb.sqlite              # working DB (refresh from prodction)
mydb.sqlite.ref          # reference DB (refresh from production, immutable)
mydb.sqlite.migrations/
    001.create_users.sql
    002.add_email_column.sql

mydb.sqlite.bak/
    2026-11-19T11:11:11.000.mydb.sqlite
    2026-11-20T11:11:11.001.mydb.sqlite
    2026-11-21T11:11:11.002.mydb.sqlite
```
## API
`Migrate(engine: Engine, models: list[TableRow])`

### `check() -> CheckResult`
Read-only state check. No side effects.
State transitions are a combo of api calls and migration script changes.

### Example Transitions
| State | Allowed Actions | Result |
|-------|-----------------|--------|
| `ERROR` | manual fix (renumber files, etc.) | → check() |
| `DIVERGED` | restore() | → check() → `PENDING` |
| `DRIFT` | generate() | → check() → `PENDING` |
| `PENDING` | apply() | → check() → `DRIFT` or `CURRENT` |
| `CURRENT` | — | stay `CURRENT` |

### `generate() -> Path | None`
Auto-generate a migration scripts/instructions based on check state.

**Only allowed in `DRIFT` state**

- DDL to align working DB schema to models
    - First take is a naive drop if exists stub and recreate. later we will handle alters and recreate-select-into patterns
- For ambiguous renames: writes commented options for dev to choose
- Generated file includes comment header with consolidation hint:


### `apply(filename: str) -> None`
Run one migration script.

**Only allowed in `PENDING` state**

1. Executes script SQL
2. Records in `_migrations` table with script content and timestamps

### `restore() -> None`
Restore working DB from `.ref`, this allows iterating on your scripts, continually re-applying them to a production-like baseline.

The reference DB (`mydb.sqlite.ref`) should be refreshed regularly from production somehow.

This is meant to be a standard action to resolve `DIVERGED` states while developing migration scripts, e.g. you thought the model needed two columns, but then you noticed another.

---

## Integration Examples

### Dev server startup
```python
migrate = Migrate(engine, models)
result = migrate.check()

match result.state:
    case State.CURRENT:
        pass  # ready to go
    case State.PENDING:
        for script in result.pending:
            migrate.apply(script)
        sys.exit(0)  # reload to recheck
    case State.DRIFT:
        print(result.status())
        print("Generating migration script...")
        migration_path = migrate.generate()
        print(f"Generated migration script at {migration_path}")
    case State.DIVERGED:
        print(result.status())
        print("Restoring to an un-diverged state...")
        migrate.restore()
        print("Restored. Rechecking...")
        sys.exit(1)  # reload to recheck
    case State.ERROR:
        print(result.status())
        sys.exit(1)  # reload to recheck
    case _:
        raise RuntimeError(f"Unexpected migration state: {result.state}")
```

### Production CI
```python
migrate = Migrate(engine, models)
result = migrate.check()

match result.state:
    case State.CURRENT:
        print("Nothing to migrate")
    case State.PENDING:
        push_to_s3(db_path)  # offsite backup
        for script in result.pending:
            migrate.apply(script)
    case State.DRIFT:
        print("Model drift — generate migration first")
        print(result.status())
        exit(1)
    case _:
        print(result.status())
        exit(1)
```

# TODO
- [X] Successfully run a check against No models.
- [X] Be able to generate a migration script from schema check.
- [X] Enable the "iterate on uncommitted migration" workflow.
- [X] Make status prettier, put data on thier own lines and indent
- [X] dont require engine, just db path, we will manage connections internally, setting walmode etc.
- [X] Detect and triage conflicting migration scripts from other devs
- [ ] the other side of restore: ability to restore a migration scripts from the _migrations table.
    - e.g. handle diverged scripts really sanely.
    - one idea: restore ALWAYS restores from .ref and restores missing/mutated scripts from _migrations table.
    - DO we need another state, diverged from ref vs diverged from working db?
- [ ] Add a cli api will near parity with the python api, so that it can be used in bash scripts and make it easier to run from vscode tasks. It should also include the example integration scenarios such as "dev auto migrate" and "production ci migrate". Restore should be interactive with listing about "diverged" and contents of each with option to either restore db from .ref or restore scripts from ref._migrations table.
- [ ] backup method to optionally backup when applying in prod. name based on timestamp and maybe highest migration #, e.g. `mydb.sqlite.bak/2026-11-31T11:11:11.002.mydb.sqlite.bak`
- review that all status make sense and are nice
- [ ] generate alters instead of drop-create
- [ ] generate select-into general alters


## testing edges to not miss, but will do later:
- Migration numbers must be sequential, gapless and unique
- Pending migrations are always applied in order
- filename change IS a divergence
- test that the "dev migrate" workflow never trys to restore more than once (e.g. looping)
