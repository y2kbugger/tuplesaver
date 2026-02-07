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
    pre_001.mydb.sqlite.bak
    pre_002.mydb.sqlite.bak
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

## What This Replaces
`engine.ensure_table_created()` goes away. Models define structure; migrations create/alter schema. The schema-diff logic moves to `generate()`. Tests still need a way to create initial tables so maybe ensure_table_created becomes a dev-only helper that just creates tables directly from models without migrations.

# TODO
## Milestones
- [X] Successfully run a check against No models.
- [X] Be able to generate a migration script from schema check.
- [X] Enable the "iterate on uncommitted migration" workflow.
- [ ] Make status prettier, put data on thier own lines and indent
- [ ] dont require engine, just db path, we will manage connections internally, setting walmode etc.
- [ ] Detect and triage conflicting migration scripts from other devs
    - e.g. handle diverged scripts really sanely.
    - one idea: restore ALWAYS restores from .ref and restores missing/mutated scripts from _migrations table.
    - DO we need another state, diverged from ref vs diverged from working db?
- [ ] the other side of restore: ability to restore a migration scripts from the _migrations table.
- [ ] generate alters instead of drop-create
- [ ] generate select-into general alters


## Questions
- should apply be in charge of backups? or should that be caller's responsibility?
    - can all the scripts be applied in a single transaction? if so, then backup is caller's responsibility.

## Testing Scenarios

### Migration script has diverged from mydb.sqlite but not the mydb.sqlite.ref
```python
result = migrate.check()
# state=DIVERGED, divergent=['003.add_profile.sql']

# Restore DB to .ref, then re-apply pending (including edited script)
migrate.restore()
result = migrate.check()
# state=PENDING

for script in result.pending:
    migrate.apply(script)
```

### Migration script has diverged from mydb.sqlite.ref
```python
result = migrate.check()
# state=DIVERGED, divergent=['002.add_email.sql']

result = migrate.restore()
# still state=DIVERGED, divergent=['002.add_email.sql']

# Either restore file to undiverged or delete file to restore it from the _migrations table
# $ rm 002.add_email.sql
result = migrate.check()
# state=DIVERGED, divergent=['002.add_email.sql']
result = migrate.restore() # but now restored from _migrations table
result = migrate.check()
# state=CURRENT
```

### Conflicting migration numbers
```python
result = migrate.check()
# errors=["Duplicate number 003"], can_apply=False
print(result.status())  # "Error: duplicate migration number 003"
# Developer must manually resolve by renumbering
```


## testing edges to not miss, but will do later:
- All main scenarios from migrate.md
- Happy path and all possible migration id problems (gaps, duplicates)
- Renamed and edited (diverged) last script
- Renamed and not edited last script
- Renamed and edited (diverged) past (e.g., 2nd of 3) script
- Diverged script with .ref available
- Diverged script without .ref
- Auto generated model changes: add table, add column, drop column, rename column
- Migration numbers must be sequential, gapless and unique
- Pending migrations are always applied in order
- test that all status make sense
- backup api for pre-migrate hooks
- filename change IS a divergence
