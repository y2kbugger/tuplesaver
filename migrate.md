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

- `CREATE TABLE`, `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`
- For ambiguous renames: writes commented options for dev to choose
- Generated file includes comment header with consolidation hint:


### `apply(filename: str) -> None`
Run one migration script.
**Only allowed in `PENDING` state**

0. Make backup copy
1. Executes script SQL
2. Records in `_migrations` table with script content and timestamps

- Restore on failure.
- `_migrations` Table something like this:

```sql
CREATE TABLE _migrations (
    number INTEGER PRIMARY KEY,  -- 1, 2, 3...
    filename TEXT NOT NULL,      -- '001.create_users.sql'
    script TEXT NOT NULL,        -- full SQL content (newlines normalized)
    started_at TEXT NOT NULL,    -- ISO timestamp
    finished_at TEXT NOT NULL    -- ISO timestamp
);
```

```
### `restore() -> None`
```
Restore working DB from `.ref`.

1. Copies `.ref` over the working DB
2. Reopens engine connection

Requires `.ref` to exist. After restore, run `check()` and apply pending migrations as needed.

The reference DB (`mydb.sqlite.ref`) is a copy of the production database. It serves as the **restore base** — the state you can always restore to.


## Scenarios

### Iterating on uncommitted migration
```python
# Applied 003, then edited model again
result = migrate.check()
# model_diff shows new differences

# Option A: Generate another migration (accumulate)
migrate.generate()  # creates 004.add_avatar.sql
migrate.apply('004.add_avatar.sql')

# Option B: Consolidate (delete files, restore, regenerate)
# rm 003.*.sql 004.*.sql
migrate.restore()  # restore to .ref
migrate.generate()  # creates single 003 with all changes
migrate.apply('003...sql')

```

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
    case _:
        log.error(result.status())
        sys.exit(1)
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

## Milestones
- [X] Successfully run a check against No models.
- [X] Be able to generate a migration script from schema check.
- [ ] Enable the "iterate on uncommitted migration" workflow.
- [ ] Detect and triage conflicting migration scripts from other devs
    - e.g. handle diverged scripts really sanely.
    - one idea: restore ALWAYS restores from .ref and restores missing/mutated scripts from _migrations table.
- [ ] ability to ignore tables.
