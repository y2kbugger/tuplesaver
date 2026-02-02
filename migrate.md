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

### `Migrate(engine: Engine, models: list[TableRow])`

### `check() -> CheckResult`
Read-only checks. No side effects.

```python
class State(Enum):
    ERROR = "error"         # Blocking issues (duplicate numbers, gaps)
    DIVERGED = "diverged"   # Script on disk differs from what was applied
    PENDING = "pending"     # Scripts ready to apply
    DRIFT = "drift"         # DB doesn't match models, need to generate
    CURRENT = "current"     # Fully in sync

@dataclass
class CheckResult:
    pending: list[str]       # scripts on disk not yet applied
    applied: list[str]       # scripts recorded in _migrations table
    divergent: list[str]     # applied scripts where disk content != recorded content
    errors: list[str]        # blockers (duplicate numbers, gaps, etc.)
    model_diff: ModelDiff    # schema differences between models and actual DB

    @property
    def state(self) -> State:
        """Primary state for decision-making (first match wins)."""
        if self.errors:
            return State.ERROR
        if self.divergent:
            return State.DIVERGED
        if self.pending:
            return State.PENDING
        if not self.model_diff.is_empty:
            return State.DRIFT
        return State.CURRENT

    def status(self) -> str:
        """Human-readable summary, like `git status`."""
        ...
```

#### Error Checks:

    - Migration numbers must be sequential, gapless and unique.

#### Example Transitions

| State | Allowed Actions | Result |
|-------|-----------------|--------|
| `ERROR` | manual fix (renumber files, etc.) | → check() |
| `DIVERGED` | restore() | → check() → usually `PENDING` |
| `PENDING` | apply() | → check() → `DRIFT` or `CURRENT` |
| `DRIFT` | generate() | → check() → `PENDING` |
| `CURRENT` | — | stay `CURRENT` |


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

### `generate() -> Path | None`
Auto-generate a migration scripts/instructions based on check state.
**Only allowed in `DRIFT` state**

- `CREATE TABLE`, `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`
- For ambiguous renames: writes commented options for dev to choose
- Generated file includes comment header with consolidation hint:

```
### `restore() -> None`
Restore working DB from `.ref`.

1. Copies `.ref` over the working DB
2. Reopens engine connection

Requires `.ref` to exist. After restore, run `check()` and apply pending migrations as needed.

The reference DB (`mydb.sqlite.ref`) is a copy of the production database. It serves as the **restore base** — the state you can always restore to.


## Scenarios

### Fresh DB, models only (no scripts yet)
```python
migrate = Migrate(engine, models)
result = migrate.check()
# state=DRIFT, model_diff shows missing tables
migrate.generate()  # creates 001.create_users.sql
result = migrate.check()  # state=PENDING
migrate.apply(result.pending[0])
```

### Fresh DB with existing scripts
```python
migrate = Migrate(engine, models)
result = migrate.check()
# state=PENDING, pending=['001.create_users.sql']
for script in result.pending:
    migrate.apply(script)
```

### Steady state (nothing to do)
```python
result = migrate.check()
# state=CURRENT
```

### Model changed, need new migration
```python
result = migrate.check()
# state=DRIFT, model_diff shows the differences
migrate.generate()  # creates next script, e.g. 003.add_email.sql
result = migrate.check()  # state=PENDING
migrate.apply(result.pending[0])
```

### Iterating on uncommitted migration
```python
# Applied 003, then edited model again
result = migrate.check()
# model_diff shows new differences

# Option A: Generate another migration (accumulate)
migrate.generate()  # creates 004.add_avatar.sql
migrate.apply('004.add_avatar.sql')

# Option B: Consolidate (delete files, restore, regenerate)
# 1. rm 003.*.sql 004.*.sql
# 2. migrate.restore()  # restore to .ref
# 3. migrate.generate()  # creates single 003 with all changes
# 4. migrate.apply('003...sql')
# 4. migrate.apply('003...sql')
```

### Diverged script (with .ref)
```python
result = migrate.check()
# state=DIVERGED, divergent=['003.add_profile.sql']
print(result.status())  # "1 diverged: 003.add_profile.sql"

# Restore DB to .ref, then re-apply pending (including edited script)
migrate.restore()
result = migrate.check()  # state=PENDING
for script in result.pending:
    migrate.apply(script)
```

### Diverged script (no .ref — someone else's migration)
```python
result = migrate.check()
# state=DIVERGED, divergent=['002.add_email.sql']
# check() has added a warning comment to the file explaining options

# Option A: Restore file from git
# $ git checkout 002.add_email.sql
result = migrate.check()  # state=PENDING or CURRENT

# Option B: Delete file to restore from _migrations table
# $ rm 002.add_email.sql
# (system restores original from _migrations on next check)
result = migrate.check()
```

### Conflicting migration numbers
```python
result = migrate.check()
# errors=["Duplicate number 003"], can_apply=False
print(result.status())  # "Error: duplicate migration number 003"
# Developer must manually resolve by renumbering
```

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
`engine.ensure_table_created()` goes away. Models define structure; migrations create/alter schema. The schema-diff logic moves to `generate()`.

## Testing Strategy
Each case is pytest using Migrate api + fixtures as a kind of high level DSL.

There can be a handful of starting scenarios, each defined by a folder with:
- Initial `.sqlite` file (or missing)
- `.migrations/` folder with scripts (or missing)
- a models.py defining the models (may or may not match DB)

then each test can pick one of those scenarios and then:
- add/mutate/delete models (if needed)
- call `check()`, `apply()`, `generate()`, `restore()` as needed for the scenario
- check agains Expected `CheckResult` values along the way.

Tests copy the scenario to a temp dir before running to avoid mutation.

### test cases to not miss (there almost surely are more)
- All main scenarios above
- happy path and all possible migration id problems
- renamed and edited (diverged) last script
- renamed and not edited last script
- renamed and edited (diverged) past (e.g., 2nd of 3) script
- renamed and not edited past script
- diverged script with .ref available
- diverged script without .ref
- auto generated model changes: add table, drop table, add column, drop column, rename column


## Milestones
- [ ] Successfully run a check against No models.
- [ ] Be able to generate and apply clean straightforward new model table.
- [ ] Enable the "iterate on uncommitted migration" workflow.
- [ ] Detect and triage conflicting migration scripts from other devs
