# Notes
## Target Applications
- Single node for app + sqlite db
- Only your application access the db directly
  - ETL prefetch for reads of external data
  - Save, export for external writes
  - API access for reads from external services

This is a niche that viable many web applications, as well as for many enterprise applications.

## Why
If you meet the above constraints, there are tangible benefits.
- Simplified application infrastructure, no need for a separate db server
- If noone else accesses your db directly, you maintain the freedom to refactor the db schema
- Latency of persistance is negligible
  - Apps can leverage this and simplify by eliminating unpersisted state.
- Consistancy between devlocal, test, and production environments

## Library Goals
- Correct type hinting on both sides of persistance
- Improve refactorability
  - Eliminate stringly referenced columns
  - Migrations distilled to thier essential complexity

## Design Principles
- Truely simple, not seemingly simple
- no dependencies
- minimize boilerplate
- Between magic and more boilerplate, choose boilerplate
- Only do what can't be done with the sqlite standard library
  - Don't wrap if not REQUIRED to acomplish goals

## Outstanding Design Questions
- Tolerance to out-of-band schema changes
  1. Be fully compatible out-of-band changes to the db schema
    - adding indexes, constraints, etc.
    - interop with handwritten queries to avoid stringly typed column names.
  2. Narrow scope to allow stonger assumptions
    - Enhanced predictability of db responses, e.g. contraints allowed or now? triggers?
    - Simplified migrations, because we know nothing happened out-of-band
    - Only support querys that can be created from our query builder.

## Reference
https://docs.python.org/3/library/sqlite3.html
https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders
I think we will want to use named placeholder when possible

    cur.executemany("INSERT INTO lang VALUES(:name, :year)", data)

think about this for migrations
https://martinfowler.com/articles/evodb.html


## Types
https://docs.python.org/3/library/sqlite3.html#sqlite3-types

These are the only built in type mappings

| Python | SQLite  |
|--------|---------|
| None   | NULL    |
| int    | INTEGER |
| float  | REAL    |
| str    | TEXT    |
| bytes  | BLOB    |

For other types we use a thin wrapper on top of Adapt/Convert api
https://docs.python.org/3/library/sqlite3.html#sqlite3-adapter-converter-recipes

    def adapt_date_iso(val):
        """Adapt datetime.date to ISO 8601 date."""
        return val.isoformat()

    sqlite3.register_adapter(datetime.date, adapt_date_iso)


    def convert_date(val):
        """Convert ISO 8601 date to datetime.date object."""
        return datetime.date.fromisoformat(val.decode())

    sqlite3.register_converter("date", convert_date)

So our api which wraps the above would look like this:

    def adapt_datetime_iso(val: datetime.datetime) -> bytes:
        """Adapt datetime.datetime to ISO 8601 date and time."""
        return val.isoformat().encode()

    def convert_datetime_iso(data: bytes) -> datetime.datetime:
        """Convert ISO 8601 date and time to datetime.datetime object."""
        return dt.datetime.fromisoformat(data.decode())

    from micro_namedtuple_sqlite_persister import register_adapt_convert

    register_adapt_convert(datetime.datetime, adapt_datetime_iso, convert_datetime_iso)


| Python | SQLite  |
|--------|---------|
| None   | NULL    |
| int    | INTEGER |

# Development
Use poetry to install the dependencies:

    $ poetry install --with dev

Install pre-commit hooks:

    $ pre-commit install --hook-type pre-commit --hook-type pre-push

then activate and run the tests via vscode or the cli:

    $ poetry shell
    $ pytest

There is a test Task setup in vscode that, you can add a keybinding to run it, e.g.

    [Ctrl]+[Shift]+G

Interactively code with the python API in the `example.ipynb` notebook. This should include many examples.

## Linting
pre-commit hooks are installed and should be run before committing. To run them manually, use the following command:

    $ pre-commit run --all-files

to manually run ruff check, use the following command:

    ruff check

or

    ruff check --fix

and for formatting:

    ruff format

## Updating
### System Poetry itself

    poetry self update

### Poetry deps
Ensure you have poetry-plugin-up installed

    poetry self add poetry-plugin-up

Then run the following to update all dependencies in the pyproject.toml file

    poetry up --latest --preserve-wildcard

Then run the following to update the lock file

    poetry update

### Precommit
If you need to update the precommit hooks, run the following:

    pre-commit autoupdate

# WIP

# Bugs

# Tests
- overwrite flag on adapt/convert registrations

# Backlog
- Overload on delete so you can just pass the whole row
- fetchone, fetchall, fetchmany on the query executer results
  - or queryone, queryall, querymany
  - Could this be done with a cursor proxy and row factory?
- replace style api for update
  ```python
  engine.update(MyModel, set=(MyModel.name, "Apple"), where=(MyModel.id, 42))
  ```
  or
  ```python
  engine.update(row, set={MyModel.name: "Apple"})
  ```
  or maybe this is all to hyper-specialized.
  - replace insert and update with `save` method?
    - if row has an id, update, otherwise insert
  - maybe the querybuilder should create updates as well?
  - single get api on id also seems silly, just use query builder
    - maybe is useful when you have a relationship and want to get the related object
  - maybe engine should have methods: select, update, delete with query builder interfaces
    - or should user code grab handles to the model and sql and params and call execute directly?
- pull in object from other table as field (1:Many, but on the single side)
- pull in list of objects from other table as field (1:Many, but on the many side)

## Engineering
- refactor out table creation in test fixture
- refactor tests to use test specific Models in a small scope
- refactor tests to be more granualar, e.g. test one table column at a time using smaller specific models, but also use parametrize to make test matrices
- benchmark performance
- Use a protocol to fix some weird typing issues
  - row: NamedTuple vs row: ROW
  - self.connection.execute(query, (*row, row[0]))
  - etc.
  - maybe also address: def is_namedtuple_table_model(cls: object) -> bool:
- Consider connection/transaction management
  - context manager?
  - how long is sqlite connection good for? application lifetime?
- Consider Concurrentcy in both read and write
  - what happens if two threads try to write to the same table at the same time?
  - How to actually test this?
  - Is there a connection setting (check same thread) that can be used to at least detect this?
  - https://www.sqlite.org/wal.html
- All exceptions raised to client api should have a custom exception type
- approx 20% perf boost for execute many on 20k rows
  - not worth complexity compared to other things to spend time on
- Can persister.py have to imports from query.py?
  - NO
- is this suffient for a fully qualified type name?
    field_type_name = f"{AdaptConvertType.__module__}.{AdaptConvertType.__name__}"

## will not implement
- Add passthrough for commit? e.g. engine.commit
  - just let the user use the existing connection, engine.connection.commit()?
  - Violates, only do what can't be done with the sqlite standard library
- Allow str serde, i.e. in addtion to the bytes api
  - just explicitly encode/decode to bytes
  - Violates choose boilerplate over magic

## Migration
- Auto add column(s) to table if they don't exist
  - or maybe just let this be the easy way to teach people to use explicit migrations

## QUERYING
- limit row count
- order by
- Supra-binay logical operators e.g. (a or b or c)
- Subset of columns query
  - need way to specify which actual table the columns are from
- Insure that the query typehints have no type errors IF and only IF the query will be valid SQL at runtime
- SQL injection mitigation, correctly parameterize queries


### Purely functional API

```python
query = select(
    MyModel,
    where=and_(
        eq(MyModel.name, "Apple"),
        eq(MyModel.id, 42),
    ),
    group_by=MyModel.type,
    having=eq(func.count(MyModel.id), 1),
    order_by=desc(MyModel.updated_at),
    limit=10,
)
```
```sql
SELECT * FROM MyModel
WHERE name = ? AND id = ?
GROUP BY type
HAVING count(id) = ?
ORDER BY updated_at DESC
LIMIT 10
```
```python
query = select(
    (MyModel.name, MyModel.score)
)
```
```sql
SELECT name, score FROM MyModel
```

```python
query = select(
    (count(MyModel),)
)
```
```sql
SELECT count(*) FROM MyModel
```

### AST Based Query Builder
Just brainstorming here, the purely functional API is follows my interest in simplicity and no surprises.
That is a lot to sacrifice for gaining infix notation.

```python
@sql_query
def build_query():
    select(MyModel).columns(MyModel.id, MyModel.name, MyModel.type)
    where((MyModel.id == 42) and (MyModel.name != "Apple") or (MyModel.count > 100))
    group_by(MyModel.type)
    having(avg(MyModel.count) > 50)
    order_by([MyModel.created_at.desc(), MyModel.name.asc()])
    limit(200)
```
```sql
SELECT id, name, type FROM MyModel
WHERE id = 42 AND name != "Apple" OR count > 100
GROUP BY type
HAVING avg(count) > 50
```

## Extra-typical metadata
Some features requiring metadata than can't expressed in standard typehints
- unique constraints
- indexes
- upsert, true upsert relys on unique constraints in sqlite.
  - you must define which columns unique constraint you are basing the upsert on
- check constraints


Considerations for extra-typical metadata
```python
  class TUnique(NamedTuple):
      id: int | None
      name: Annotated[str, UNIQUE]
      age: int
  ```
  and here a simple method for unrapping metadata.
  ```python
  def unwrap_metadata(type_hint: Any) -> tuple[tuple[Any], Any]:
      """Determine if a given type hint is an Annotated type

      Annotated (e.g., Annotated[int, Unique])

      Returns
      - A list of metadata values
      - The underlying type if it is Annotated, otherwise the original type.
      """

      # Not any form of Annotated type
      if get_origin(type_hint) is not Annotated:
          return tuple(), type_hint

      metadata = get_args(type_hint)[1:]
      underlying_type = get_args(type_hint)[0]

      return metadata, underlying_type
  ```
  Options for defining UNIQUE
  ```python
  UNIQUE = 'UNIQUE'
  class UNIQUE: pass
  class UNIQUE:
    def __init__(self, name:str):
      '''maybe somthing multiple column described here'''
  ```
  There is also the option of using a inner Meta class
  ```python
  class TUnique(NamedTuple):
      id: int | None
      name: str
      age: int

      class MNSqlite:
          unique = ('name','age')
  ```
  Or as extra info in the create_table method
  ```python
  engine.ensure_table_created(TUnique, unique=('name','age'))
  ```
