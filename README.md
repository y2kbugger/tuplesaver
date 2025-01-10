# Notes

We want to be as minimal as possible leveraging the python standard library.

https://docs.python.org/3/library/sqlite3.html

https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders
I think we will want to use named placeholder when possible

    cur.executemany("INSERT INTO lang VALUES(:name, :year)", data)


Use parse DECL types to handle adapting and converting.
converters always are passed bytes so if we want to support str adapter/converters we need to decode them first.

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

# Backlog
- Optional columns types
- upsert
- pull in object from other table as field (1:Many, but on the single side)
- verify columns of created tables with option to delete table if mis-matched or fail instead, e.g. force=True
- Require and check that id is defined as an int and as the first column
- Overload on delete so you can just pass the whole row
- Unique constraints
- pull in list of objects from other table as field (1:Many, but on the many side)
- Add passthrough for commit? e.g. engine.commit???? or just leave them to use engine.connection.commit()?
- fetchone, fetchall, fetchmany on the query executer results
  - or queryone, queryall, querymany
- Allow str serde

## Engineering
- refactor out table creation in test fixture
- benchmark performance
- make update only update changed fields
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

## QUERYING
- limit row count
- order by
- Supra-binay logical operators e.g. (a or b or c)
- Subset of columns query
  - need way to specify which actual table the columns are from
- Literal str vs keyword str in query builder
- SQL injection possible, must parameterize queries correctly

- Insure that the query typehints completely and correctly describe the possible queries that can be made
  e.g.
  only things that should be tested for equality are tested for equality
  in summary the expression should have no type error IFF the expression will be valid SQL at runtime.
- string literal in predicate, vs SQL TEST, how to differentiate while preventing SQL injection?
- Maybe not allowing sql text literal afterall?
  - e.g. sql = ('select', 'count(*)', 'from', MyModel, 'where', MyModel.name, '=', 'y2k')


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
