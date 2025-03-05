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
- truely simple, not seemingly simple
- minimize library specific knowledge requirements, use standard types, type hints, and features
- minimize boilerplate
- between "more magic" and "more boilerplate", choose "more boilerplate"
- principle of least surprise
- all options are discoverable
  - e.g. through args of def: select(Model, where, limit, etc.)
- no dependencies
- User should never have to pass a Meta, when a Model would suffice

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


# Development
Use poetry to install the dependencies:

    $ poetry install --with dev

Install pre-commit hooks:

    $ pre-commit install --hook-type pre-commit --hook-type pre-push --hook-type post-commit

then activate and run the tests via vscode or the cli:

    $ poetry shell
    $ pytest

There is a test Task setup in vscode that, you can add a keybinding to run it, e.g.

    [Ctrl]+[Shift]+G

Interactively code with the python API in the `example.ipynb` notebook. This should include many examples.


## Benchmarking
to run perf regression tests exactly as they would be ran in pre-commit:

    pre-commit run pytest-check

to redraw the benchmark baseline, first stash to get a clean baseline of HEAD, then run the benchmark in save mode:

    git stash
    pre-commit run pytest-save-benchmarks

On linux you can set kernel parameters to isolate the cpu and get kernel threads off the cpu:

  isolcpus=6,7 nohz_full=6,7

For my dev system, cores 6 and 7 are the two hyperthreaded cores of my physical core 3, this is hardcoded into the testing harness to put benchmarks on this core.

Other things to disable/consider: backup software, web browsers, compositors, large monitors. Disconnect from a docking station and just use a power adapter. An extreme approach would be to boot directly to Virtual Console. Note, just switching to a virtual console does not eliminate the effect of being connected to a docking station.


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
- handle build in containers as special cases
  - list, dict, set, tuple
  - Test that bare container raises
  - will be stored as JSON, as long as adapt/convert has been specified
- pull in object from other table as field (1:Many, but on the single side)
  - Add foreign key constraints to the table creation
    - through the metadata system?? appending to meta during ensure_table_created?
  - Test using model with int as foreign key rather than model to prevent recursion
    - e.g. int instead of Node
    - need to have more sophisticated tablename e.g. split on _
  - Test the forgein key may only be a union with None i.e. Optional BUT NOT with int or something else

one to many
```python
class Team(NamedTuple):
    id: int | None
    name: str

class Person(NamedTuple):
    id: int | None
    name: str
    team: Team

# Use this model to query without recursion
class Person_Node(NamedTuple):
    id: int | None
    name: str
    team: id

# forward direction should be easy
class Person(NamedTuple):
    id: int | None
    name: str
    primary_team: Team
    secondary_team: Team
```
```sql
create table Team (
    id integer primary key,
    name text
);

create table Person (
    id integer primary key,
    name text,
    team_id integer,
    foreign key (team_id) references Team(id)
);
```

# Bugs
- but instead is `setting_name: <class 'str'>`")
  - > but instead is `setting_name: str`")
  - not sure how to make it look exactly like the type hint
- Investigate/ Test what Happens when specifying Model | int, should this raise??
- recursive get breaks depth first cursor proxy?? only finds one person.
  FIXED but needs regression test
  ```
  teama = Team(None, "Team A")
  teamb = Team(None, "Team B")
  teama = engine.insert(teama)
  teamb = engine.insert(teamb)

  pa1 = Person(None, "Alice", teama)
  pa2 = Person(None, "Bob", teama)
  pb1 = Person(None, "Charlie", teamb)
  pb2 = Person(None, "David", teamb)
  pa1 = engine.save(pa1)
  pa2 = engine.save(pa2)
  pb1 = engine.save(pb1)
  pb2 = engine.save(pb2)
  engine.connection.commit()
  print(teama)
  M, q = select(Person, where=eq(Person.team, teama.id))

  rows = engine.query(M, q).fetchall()
  print(rows)
  ```

# Tests
- Test what happens when you have two adapters, one more specific than the other
- test for fetchone returning none
- test that you cannot insert, update, or delete, a view model, only a table model
  - test that mutation queries don't even get set for the view meta


# Backlog
- I want to be able to persist built in container types without configuration
- I want to be able to persist an Enum without configuration
- Starting to think a unified save api is better than recursive insert
  - insert/update/delete can be non-recursive (and raise if relation is not persisted)
  - save can be a recursive upsert on id
    ```
      def _insert_shallow[R: Row](self, row: R) -> R:
          # insert only the current row, without recursion
          cur = self.connection.execute(get_meta(type(row)).insert, row)
          return row._replace(id=cur.lastrowid)

      def save[R: Row](self, row: R) -> R:
          # recursively insert or update records, based on the presence of an id
          # ids can be mixed and matched anywhere in the tree
          row = row._make(self.save(f) if is_registered_table_model(type(f)) else f for f in row)
          if row[0] is None:
              return self._insert_shallow(row)
          else:
              self.update_(row)
              return row
    ```
- I want to fall back to pickles for any type that is not configured, and just raise if pickle fails
- I want to be able to explain model function. This would explain what the type annotation is., what the sqllite column type is, And why?. Like it would tell you that an INT is a built-in Python SQLite type., but a model is another model, And a list of a built-in type is stored as json., And then what it would attempt to pickle if there would be a pickle if it's unknown..
This would help distinguish between a list of model and a list of something else. 
This is cool cuz it blends casa no sql with SQL. We could probably even make a refactoring tool to switch between the two.
- how handle unions of two valid types, e.g. int | str
- expanded api for update/delete
- extra-typical metadata
- unique contraints
  - requires extra-typical metadata
- upsert
  - engine.upsert(MyModel, row)
  - requires unique constraints
  - requires a way to specify which columns the unique constraint is on

## Engineering
- Extract TODO from README.md
- Move user notes all to example.ipynb
- Don't do this, violates don't wrap unesscarityly. if echo_sql: self.connection.set_trace_callback(print)
- Dedent create statement
- Benchmark and test connection creation and closing
- Test cleanup
  - Harmonize the def-scoped Model class names in the tests
  - use test specific Models in a small scope
  - refactor tests to be more granualar, e.g. test one table column at a time using smaller specific models, but also use parametrize to make test matrices
  - group tests, and promote _some_ model reuse if it makes sense
- maybe simplify "included adapters" to not be dict, but just a function with defs
  - maybe put in own file?
- use the assert_type from typing to check type hints throught all tests
- Store tablename in meta
- could we store map from _tuplegetter -> MetaField in Meta and get_meta_by_tuplegetter(tg) -> Meta, this would allow writing multi table quyeries using Model.name, Model2.value etc
- Use extra-typical metadata to store standard queries\
  - delete, update by id, insert
- Consider connection/transaction management
  - context manager?
  - how long is sqlite connection good for? application lifetime?
  - closing cursors? commit? difference between?
    - Context manager on cursor?
- Consider Concurrentcy in both read and write
  - what happens if two threads try to write to the same table at the same time?
  - How to actually test this?
  - Is there a connection setting (check same thread) that can be used to at least detect this?
  - https://www.sqlite.org/wal.html
- All exceptions raised to client api should have a custom exception type
- approx 20% perf boost for execute many on 20k rows
  - not worth complexity compared to other things to spend time on
- Minimize stack depth of engine.insert for deep recursive models e.g. depth=2000 BOM
- figure out why the benchmark warns about different system specs EVERY run.
- Can persister.py have to imports from query.py?
  - NO

## will not implement
- Add passthrough for commit? e.g. engine.commit
  - just let the user use the existing connection, engine.connection.commit()?
  - Violates, only do what can't be done with the sqlite standard library
- Allow str serde, i.e. in addtion to the bytes api
  - just explicitly encode/decode to bytes
  - Violates choose boilerplate over magic
- save (e.g. upsert on id) instead of insert/update
  - just use insert and update
  - Violates choose boilerplate over magic
- Query builder on engine
  - just use the query builder directly
  - Violates choose boilerplate over magic
  - better to use Model, sql, params as a stable and interoperable intermediate representation
- A TypedId as primary key of base models, see `typedid` tag for exploratory implementation
  - Reason for investigating
    - Reference a row by a single value, rather than Model+id
    - could simplify delete/update/get api
    - could make fetching a relationship row simpler
      Honors: minimize boilerplate
  - How
    - return TypedId replaced during insert
    - Add adapters for TypedId -> int (only need one, because we are losing the type info)
    - Add converters for int -> TypedId (need one for each model/table, as we need to add the type info)
      - One problem here was that you needed use "parse column names" to make the convert recognized.
        this means there are two different return types to the query, one when you do the converter name in column hint way:
        `select id as "id [TypedId_MyModel]"`
        and the normal way:
        `select id, name`
        This to me could causes surprises, and also makes types lie if you do a manual query and "forget" to add the type info.
        It also just makes all the queries noisy to look at.
        violates
          - minimize boilerplate
          - minimize library specific knowledge requirements
          - actually simple vs seemingly simple
          - principle of least surprise

  - It is also annoying that you have to repeat the Model name in the Model def:
    ```python
    class MyModel(NamedTuple):
        id: Id[MyModel]
        name: str
        date: dt.datetime
    ```
    Violates minimize boilerplate
  - The supposed benefit of simpler delete api actually hurts readability.
    ```python
    engine.delete(some_id)
    ```
    is less clear than
    ```python
    engine.delete(MyModel, some_id)
    ```
    and we already have an overload for deleting a row which fixes possibility of mixing id with wrong model
    ```python
    engine.delete(row)
    ```
    This violates choose boilerplate over magic
  - The typed ID is just another think to know, understand, and is a failure point.
    - violates actually simple vs seemingly simple
    - violates minimize library specific knowledge requirements


## Migration
- Auto add column(s) to table if they don't exist
  - or maybe just let this be the easy way to teach people to use explicit migrations
- Maybe store all versions of models in the db in additions to migrations script.
  - What about when "blowing away" the db?
  - what value does this add?
    - Could it help generate scripts?
    - could it just give insight to recent changes during dev?

## QUERYING
- can we use a Model reference eq to fk in where clause??
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
  class XXX(NamedTuple):
      id: int | None
      name: str
      place: str
      score: float

      _meta = Meta(
          unique_contraints=[('name','place')]
      )
  ```

  ## Upsert
```sql
create table XXX (
    id integer primary key,
    name text,
    place text,
    value int
);
-- the obligate unique constraint
CREATE UNIQUE INDEX IF NOT EXISTS XXX_name_place ON XXX (name, place);

-- make upsert on name,place combo
insert into XXX (name, place, value) values ('a', 'b', 777)
on conflict(name, place) do update set value = excluded.value;
-- or even just allow it to happen on any conflict (just set all non-id fields)
-- This gets a little tricky with existing data, but if we follow api of insert
--   and up, this makes sense, all fields are persisted (in either case insert or
--   update)
insert into XXX (name, place, value) values ('a', 'b', 888)
on conflict
    do update
        set name = excluded.name, place = excluded.place, value = excluded.value;
```

This is the user code
```python
class XXX(NamedTuple):
    id: int | None
    name: str
    place: str
    value: int

    _meta = Meta(
        unique_contraints=[('name','place')]
    )

engine.ensure_table_created(XXX)
engine.upsert(XXX(name='a', place='b', value=777))
engine.upsert(XXX(name='a', place='c', value=888))
```


## expanded api for update/delete

### Delete
To delete on id
```sql
delete from XXX where id = 42;
```
we already two ways
```python
engine.delete(row)
# and
engine.delete(XXX, 42)
```

To delete on multiple columns or for multiple rows, e.g..
```sql
delete from xxx where name = 'a' and place = 'b';
```
we could add
```python
engine.delete(XXX, where=and_(eq(XXX.name == 'a'), eq(XXX.place,'b')))
# or
engine.delete
```

### Update
To only some fields, on a single existing row, pull id from row:
```python
engine.update(row, set={MyModel.name: "Apple"})
```
```sql
update MyModel set name = 'Apple' where id = 42;
```

To update muliple rows, pass a Type[Row], it won't use an implicit id==row.id in where
```python
engine.update(MyModel, set=(MyModel.name, "Apple"), where=gt(MyModel.score, 42))
```
```sql
update MyModel set name = 'Apple' where score > 42;
```
### Backpop
Thinking to not do this, circular references might make it impossible anyway. just make it easy to fetch.
It also side steps the issue of double querying to fill in the forward reference/caching and wiring up the FK to the backprops. it actually forces everying to be a circular reference which isn't possible.
- backpop
  ```python
  class Team(NamedTuple):
      id: int | None
      name: str
      teams: list[Person] # Backpop

  class Person(NamedTuple):
      id: int | None
      name: str
      team: Team # Forward
  ```

  Need a way to differentiate between two different backpop of same type
  - backprop must include the full name of the forward reference as the prefix of it's name
  - if this is not specified or not unique, raise an `AmbiguousBackpopError`
  - not FK is allowed to be a subset of another FK on the same model. `AmbiguousForwardReferenceError`
  ```python
  # Ex 1. disambiguating backpop
  class Team(NamedTuple):
      id: int | None
      name: str
      primary_teams: list[Person]
      secondary_teams: list[Person]

  class Person(NamedTuple):
      id: int | None
      name: str
      primary_team: Team
      secondary_team: Team

  # Ex 2. disambiguating backpop
  class Employee(NamedTuple):
      id: int | None
      name: str
      manager_of: List[Project]
      lead_developer_of: List[Project]
      lead_maintainer_of: List[Project]

  class Project(NamedTuple):
      id: int | None
      name: str
      manager: Employee
      lead_developer: Employee
      lead_maintainer: Employee
      lead: Employee # not allowed, because it is an ambiguous subset of lead_developer
  ```

  - Backpop without a forward reference, should just be `AmbiguousBackpopError` because it is ambiguous if you cannot find a forward reference that is a complete prefixed subset of the backpop name.
    ```python
    class Team(NamedTuple):
        id: int | None
        name: str
        teams: list[Person]
    class Person(NamedTuple):
        id: int | None
        name: str
    ```
  - Many-to-Many shall just fall out of two 1:1, is not really a concept
  - Here is a test case with complex relations
  try and figure out if this is ambiguous or not
  ```python
  class Employee(NamedTuple):
      id: int | None
      name: str
      manager_of: List[Project]
      lead_developer_of: List[Project]
      contributor_roles: List[ProjectEmployee]

  class Project(NamedTuple):
      id: int | None
      name: str
      manager: Employee
      lead_developer: Employee
      contributors: List[ProjectEmployee]

  class ProjectEmployee(NamedTuple):
      project: Project
      employee: Employee
      role: str
  ```
