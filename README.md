# Notes
## Target Applications Constraints
- Python app + sqlite db served from a single server.
- The app will ONLY access the db.
  - Instead, use ETL to load data in the db from external sources.
- ONLY the App will access the db.
  - Have a stable API for external access to underlying data.

This is a viable niche for many web apps, include a large percentage of those with enterprises.

## Why
If you can meet the above constraints, there are tangible benefits.
- Simplified application infrastructure, no need for a separate db server
- If noone else accesses your db directly, you maintain the freedom to refactor the db schema
- Latency of persistance becomes negligible
  - Apps can leverage this and simplify by eliminating unpersisted state within the app and become stateless.
- True consistancy between devlocal, test, and production environments.
  - Migrations become easier to automate and test.

## Library Goals
- Correct static type hinting on both sides of persistance
- Improve refactorability
  - Eliminate stringly referenced columns
  - Migrations distilled to thier essential complexity

## Design Principles
- truely simple, not seemingly simple
- minimize library specific knowledge requirements, use standard types, type hints, and features
  - Never wrap native functionality
- minimize boilerplate
- between "more magic" and "more boilerplate", choose "more boilerplate"
- principle of least surprise
- library specific knowlege should be self revealing
  - e.g. through attributes, type hints, or parameters
- no dependencies

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

Really need to read and understand this new annotation sematics coming in 3.14, as well as difference between inspect.get_nnotations and typing.get_type_hints
https://docs.python.org/3/howto/annotations.html#annotations-howto
https://github.com/python/cpython/issues/102405
https://peps.python.org/pep-0649/


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

We also include by default the following mappings

| Python   | SQLite            | Adapt/Convert      |
|----------|-------------------|--------------------|
| bool     | builtins.bool     | 1 -> x01, 0 -> x00 |
| list     | builtins.list     | json dumps/loads   |
| dict     | builtins.dict     | json dumps/loads   |
| date     | datetime.date     | .isoformat()       |
| datetime | datetime.datetime | .isoformat()       |

Any other types will attempt to be pickled.


If you want to customize how you serialize, we use a thin wrapper on top of Adapt/Convert api.
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

### A note on JSON columns
Currently we do not apply adapters/converters to json dumps, because without specifying a schema there is no way to reliably recover type info.

Even something as simple as date, would be ambiguous, because it could be a date or str going in

    {"date": "2021-01-01"}

If we enable dt.datetime serialization, then the above could have been

    dict(date=dt.datetime(2021, 1, 1))

or

    dict(date="2021-01-01")

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

to redraw the benchmark baseline, first stash to get a clean baseline of HEAD,
then simpulate what precommit would do (to warm up cpu, cause noise on sytem, etc),
then run the benchmark in save mode:

    git stash
    pre-commit run --hook-stage pre-commit; pre-commit run pytest-save-benchmarks --hook-stage post-commit

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
- pull in object from other table as field (1:Many, but on the single side)
  - Add foreign key constraints to the table creation
    - through the metadata system?? appending to meta during ensure_table_created?
  - test field base FK disambiguation
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
    I think there needs to be more testing for nested updates. I think this stops short when updating a row. like what if row has id but its related model is uninserted. I think "update" needs a recursive component, but "insert" does not. but maybe it is right.
- consider add column constraints as annotations, like the select
  https://sqlite.org/syntax/column-def.html
  https://sqlite.org/syntax/column-constraint.html
- I want to fall back to pickles for any type that is not configured, and just raise if pickle fails
- I want to be able to explain model function. This would explain what the type annotation is., what the sqllite column type is, And why?. Like it would tell you that an INT is a built-in Python SQLite type., but a model is another model, And a list of a built-in type is stored as json., And then what it would attempt to pickle if there would be a pickle if it's unknown..
This would help distinguish between a list of model and a list of something else. 
This is cool cuz it blends casa no sql with SQL. We could probably even make a refactoring tool to switch between the two.
- Maybe update should return the row back to match insert.
  - but no reason so misleading?
- how handle unions of two valid types, e.g. int | str
  - Adapting would work fine, but conversion could be ambiguous
  - I think we should just raise on this
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
  - move "function" end to end tests together, ensure we have good unit tests in the module specific test files.
  - Start tracking coverage?
- maybe simplify "included adapters" to not be dict, but just a function with defs
  - maybe put in own file?
  - maybe don't make this optional, just have a default set of adapters
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
  - connection pooling?
- Consider Concurrentcy in both read and write
  - what happens if two threads try to write to the same table at the same time?
  - How to actually test this?
  - Is there a connection setting (check same thread) that can be used to at least detect this?
  - https://www.sqlite.org/wal.html
- Benchmark and consider joined loads
- All exceptions raised to client api should have a custom exception type
  - especially in query builder, those give some pretty cryptic errors
- approx 20% perf boost for execute many on 20k rows
  - not worth complexity compared to other things to spend time on
- Minimize stack depth of engine.insert for deep recursive models e.g. depth=2000 BOM
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

# QUERYING 2.0 - `def` based Query Builder using f-strings
- Subset of columns query
  - need way to specify which actual table the columns are from
    - just always follow an implicit join path
- SQL injection mitigation, correctly parameterize queries
  using def
https://sqlite.org/syntax/result-column.html

This simplifies a lot. Because we only have to worry about SQLite, We can just write what we mean in SQLite proper, we don't need to abstract all possible SQL syntax to python.

This solves the refactoring (no hardcoded column names) usecase.
It also ensures type safety on both sides of the query, because all `SELECT` queries are defined in the Table or View models.

`select` still just returns a tupel of (Model, sql)

Implementation note:
- To make annotations work, we force usage of `from __future__ import annotations`
- we might have to wrap annotations e.g.

      team_name: Annotated[str, Col(f"{Athlete.team.name}")]

  but that might also allow us to drop the f-string completely

      team_name: Annotated[str, Col(Athlete.team.name)]

  Also this same scheme could be used for CREATE statement column constraints

      name: Annotated[str, Constraint("UNIQUE")]

## Table Model
```python
class Person(NamedTuple):
    id: int
    name: str
    score: int

engine.query(*select(Person))
```

## Subset of Columns (View Model)
```python
class Person_NameOnly(NamedTuple):
    name: str

engine.query(*select(Person_NameOnly))
```
```sql
SELECT name FROM Person
```

## WHERE clause

```python
@select(Person)
def high_scores():
    f"WHERE {Person.score} > 100"

engine.query(*high_scores)
```
```sql
SELECT id, name, score FROM Person
WHERE score > 100
```

## Parameters
```python
@select(Person)
def find_person_by_score(minscore: int):
    f"WHERE {Person.score} >= {minscore}"

engine.query(*find_person_by_score, params=(100,))
```
```sql
SELECT id, name, score FROM Person
WHERE  OR score > :minscore
```

## GROUP BY / Aggregation
```python
Aggregations queries are more tightly coupled to the View Model because the model must define the aggregations, but the query defines the grouping. Therefore you might want to define the query f-string in the model def. But this is
just a stylistic choice
```
```python
class Person_TotalScore(NamedTuple):
    name: str
    total_score: Annotated[int, f"sum({Person.score})"]

@select(Person_TotalScore)
def apple_total()
    f"""
    WHERE {Person.name} = 'Apple'
    GROUP BY {Person.name}
    """
engine.query(*apple_total)

# or in the nested style, to communicate coupling
class Person_TotalScore(NamedTuple):
    name: str
    total_score: Annotated[int, f"sum({Person.score})"]

  @select(Person_TotalScore)
  def apple_total():
      f"""
      WHERE {Person.name} = 'Apple'
      GROUP BY {Person.name}
      """
engine.query(Person_TotalScore.apple_total)
```
Both of these would generate the same SQL
```sql
SELECT name, sum(score) as total_score
FROM Person
WHERE name = 'Apple'
```

## JSON extracted field in WHERE
Get out of your way and let you use SQLite JSON functions
```python
class Character(NamedTuple):
    id: int
    name: str
    stats: dict[str, int | str | dt.datetime]

example_char = Character(1, 'Apple', {'spell': 'Fireball', 'level': 3, 'date': dt.datetime.now()})

@select(Character)
def get_fireball_characters():
    f"WHERE {Character.stats} -> '$.spell' = 'Fireball'"

engine.query(*get_fireball_characters)
```
```sql
SELECT id, name, stats
FROM Character
WHERE stats -> '$.spell' = 'Fireball'
```

## JSON extracted field in SELECT
```python
class Character_WithSpell(NamedTuple):
    id: int
    name: str
    spell: Annotated[str, f"{Character.stats} -> '$.spell'"]

engine.query(*select(Character_WithSpell))
```
```sql
SELECT id, name, stats -> '$.spell' as spell
FROM Character
```
## View Model Referring to another View Model
This seems in theory possible, but might have impossible edge cases
```python
class Character_WithPowerColumn(NamedTuple):
    id: int
    name: str
    power: Annotated[str, f"{Character.stats} -> '$.power'"]

class Character_TotalPower(NamedTuple):
    id: int
    name: str
    total_power: Annotated[str, f"sum{Character_WithPowerColumn.power}"]

  @select(Character_TotalPower)
  def total_power():
      f"GROUP BY {Character.name}"

engine.query(*Character_TotalPower.total_power)
```
```sql
SELECT id, name, sum(stats -> '$.power') as total_power
FROM Character
GROUP BY name
```

## JOINs for predicate
```python
class Team(NamedTuple):
    id: int
    name: str

class Athlete(NamedTuple):
    id: int
    name: str
    team: Team

@select(Athlete)
def athletes_on_red_team():
    f"WHERE {Athlete.team.name} = 'Red'"

engine.query(*athletes_on_red_team)
```
```sql
SELECT id, name, team
FROM Athlete
JOIN Team ON Athlete.team = Team.id
WHERE team.name = 'Red'
```

## JOINs for SELECT
```python
class Athlete_WithTeamName(NamedTuple):
    name: str
    team_name: Annotated[str, f"{Athlete.team.name}"]

engine.query(*select(Athlete_WithTeamName))
```
```sql
SELECT Athlete.name, Athlete.team.name as team_name
FROM Athlete
JOIN Team team ON Athlete.team = team.id
```

## Alternate lambda syntax
```python
M, q = select(Athlete)(lambda: f"WHERE name LIKE '%e%'")
```



# Extra-typical metadata
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

# Rebuttles to criticisms
This is all just thinking out loud. Will reread and condense.

1.  **Implicit vs. Explicit Joins** – Your `JOIN` logic relies on Python’s type annotations. How will you handle ambiguous foreign keys (e.g., an `Athlete` belonging to multiple teams)?
  - you can see in my models that I reference the field and not the table. e.g. `Athelete.team.name`
2.  **Aliasing Strategy** – How do you plan to handle column name collisions when selecting from multiple tables?
  - I can make the generator include the table name in the column name
3.  **Dynamic Queries** – You’re using f-strings for query generation, but what if a `WHERE` clause needs conditional logic (e.g., filtering only when a parameter is present)?
  - Good point. because of the inpection/reflection I will use, this will never work as the body is never executed. i actually don't feel like dynamic sql is a virtue anyhow.
4.  **Performance Considerations** – SQLite is great, but large queries could get expensive. Are you considering indexing hints or query optimizations?
  - I can add indexes via migration script mechanisms, not thought too much about it though. I as I stated. I want the raw and FULL power of SQLite, so anything that possible is within my scope. I see this filed under "should not wrap native functionality"
5.  **Schema Evolution** – How do you handle schema changes? What happens when a column is renamed or a new field is added?  1. **Schema Evolution & Refactorability** – If you have full control over schema changes, how do you handle old queries still expecting the previous schema?  10.  **API Access for Reads from External Services** – How do you ensure data freshness for API reads? Is there caching or invalidation logic when external data updates?” 3.  **Consistency Across Environments** – SQLite behaves slightly differently across OSes and versions. How do you ensure schema and behavior consistency across dev, test, and production?  10.  **Migrations & Backwards Compatibility** – If a query relies on an old table structure, how do you prevent breaking changes in production?”
  - Having full control because I am the only and only app touching the DB allows me to handle this. I will use replication and rollbacks to test migration script against the real and full DB locally and in CI/CD.
6.  **Parameterized Queries** – Your `find_person_by_score` example shows a parameter, but the SQL output has an `OR` typo (`score > :minscore`). How are you validating generated SQL?
  - This are imagined output, so that was my typo. In real life typos in f-string will be caught by sqlite itself, again, don't wrap native functionality.
7.  **Security Concerns** – f-strings make it easy to inject SQL directly. How do you ensure queries remain safe from accidental injection vulnerabilities?
  - You are missing how this works. the parameters are generated as SQL params, by the generator.
8.  **JSON Queries** – SQLite’s JSON functions are powerful, but they return `TEXT` unless explicitly cast. Are you handling type enforcement properly when querying JSON fields?
  - This will be handled by the query, maybe runtime checking is needed
9.  **View Models** – Your View Models are great for encapsulation, but what happens when you need a calculated field in multiple queries? Do you duplicate logic, or have a strategy for reusable fragments?  6.  **Discoverability via Function Args** – This makes API usage clear, but what if a user wants composability? How do you handle dynamic query generation without introducing excessive complexity?
  - I believe a View Model can reference another one.
    This seems in theory possible, but might have impossible edge cases
    ```python
    class Character_WithPowerColumn(NamedTuple):
        id: int
        name: str
        power: Annotated[str, f"{Character.stats} -> '$.power'"]

    class Character_TotalPower(NamedTuple):
        id: int
        name: str
        total_power: Annotated[str, f"sum{Character_WithPowerColumn.power}"]

      @select(Character_TotalPower)
      def total_power():
          f"GROUP BY {Character.name}"

    engine.query(*Character_TotalPower.total_power)
    ```
    ```sql
    SELECT id, name, sum(stats -> '$.power') as total_power
    FROM Character
    GROUP BY name
    ```

2.  **ETL Prefetch for Reads** – How do you handle batch processing efficiently? Are you considering bulk inserts, WAL mode, or pragma tuning for large data imports?
  - This is a dumb question. All i mean is that if we need to access external data, we will not do it from application. we will use ETL to load up sqlite with the data we need, in correct format for model.
4.  **Minimizing Library-Specific Knowledge** – Type hints provide strong correctness, but what happens when users want to execute ad hoc queries not easily expressed via type annotations?
 - Literally look at my query example. I can do anything with the query. I can even do `engine.query("SELECT * FROM Character")`
5.  **No Dependencies** – Are you considering leveraging optional dependencies for specific use cases (e.g., `pydantic` for schema validation, `duckdb` for in-memory analytics)?
  - Another dumb question
7.  **Minimizing Boilerplate vs. Explicitness** – Your philosophy leans toward "more boilerplate" for clarity. Are there cases where too much boilerplate makes queries harder to manage?
 - This is not a real question or critique........
8.  **Meta-Free Models** – You aim to avoid explicit metadata definitions, but certain SQL features (indexes, constraints) need metadata. How do you handle those while staying true to your principles?
  - I'm backtracking on this. in fact I say:

    > Also this same scheme could be used for CREATE statement column constraints
    > ```
    > name: Annotated[str, Constraint("UNIQUE")]
    > ```
9.  **Concurrency & Multi-Threading** – SQLite supports concurrent reads but locks on writes. How do you handle high write contention in a single-node scenario?
  - Yes I am thinking about it.
