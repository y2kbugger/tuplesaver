# WIP
- Move create and update ddl to model.py
- Harmonize names of model types throughout the codebase
  - "table model" - Backed by a table in the database
  - "alt model" - Backed by a view in the database, but could have fields that are added (eventually), removed, or modified. Still have an id field that mapps to the original table.
  - "adhoc model" - Backed by any arbitrary query, doesnt have an id field, and can have any fields.
  - "nontable model" - "alt model" or "adhoc model"
- Ensure that we use named placeholder when possible
  https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders
    cur.executemany("INSERT INTO lang VALUES(:name, :year)", data)
- move all sql generation to sql.py (combo of query.py and insert/update/create stuff from engine and model)
  - compress find_by into find
  - see if we can switch to dict based queries, e.g. `engine.find(MyModel, {MyModel.name: "Bart"})`
  - how to make query.select more integrated to Engine so its more like find?, and also update_all/delete_all?
  - then we can use that exact where clause in select, update, and delete
    - attempt to overload these in a way feels natural, e.g. find returns one, select returns many, update and delete can work either by id or by where clause.
    - Also allow an fstring for the where clause, e.g. `engine.find(MyModel, f"{MyModel.name} = 'Bart'")`
    - maybe we can rely on our getattribute hack to make fstrings work without AST hacking.
- Add foreign key constraints to the table creation
  - through the metadata system?? appending to meta during ensure_table_created?
  `foreign key (team_id) references Team(id)`


# Bugs


# Testing
- ematest that everything works on when doing arbitrary view model queries that select FK in as model relationships
- Could make a unit test for self join also
- test is_registered_fieldtype
  - unknown types, unregistered models, both Optional and non-Optional variants
- find/find_by raise if more than one result matched
- test reensureing model updates if and only if schema has been migrated correctly
- Test that non Fields greater than zero cannot be called id
- Test for cyclic data structures e.g. A -> B -> C -> A
- Test the foreign key may only be a union with None i.e. Optional BUT NOT with int or something else
- Investigate/ Test what Happens when specifying Model | int, should this raise??
- how handle unions of two valid types, e.g. int | str
  - Adapting would work fine, but conversion could be ambiguous
  - I think we should just raise on this
- Test can get using model with int as FK rather than Model to stop recursive loading
  e.g. int instead of Node in a Person_IntFK model
- Test you can have two field of same type,e.g. right_node, left_node
- How to test that we don't trigger lazy queries ourselves?
- Validate in Meta creation that related models in fields of table models are actually table models and not view models
- Test duplicate joins in query.select deduplicates
- Benchmark and test connection creation and closing
## testingmeta
- I want to instrument sqlite to log and profile queries.
- use the assert_type from typing to check type hints
  - Test types on select (both decorator and non)
- fix names / order of model_test.py, e.g. test_table_meta_... -> test_get_meta__....


# Next
- More standard adaptconverters Enum, set, tuple, time, frozenset, Path, UUID, Decimal, bytes
  - tests?, examples?
- I want to fall back to pickles for any type that is not configured, and just raise if pickle fails
  - tests?, examples?
- maybe look at that decorator that tells typing checkers that a class is only for types for cursor proxy

## engine.update
To only some fields, on a single existing row, pull id from row:
```python
engine.update(id, name="Apple")
```
```sql
update MyModel set name = 'Apple' where id = 42;
```

## engine.upsert
| Upsert          | `Model.upsert(attrs, unique_by)`  | Insert or update based on unique key |

infered constraits from DB
- https://sqlite.org/syntax/column-def.html
- https://sqlite.org/syntax/column-constraint.html
- We can just use migrations to add constraints and make db the source of truth.
- We don't actually even need to read them in except to add validation on upserts (ie, only allow upserting on sets of unique columns)


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

## Backpop
- Also considder one to one relationships that backpop to a single instance rather than a list
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


# Later

## Explain Model
I want to be able to explain model function. This would explain what the type annotation is., what the sqllite column type is, And why?. Like it would tell you that an INT is a built-in Python SQLite type., but a model is another model, And a list of a built-in type is stored as json., And then what it would attempt to pickle if there would be a pickle if it's unknown..
This would help distinguish between a list of model and a list of something else. 
This is cool cuz it blends casa no sql with SQL. We could probably even make a refactoring tool to switch between the two.
- Also want to explain querys from engine
  - This could also be an off ramp from engine.select to a more generic query builder, e.g. `engine.query(Model, sql, params)`



## Foreign Key enforcement
Off by default, but can be enabled with
- `PRAGMA foreign_keys = true;`

## DDL 2.0, e.g. future of `engine.ensure_table_created`
  - maybe leave mutation/creation up to migrations. Then either
    - leave "ensure table created" to register Model
    - or register Model lazily upon first use. **favorite**

## Migrations
- consider https://martinfowler.com/articles/evodb.html
- Auto add column(s) to table if they don't exist
  - or maybe just let this be the easy way to teach people to use explicit migrations
- Maybe store all versions of models in the db in additions to migrations script.
  - What about when "blowing away" the db?
  - what value does this add?
    - Could it help generate scripts?
    - could it just give insight to recent changes during dev?
- Have a dedicated external entrypoint to control migrations and rollback ala RoR AR
- Use sqlite backup api to snapshot safely the DB while connected before a migration, this will allow easy rollback

## Connection Management and Concurrency
- one connection per thread, like RoR AR
- Another options is to have two thread pools, one for reads and one for writes
  - This is more complex, but elimnates Busy errors
  -
   https://kerkour.com/sqlite-for-servers
- SQLite supports concurrent reads but locks on writes.
  - Can be configured to block instead of raising an error on write contention
    https://sqlite.org/c3ref/busy_timeout.html
- Make sure to check for WAL mode for better concurrency
  - https://www.sqlite.org/wal.html
- https://kerkour.com/sqlite-for-servers
  - A 2024 guide to SQLite use and tuning on backend
  - `PRAGMA busy_timeout = 5000;` 5 seconds for app, 15 seconds for API
    - Allows waiting for a write lock to be released before raising an error

## transaction management
Offer a context manager for transactions, cursors, and committing



# One Day Maybe
- Allow implicitly created NamedTuple models to be returned from queries
  - e.g. constructed based on query builder, etc.  could be usedful for adhoc queries to reduce boilerplate
  - do implicit instead of this older idea: "Instead of using view models to reduce number of columns, just inject raising Lazy Stubs for deselected columns"
- how to express more complex updates like this:
    `Book.where('title LIKE ?', '%Rails%').update_all(author: 'David')`
- Auto detect or provide a way to santize/escape LIKE params. e.g.  of % or _
- leverage tstring in python 3.14 to avoid AST hacking
- Allow query builder to allow partial paths in f-strings
  - e.g. `f"{RelatedModel.field}"` ipo full path of `f"{Model.related_model.field}"` in queries
  - then we can let sqlite fail if abigous? (does it fail or just guess?
  - Also we would need to guess the join and fail if it is ambiguous, or require the join to be specified
- Strict mode to disallow certain lazy ops, require explict eager loads
  https://guides.rubyonrails.org/active_record_querying.html#strict-loading
- strict mode sanitize models before passing to template engines??
- engine.exists (rails has relation.exists, e.g. Customer.where(first_name: "Ryan").exist
- scalar accessors, e.g. RoR AR's pick. get one value from one row and one
  column (technically pick also allows multiple colums) don't see why not just use
  find/find_by then access the field
- RoR annotate (and sql comments so that later we can use it during observabilites)
- Non recursive engine.save(root, deep=True), eliminate stackoverflow for deep recursive models e.g. depth=2000 BOM
- mutable id object as id which can mutate when saved.
- Consider dropping the injected Engine, and goto a fluent RoR AR style interface
  - e.g. `row.save()` ipo `engine.save(row)`




## GROUP BY / Aggregation
Aggregations queries are more tightly coupled to the View Model because the model must define the aggregations, but the query defines the grouping. Therefore you might want to define the query f-string in the model def. But this is
just a stylistic choice

To make annotations work, we force usage of `from __future__ import annotations`

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

## Multi Table View Model SELECT
This wouldn' be worth it, except we are already introducing this syntax for  aggregations
- make sure to test for column name collsions
- could we store map from _tuplegetter -> MetaField in Meta and get_meta_by_tuplegetter(tg) -> Meta, this would allow writing multi table quyeries using Model.name, Model2.value etc

Maybe wrap it like this?

    team_name: Annotated[str, Col(f"{Athlete.team.name}")]

but that might also allow us to drop the f-string completely like this?:

      team_name: Annotated[str, Col(Athlete.team.name)]

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

## JSON extracted field in SELECT
This kind of thing is already supported effortlessly in select style predicates.
This might come for free with aggregations.
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

## Alternate lambda syntax
Just a more concise version of the decorator version. might be hard to squeeze into the typehints
```python
M, q = select(Athlete)(lambda: f"WHERE name LIKE '%e%'")
```

## A way to package queries with models to make view like objects??
Could be one or more queries for one model. Could have parameters. could want to
reuse by adding where, or somethiing else??
```python
class TableInfo(NamedTuple):
    cid: int
    name: str
    type: str
    notnull: int
    dflt_value: Any
    pk: int

sql = f"PRAGMA table_info({Athlete.__name__})"

cols = engine.query(TableInfo, sql).fetchall()
```
Note: this is like RoR AR's scopes
  scope :in_print, -> { where(out_of_print: false) }
  scope :out_of_print, -> { where(out_of_print: true) }
  scope :old, -> { where(year_published: ...50.years.ago.year) }
  scope :out_of_print_and_expensive, -> { out_of_print.where("price > 500") }
  scope :costs_more_than, ->(amount) { where("price > ?", amount) }

also allows a default scope

  default_scope { where(out_of_print: false) }



## Performance
- https://kerkour.com/sqlite-for-servers
  - `PRAGMA synchronous = NORMAL;`
  - `PRAGMA journal_mode = WAL;`
  - `PRAGMA cache_size = 10000;`
  - `PRAGMA cache_size = 1000000000`
- https://gcollazo.com/optimal-sqlite-settings-for-django/
- types of eager loads, see https://guides.rubyonrails.org/active_record_querying.html#eager-loading-associations
approx 20% perf boost for execute many on 20k rows, not worth it, yet
- Bulk inserts

```python
def insert_all[R: Row](self, Model: type[R], rows: Iterable[R]) -> None:
    """Insert multiple rows at once."""
    insert = get_meta(Model).insert
    assert insert is not None, "Insert statement should be defined for the model."
    with self.connection:
        self.connection.executemany(insert, rows)
```

## View Model Reuse/Composition
This would be like relations in RoR AR
I believe a View Model can reference another one.
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





# Probably Never
- a true cursor proxy and fetchoneonly helper/wrapper
  - cost penalty for get row benchmark (maybe test again later)
  - a pretty thin wrapper over native functionality

# Never, Will not Implement
- `Any` for table models
  - it would automatically use the dynamic type adapter, but it would not know which converter to use to get it back to the original type
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
  - Why Not?
    - The typed ID is just another thing to know, and understand for users
      - violates actually simple vs seemingly simple
      - violates minimize library specific knowledge requirements
    - The supposed benefit of simpler delete api actually hurts readability.
      ```python
      engine.delete(some_id)
      ```
      is less clear than
      ```python
      engine.delete(MyModel, some_id)
      ```
      and we already have an overload for deleting a row
      ```python
      engine.delete(row)
      ```
      This violates choose boilerplate over magic
    - It is annoying that you have to repeat the Model name in the Model def:
      ```python
      class MyModel(NamedTuple):
          id: Id[MyModel]
          name: str
          date: dt.datetime
      ```
      Violates minimize boilerplate
- Add passthrough for commit? e.g. engine.commit
  - just let the user use the existing connection, engine.connection.commit()?
  - Violates, only do what can't be done with the sqlite standard library
- Allow str serde, i.e. in addtion to the bytes api
  - just explicitly encode/decode to bytes
  - Violates choose boilerplate over magic
- Query builder on engine
  - just use the query builder directly
  - Violates choose boilerplate over magic
  - better to use Model, sql, params as a stable and interoperable intermediate representation
