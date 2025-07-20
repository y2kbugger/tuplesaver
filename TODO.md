# WIP
- I want to be able to persist an Enum without configuration
  - tests?, examples?
- Test for id as a str, and int but not int | None to raise FieldZeroIdRequired

# Bugs
- Meta caches invalid models e.g. missing id, or missing adapters

# Testing
- Test cleanup
  - Harmonize the def-scoped Model class names in the tests
  - use test specific Models in a small scope
  - refactor tests to be more granualar, e.g. test one table column at a time using smaller specific models, but also use parametrize to make test matrices
  - group tests, and promote _some_ model reuse if it makes sense
  - All test should use double underscore unit_under_test__when__effect style when possible
- Test types on select (both decorator and non)
- test for fetchone returning none
- Test for cyclic data structures e.g. A -> B -> C -> A
- test that you cannot insert, update, or delete, a view model, only a table model
  - test that mutation queries don't even get set for the view meta
- Test the foreign key may only be a union with None i.e. Optional BUT NOT with int or something else
- Investigate/ Test what Happens when specifying Model | int, should this raise??
- how handle unions of two valid types, e.g. int | str
  - Adapting would work fine, but conversion could be ambiguous
  - I think we should just raise on this
- Test can get using model with int as FK rather than Model to stop recursive loading
  e.g. int instead of Node in a Person_IntFK model
- Test you can have two field of same type,e.g. right_node, left_node
- How to test that we don't trigger lazy queries ourselves?
- use the assert_type from typing to check type hints throughout all tests
- Benchmark and test connection creation and closing
- I want to instrument sqlite to log and profile queries.

# Next
- Modify example story of "queries", do raw sql first, then show query.py builder
- Remove demos of error handling in example.ipynb
- Ensure that we use named placeholder when possible
  https://docs.python.org/3/library/sqlite3.html#sqlite3-placeholders
    cur.executemany("INSERT INTO lang VALUES(:name, :year)", data)
- Add foreign key constraints to the table creation
  - through the metadata system?? appending to meta during ensure_table_created?
  `foreign key (team_id) references Team(id)`
- I want to fall back to pickles for any type that is not configured, and just raise if pickle fails
  - tests?, examples?
- Add in lazy loading of relationships
  - should reduce need for some of the model views in examples e.g. id only views
  - This would unlock the ability to define backpop relationships and use them in queries

# Later
- column constraints as annotations, like the select
  https://sqlite.org/syntax/column-def.html
  https://sqlite.org/syntax/column-constraint.html
  Somthing like this
      name: Annotated[str, Constraint("UNIQUE")]
- joined loads
  - approx 20% perf boost for execute many on 20k rows, not worth it, yet
- engine.update
  To only some fields, on a single existing row, pull id from row:
  ```python
  engine.update(row, name="Apple")
  ```
  ```sql
  update MyModel set name = 'Apple' where id = 42;
  ```
- Make "ensure table created" concept part of migrations instead of engine??
  - e.g. i don't want all those tests for schema mixed in with CRUD
  - or maybe just refactor files
  - or maybe make the schema mutation as part of migration, but leave "ensure table created" as a just a check in engine that registers table meta
  - or could ensure happen implicity on first use??, (and recursively for all relationships)

## engine.upsert
| Upsert          | `Model.upsert(attrs, unique_by)`  | Insert or update based on unique key |
|                 | `Model.find_or_initialize_by(attr: val)` | Find or create new object  (simulated upsert)|

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


## Explain Model
I want to be able to explain model function. This would explain what the type annotation is., what the sqllite column type is, And why?. Like it would tell you that an INT is a built-in Python SQLite type., but a model is another model, And a list of a built-in type is stored as json., And then what it would attempt to pickle if there would be a pickle if it's unknown..
This would help distinguish between a list of model and a list of something else. 
This is cool cuz it blends casa no sql with SQL. We could probably even make a refactoring tool to switch between the two.


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


## multi-row Model based api
multi-row apis like update_all, delete_all, etc.

## ROR Persistence api (double check with real docs)
| Action          | API Signature                     | Notes                              |
| --------------- | --------------------------------- | ---------------------------------- |
| Insert (create) | `Model.create(attrs)`             | Creates and saves one new record   |
|                 | `Model.new(attrs)` + `obj.save`   | Two-step create                    |
| Update          | `obj.update(attrs)`               | Sets and saves attrs               |
|                 | `obj.update_attribute(attr, val)` | No validations/callbacks           |
|                 | `Model.update(id, attrs)`         | Update by ID                       |
|                 | `Model.update_all(attrs)`         | Bulk update; no callbacks          |
| Delete          | `obj.destroy`                     | Deletes with callbacks             |
|                 | `obj.delete`                      | Deletes without callbacks          |
|                 | `Model.delete(id)`                | One-liner delete                   |
|                 | `Model.delete_all`                | Deletes all (no callbacks)         |
| Save            | `obj.save`                        | Insert or update, runs validations |
| Force Save      | `obj.save!`                       | Same, raises exception on failure  |
| Reload          | `obj.reload`                      | Re-fetch from DB                   |
| Upsert          | `Model.upsert(attrs, unique_by)`  | Insert or update based on unique key |
|                 | `Model.find_or_initialize_by(attr: val)` | Find or create new object  (simulated upsert)|
## ROR Retrieval api (double check with real docs)
| Action        | API Signature                    | Notes                       |
| ------------- | -------------------------------- | --------------------------- |
| Get by ID     | `Model.find(id)`                 | Raises if not found         |
| Optional get  | `Model.find_by(attr: val)`       | Returns nil if not found    |
| Where clause  | `Model.where(attr: val)`         | Returns a Relation          |
| All rows      | `Model.all`                      | Lazy-loaded Relation        |
| First/Last    | `Model.first`, `Model.last`      | Based on PK sorting         |
| Limit         | `Model.limit(n)`                 | Chainable                   |
| Order         | `Model.order(:attr)`             | Chainable                   |
| Select fields | `Model.select(:attr1, :attr2)`   | Partial row loading         |
| Pluck fields  | `Model.pluck(:attr)`             | Returns array of raw values |
| Exists?       | `Model.exists?(attr: val)`       | Boolean                     |
| Count         | `Model.count`                    | Integer                     |
| Batch read    | `Model.find_each(batch_size: n)` | Iterates in chunks          |



# One Day Maybe
- Consider connection/transaction management
  - context manager?
  - how long is sqlite connection good for? application lifetime?
  - closing cursors? commit? difference between?
    - Context manager on cursor?
  - connection pooling?
- mutable id object as id which can mutate when saved.
- Consider dropping the injected Engine, and goto a fluent RoR AR style interface
  - e.g. `row.save()` ipo `engine.save(row)`
- Consider Concurrency in both read and write
  – SQLite supports concurrent reads but locks on writes.
  - what happens if two threads try to write to the same table at the same time?
  - How to actually test this?
  - Is there a connection setting (check same thread) that can be used to at least detect this?
  - https://www.sqlite.org/wal.html
- Non recursive engine.save(root, deep=True), eliminate stackoverflow for deep recursive models e.g. depth=2000 BOM


## Fully qualified field names that are rename symbol safe in queries kwargs
i.e.

    mylist  = e.find_by(List, List.name == list_name)

ipo

    mylist  = e.find_by(List, name = list_name)


## Backpop
Thinking to not do this, circular references might make it impossible anyway. just make it easy to fetch.
It also side steps the issue of double querying to fill in the forward reference/caching and wiring up the FK to the backprops. it actually forces everying to be a circular reference which isn't possible.
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

## View Model Reuse/Composition
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


# Probably Never
- a true cursor proxy and fetchoneonly helper/wrapper
  - cost penalty for get row benchmark (maybe test again later)
  - a pretty thin wrapper over native functionality

# Never, Will not Implement
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
