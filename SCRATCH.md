# Docs/Notes
## Types of Models
- "table model" - Backed by a table in the database
- "alt model" - Backed by a view in the database, but could have fields that are added (eventually), removed, or modified. Still have an id field that mapps to the original table.
- "adhoc model" - Backed by any arbitrary query, doesnt have an id field, and can have any fields.
- "nontable model" - "alt model" or "adhoc model"
## JOINs
JOINs are automatic, and disambiguated by the the reference path `Athelete.team.name`
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

    from tuplesaver import register_adapt_convert

    register_adapt_convert(datetime.datetime, adapt_datetime_iso, convert_datetime_iso)

### A note on JSON columns
Currently we do not apply adapters/converters to json dumps, because without specifying a schema there is no way to reliably recover type info.

Even something as simple as date, would be ambiguous, because it could be a date or str going in

    {"date": "2021-01-01"}

If we enable dt.datetime serialization, then the above could have been

    dict(date=dt.datetime(2021, 1, 1))

or

    dict(date="2021-01-01")

## sqlite3
https://docs.python.org/3/library/sqlite3.html
https://sqlite.org/np1queryprob.html
https://andre.arko.net/2025/09/11/rails-on-sqlite-exciting-new-ways-to-cause-outages/
https://fractaledmind.com/2024/04/15/sqlite-on-rails-the-how-and-why-of-optimal-performance/
https://rogerbinns.github.io/apsw/cursor.html - Richard Hipp says this is a better wrapper.

## Other big users of apsw
see: https://clickpy.clickhouse.com/dashboard/apsw
- all of these seem to focus on data-exploration / analytics use cases
  - https://github.com/AnswerDotAI/fastlite
  Similar, add CRUD ORM on top of DataClass models that generate straight from the schema, or CREATE from dataclasses.
  - https://sqlite-utils.datasette.io/en/stable/python-api.html / https://github.com/AnswerDotAI/apswutils this is a library and a fork that adds apsw support to sqlite-utils.


Really need to read and understand this new annotation sematics coming in 3.14, as well as difference between inspect.get_nnotations and typing.get_type_hints
https://docs.python.org/3/howto/annotations.html#annotations-howto
https://github.com/python/cpython/issues/102405
https://peps.python.org/pep-0649/

## Integrating into a web framework

### Error Handling for Web Frameworks

`engine.find()` raises `RecordNotFoundError` when no matching record exists, following Ruby on Rails semantics. This makes it easy to convert to HTTP 404 responses in web frameworks:

```python
from tuplesaver.engine import Engine, RecordNotFoundError

# Flask example
@app.errorhandler(RecordNotFoundError)
def handle_not_found(e):
    return {"error": str(e)}, 404

# FastAPI example
@app.exception_handler(RecordNotFoundError)
async def not_found_handler(request, exc):
    return JSONResponse(status_code=404, content={"detail": str(exc)})
```

Note: `engine.find_by()` returns `None` instead of raising, giving you the choice of how to handle missing records.

## API Comparison

|   | Feature                                | tuplesaver                                                             | Rails ActiveRecord                                               |
|:--|:---------------------------------------|:-----------------------------------------------------------------------|:-----------------------------------------------------------------|
|   | **Model Definition**                   |                                                                        |                                                                  |
|   | Model class                            | `class Post(NamedTuple): ...`                                          | `class Post < ApplicationRecord`                                 |
|   | Field definitions                      | `name: str`  (type annotation)                                         | Inferred from database schema                                    |
|   | Foreign key definition                 | `band: Band` (type annotation)                                         | `belongs_to :band`                                               |
|   | Model instantiation                    | `post = Post(None, "Hi", dt.now())`                                    | `post = Post.new(name: "Hi")`                                    |
|   | Modify fields                          | `post._replace(name="Hello")`                                          | `post.name = "Hello"`                                            |
|   |                                        |                                                                        |                                                                  |
|   | **Basic CRUD Read**                    |                                                                        |                                                                  |
|   | Find one by Id                         | `engine.find(Post, 1)`                                                 | `Post.find(1)`                                                   |
|   | Find one by field                      | `engine.find_by(Post, name="Hi")`                                      | `Model.find_by(name: "Hi")`                                      |
|   | Find one or Create                     | Not planned                                                            | `Post.find_or_create_by(a: 1, b: 2)`                             |
|   | Find many                              | @select(Post): f"WHERE {Post.name} = 'Hi'"                             | `Post.where(name: "Hi")`                                         |
|   | __Querying__                           |                                                                        |                                                                  |
|   | Raw SQL                                | `engine.query(Model, sql)`                                             | `Model.find_by_sql(sql)`                                         |
|   | Query builder                          | `@select(Model)` decorator                                             | `Model.where(...).order(...)`                                    |
|   | joins                                  | implicit for predicates, nothing explict yet                           | `joins(:team => :league)`                                        |
| * | Aggregations                           | Adhoc models with annotations                                          | `Model.group(...).sum(...)`                                      |
|   |                                        |                                                                        |                                                                  |
|   | **Basic CRUD Write**                   |                                                                        |                                                                  |
|   | Save (Insert or Update)                | `engine.save(post)`                                                    | `post.save`                                                      |
|   | Save (one-liner)                       | `engine.save(Post(None, "Hi", dt.now()))`                              | `Post.create(name: "Hi")`                                        |
| * | Update by Id                           | `engine.update(id, name="Apple")`                                      | `post.update(name: "Apple")`                                     |
| * | Upsert (Single statement, UQ cols req) | `engine.upsert(Post, {a:1, b:2}, unique_by=['a'])`                     | `Post.upsert({a: 1, b: 2}, unique_by: ['a'])`                    |
| * | Upsert (Select+Update/Insert, no UQ)   | `engine.update_or_create_by(Post, {a:1, b:2}, unique_by=['a'])`        | `Post.update_or_create_by(...)`                                  |
|   | Delete by Id                           | `engine.delete(Post, 1)`                                               | `Post.delete(1)`                                                 |
|   | Delete by instance                     | `engine.delete(post)`                                                  | `post.destroy`                                                   |
| * | Insert many                            | `engine.insert_all(Post, [Post(1,2), Post(3,4)])`                      | `Post.insert_all([{a: 1, b: 2}, {a: 3, b: 4}])`                  |
| * | Update many                            | `engine.update_all(Post, {name: "y2k"}, where={title="snails"})`       | `Book.where(:title => 'snails').update_all(name: 'y2k')`         |
| * | Update many (full query)               | @update(Post, {name: 'y2k'}): f"WHERE {Post.title} LIKE '%snails%'"    | `Book.where('title LIKE ?', '%snails%').update_all(name: 'y2k')` |
| * | Delete many                            | `engine.delete_all(Post, where={title="snails"})`                      | `Book.where(:title => 'snails').delete_all`                      |
|   | Delete many (full query)               | @delete(Post): f"WHERE {Post.title} LIKE '%snails%'"                   | `Book.where('title LIKE ?', '%snails%').delete_all`              |
|   |                                        |                                                                        |                                                                  |
|   | **Relationships**                      |                                                                        |                                                                  |
|   | Recursive save                         | `engine.save(member, deep=True)`                                       | `member.save` (automatic) ???                                    |
|   | Recursive loading                      | Automatic by default                                                   | Lazy loading with `includes`                                     |
| * | Backref relationships                  | `teams: list[Person]`                                                  | `has_many :people`                                               |
| * | Many-to-many                           | Through join models                                                    | `has_and_belongs_to_many`                                        |
|   |                                        |                                                                        |                                                                  |
|   | **Type System**                        |                                                                        |                                                                  |
|   | Type safety                            | Full static typing with mypy                                           | Runtime with Sorbet (optional)                                   |
|   | Custom types                           | `register_adapt_convert()`                                             | ActiveRecord serializers                                         |
|   | JSON fields                            | `list/dict` auto-serialized                                            | `serialize` or `json` column type                                |
|   |                                        |                                                                        |                                                                  |
|   | **Schema Management**                  |                                                                        |                                                                  |
|   | Table creation                         | `engine.ensure_table_created(Model)`                                   | Rails migrations                                                 |
| * | Migrations                             | Dedicated migration system                                             | `rails generate migration`                                       |
| * | Foreign key constraints                | Auto-generated                                                         | Manual in migrations                                             |
|   |                                        |                                                                        |                                                                  |
|   | **Connection Management**              |                                                                        |                                                                  |
|   | Connection handling                    | Explicit `Engine` instance                                             | Implicit connection pool                                         |
|   | Transactions                           | Manual `engine.connection.commit()`                                    | `Model.transaction do ... end`                                   |
| * | Connection pooling                     | Per-thread connections                                                 | per-thread connection                                            |
| * | Transaction context mgr                | `with engine.transaction():`                                           | `Model.transaction do ... end`                                   |
|   |                                        |                                                                        |                                                                  |
|   | **Advanced Features**                  |                                                                        |                                                                  |
|   | Validations                            | Not planned                                                            | Built-in validations                                             |
|   | Callbacks/Hooks                        | Not planned                                                            | before_save, after_create, etc.                                  |
| * | N+1 problem mitigation                 | Direct DB calls (fast SQLite)                                          | `includes()` eager loading                                       |



## Notes from RoR Active Record
### Connection Handling
Implicit Connection Handling: Active Record uses a global connection pool and thread-local connections instead of an explicitly passed session object. On the first database call in a given thread (e.g. a web request thread), Active Record will check out a connection from the pool and associate it with that thread
discuss.rubyonrails.org . That same connection is reused for all queries in the thread by default, instead of checking out/in on every query, to reduce lock overhead discuss.rubyonrails.org . Rails keeps track of the “current” connection via a thread-local key, ensuring each thread uses its own database connection .

Request Lifecycle and Cleanup: In a typical Rails request, the framework ensures the connection is returned to the pool at the end. The Rack middleware

Transactions and Context: Active Record provides methods like Model.transaction do ... end to run a block of code in a database transaction. Internally, this just uses the thread’s connection to BEGIN/COMMIT

### No Identity Map
Identity Map (or Lack Thereof): One notable aspect of Active Record’s implicit approach is that it historically does not implement a global identity map by default (unlike explicit session ORMs which typically do). In other words, if you query the same record twice in Rails (outside of the same short-lived transaction or object reference), you’ll get two separate Ruby object instances representing the same row. Rails did experiment with an optional Identity Map in Rails 3.2 (to ensure each object is loaded only once per request/thread)
api.rubyonrails.org
api.rubyonrails.org
, but it was disabled by default and later removed. Without an explicit session tracking all loaded entities, Rails forgoes the complexity of a long-lived identity map. This means less memory overhead and bookkeeping, at the cost of potential duplicate objects

### No engine or session
These are injected into the objects themselves at runtime. AR assumes DB schema is source of truth.
Migrations are done in a DSL with an external

### recursive saves are configured per attribute/field

### ruby autocompletions for fields are not native and come in via rbi files (like pyi files)
### relationships defined with `has_many`, `belongs_to`, `has_one`, `has_and_belongs_to_many`,


# One day do a Cython implementation for faster field descriptor access
# field_descriptor.pyx
# Cython implementation of FieldDescriptor for better performance

``` python
from typing import NamedTuple

cdef class Column:
    """Fast Column class using Cython"""
    cdef public str name
    cdef public object coltype

    def __init__(self, str name, object coltype):
        self.name = name
        self.coltype = coltype

    def __repr__(self):
        return f"Column(name={self.name!r}, coltype={self.coltype!r})"

    def __eq__(self, other):
        if not isinstance(other, Column):
            return False
        return self.name == other.name and self.coltype == other.coltype


cdef class CythonFieldDescriptor:
    """High-performance field descriptor using Cython"""
    cdef public Column column
    cdef public int index

    def __init__(self, Column column, int index):
        self.column = column
        self.index = index

    def __get__(self, object instance, object owner):
        # Fast path: if instance is None, return Column
        if instance is None:
            return self.column
        # Fast path: direct tuple indexing for instances
        return instance[self.index]

    def __repr__(self):
        return f"CythonFieldDescriptor(column={self.column}, index={self.index})"
```
