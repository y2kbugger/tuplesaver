<!-- AUTO-GENERATED — DO NOT EDIT. Regenerate with: python scripts/sync_plugin.py Source: API.md -->

# API

## Engine

```python
class Engine:
    def ensure_table_created(self, Model: type[M]) -> None: ...

    def insert(self, row: M) -> M: ...

    def find(self, Model: type[M], target, params=None, /, *, order=None) -> M: ...
    def select(self, Model: type[M], target=None, params=None, /, *, order=None, limit=None, offset=None) -> TypedCursorProxy[M]: ...

    def update(self, Model: type[M], target, params=None, /, **patch) -> int: ...
    def delete(self, Model: type[M], target, params=None, /) -> int: ...
```

- `e.insert` a record and return it with `id` populated.
- `e.find` a single record or `RecordNotFoundError` when nothing matches. If you ever _find_ yourself useing `cur.fetchone`, you may want `e.find` instead.
- `e.select` records via a `TypedCursorProxy[M]`. The cursor is **iterable**, and exposes the usual apsw methods (`cur.fetchone`, `cur.fetchall`, `cur.fetchmany`).
- `e.update` / `e.delete` records and return the number affected.

- `target` specific records
    - match all records, e.g. `None` (default)
    - by id, e.g. `10`
    - or a predicate expression, e.g. `Post.name == "Hi"` or `t"{Post.body} LIKE '%hello%'"`
- `params` override bind parameters in the predicate or bound `__select_query__`
  Interpolations whose source expression is a valid identifier are bound under that name
  (e.g. `{tolerance}` → `:tolerance`), so the same predicate can be reused
  with different values:
  ```python
  pred = t"{Post.score} >= {min_score}"
  engine.select(Post, pred, {"min_score": 50})
  engine.select(Post, pred, {"min_score": 90})
- `order` clause, e.g. `"name DESC, id"`, or a t-string for refactor-safe
  column references, e.g. `t"{Post.name} DESC"` (FK paths like
  `t"{Athlete.team.name}"` lower to scalar subqueries).
- `limit` and `offset` for pagination.
  ```

## Models

**Row** — A dataclass of typed fields for returning `e.find` and `e.select` results.

The query can be bound two ways:

- `__select_query__` to bind a specific SQL query to the model, allows parameters.
- `__table_name__` assigned to a db `VIEW`

    ```python
    class One(Row):
        val: int
        __select_query__ = "SELECT 1"
    ```
    ```python
    class Managers(Row):
        name: str
        __table_name__ = "managers_view"
    ```

**TableRow** — A `Row` that is backed by a table, provides an `id: int | None` field. In addition to read operations, `TableRow` models also support `e.insert`, `e.update`, and `e.delete`.

```python
class User(TableRow):
    name: str
    age: int
    manager: User | None
```

## Predicates

**Relation (R)** — Predicate expression built from model fields. Traversal
across foreign keys is expressed by field chaining and lowered to **EXISTS
semi-joins** in SQL.

```python
User.name == "Jon"
User.manager.name == "Paul"
(User.name == "Jon") | (User.manager.name == "Paul")
```

Raw SQL predicates can be embedded with PEP 750 t-strings so model field
references stay refactor-safe:

```python
t"{User.name} LIKE 'J%'"
```

For find/update/delete-by-id, pass the integer id directly as the target
(compiled as `id = ?`):

```python
engine.find(User, 10)
```


### `__select_query__` — model-bound queries

A `Row` model may set `__select_query__` to bake an entire SQL statement onto
the model; `engine.select` / `engine.find` then run that query instead of
generating a `SELECT`. The model becomes a typed, reusable result shape for any
query — aggregations, custom joins, `PRAGMA`, `UNION`, SQLite-specific
functions, etc. Columns map **positionally**, so they can be named anything.

```python
class TopScores(Row):
    name: str
    score: float

    __select_query__ = t"SELECT {Post.name}, {Post.score} FROM {Post}"

engine.select(TopScores, order="score DESC", limit=3)
```

- The value is a **t-string** (field references lowered to SQL) or a plain
  `str` for fixed SQL such as `t"PRAGMA table_info({Post})"`.
- `target`, `order`, `limit`, and `offset` still apply — the query is wrapped in
  a CTE that aliases its output columns positionally to the model's field names,
  so a `target` references those field names regardless of how the inner query
  named them (e.g. `engine.find(AvgScore, AvgScore.avg_score > 5)` over
  `SELECT avg({Post.score})`). A `PRAGMA` query can't be wrapped, so
  `target`/`order`/`limit` are unavailable on PRAGMA-style models.
- Only allowed on `Row` models; declaring it on a `TableRow` raises
  `SelectQueryNotAllowedOnTableRow`.

## Joins

`JOIN`s in predicates are automatic, and disambiguated by the reference path, e.g. `Athlete.team.name`.
Foreign-key traversal is lowered to either **EXISTS semi-joins** in or **scalar subqueries semi-joins** depending on the context.

## Backrefs

A backref is a virtual reverse relationship declared on the parent with the
`backref()` field specifier and an explicit forward-FK reference. `fk=` may be
either the typed field reference `Child.parent` or the equivalent
fully-qualified string `"Child.parent"`. Cardinality comes from the
annotation: `Rows[Child]` is has_many, a bare scalar `Child` is has_one.

```python
class Player(TableRow):
    name: str
    squad: Squad                                       # forward FK (child -> parent)

class Squad(TableRow):
    name: str
    players: Rows[Player] = backref(fk=Player.squad)   # reverse (parent -> children)
```

The typed form is refactor-safe, but it is evaluated while the parent's class
body runs, so it still wants the child defined **before** the parent. The
string form resolves lazily instead, so it is also available for parent-first
or self-referential declarations:

```python
class Node(TableRow):
    name: str
    parent: "Node | None"                              # forward FK to itself
    children: Rows["Node"] = backref(fk="Node.parent") # reverse (string form)
```

Backref fields back no column and never enter SQL. Navigation materializes
lazily on first access (one `select`) and is cached; has_many yields an
immutable `Rows` (a `tuple` subclass), has_one a single row or `None`. An
unsaved parent yields an empty `Rows()` / `None`.

Filter through an index to stay typed (`[0]` is statically the `Child`; the
semi-join still spans the whole relation):

```python
engine.select(Squad, Squad.players[0].number == 7)
```

## Type Mapping
SQLite has only five native storage types
(see the [sqlite3 type docs](https://docs.python.org/3/library/sqlite3.html#sqlite3-types)):

| Python | SQLite  |
|--------|---------|
| None   | NULL    |
| int    | INTEGER |
| float  | REAL    |
| str    | TEXT    |
| bytes  | BLOB    |

On top of those, `sqlir` handles the following types automatically:

| Python                                           | SQLite Schema Type     | Mechanism                                            |
|:-------------------------------------------------|:-----------------------|:-----------------------------------------------------|
| `bool`                                           | `BOOL_INT` (INTEGER)   | built-in special case (0/1)                          |
| `datetime`                                       | `DATETIME_TEXT` (TEXT) | unquoted ISO-8601 string via `msgspec.to_builtins()` |
| `date`                                           | `DATE_TEXT` (TEXT)     | unquoted ISO-8601 string via `msgspec.to_builtins()` |
| `time`                                           | `TIME_TEXT` (TEXT)     | unquoted ISO-8601 string via `msgspec.to_builtins()` |
| `Decimal`                                        | `DECIMAL_TEXT` (TEXT)  | unquoted decimal string via `msgspec.to_builtins()`  |
| `UUID`                                           | `UUID_TEXT` (TEXT)     | unquoted UUID string via `msgspec.to_builtins()`     |
| `Enum` (String-based)                            | `ENUM_TEXT` (TEXT)     | unquoted string via `msgspec.to_builtins()`          |
| `Enum` (Integer-based)                           | `ENUM_INT` (INTEGER)   | unquoted int via `msgspec.to_builtins()`             |
| buffer protocol (e.g. `memoryview`, `bytearray`) | `BLOB` (BLOB)          | apsw auto-adapts via buffer protocol as bytes        |
| `dict`, `list`, `set`, `tuple`, `dataclass`, etc | `JSON_TEXT` (TEXT)     | msgspec JSON encode/decode                           |
| `Any`                                            | (N/A)                  | on `Row` only, unconverted apsw passthrough          |

Any other type that msgspec can serialize is stored as JSON. If msgspec cannot
serialize the type, it raises at write time.


## Error Handling (Web Frameworks)

`engine.find()` raises `RecordNotFoundError` when no matching record exists,
following Ruby on Rails semantics. This makes it easy to convert to HTTP 404
responses in web frameworks:

```python
from sqlir.engine import Engine, RecordNotFoundError

# Flask example
@app.errorhandler(RecordNotFoundError)
def handle_not_found(e):
    return {"error": str(e)}, 404

# FastAPI example
@app.exception_handler(RecordNotFoundError)
async def not_found_handler(request, exc):
    return JSONResponse(status_code=404, content={"detail": str(exc)})
```

Note: `engine.select()` returns `[]`instead of raising, giving you the choice of how to handle missing records.

## API Comparison

|   | Feature                                | sqlir                                                          | Rails ActiveRecord                                               |
|:--|:---------------------------------------|:--------------------------------------------------------------------|:-----------------------------------------------------------------|
|   | **Model Definition**                   |                                                                     |                                                                  |
|   | Model class                            | `class Post(TableRow): ...`                                         | `class Post < ApplicationRecord`                                 |
|   | Field definitions                      | `name: str`  (type annotation)                                      | Inferred from database schema                                    |
|   | Foreign key definition                 | `band: Band \| None` (type annotation)                              | `belongs_to :band`                                               |
|   | Model instantiation                    | `post = Post("Hi", dt.now())`                                       | `post = Post.new(name: "Hi")`                                    |
|   | Modify fields                          | `dataclasses.replace(post, name="Hello")`                           | `post.name = "Hello"`                                            |
|   | JSON fields                            | `list` / `dict` / etc via msgspec                                   | `serialize` or `json` column type                                |
|   | _Relationships_                      |                                                                     |                                                                  |
|   | Joins                                  | implicit by path reference, e.g. `Post.user.name` (EXISTS semi-join)| `joins(:team => :league)`                                        |
|   | Loading                                | always lazy (`Lazy[M]` FK proxy)                                    | lazy with `includes`                                             |
| * | Backref relationships                  | `players: Rows[Player] = backref(fk=Player.squad)`                  | `has_many :people`                                               |
| * | Many-to-many                           | through join models                                                 | `has_and_belongs_to_many`                                        |
|   |                                        |                                                                     |                                                                  |
|   | **Read**                               |                                                                     |                                                                  |
|   | Find one by Id                         | `engine.find(Post, 1)`                                              | `Post.find(1)`                                                   |
|   | Find one by fields                     | `engine.find(Post, Post.name == "Hi")`                              | `Model.find_by(name: "Hi")`                                      |
|   | Find one (raw predicate)               | `engine.find(Post, t"{Post.name} = 'Hi'")`                          | `Post.find_by_sql(sql).take`                                     |
| * | Find one or Create                     | —                                                                   | `Post.find_or_create_by(a: 1, b: 2)`                             |
|   | Select all                             | `engine.select(Post)`                                               | `Post.all`                                                       |
|   | Select by relation                     | `engine.select(Post, Post.name == "Hi")`                            | `Post.where(name: "Hi").all`                                     |
|   | Select with order/limit/offset         | `engine.select(Post, ..., order="name", limit=10, offset=20)`       | `Post.where(...).order(...).limit(...).offset(...)`              |
|   | Streaming cursor                       | `for r in engine.select(Post, Post.name == "Hi"): ...`              | `Post.where(...).find_each`                                      |
|   | Raw SQL                                | `Row` + `__select_query__`, then `engine.select(QueryModel)`        | `Model.find_by_sql(sql)`                                         |
|   | Aggregations                           | `__select_query__` / VIEW with `Row` models (views + migrate work well) | `Model.group(...).sum(...)`                                  |
| * | Exists                                 | —                                                                   | `Post.where(name: "Hi").exists?`                                 |
| * | Pluck (scalar columns)                 | —                                                                   | `Post.pluck(:name)`                                              |
| * | Take (first N)                         | `engine.select(Post, limit=N)`                                      | `Post.take(N)`                                                   |
|   |                                        |                                                                     |                                                                  |
|   | **Write**                              |                                                                     |                                                                  |
|   | Insert                                 | `post = engine.insert(post)`                                        | `post.save`                                                      |
|   | Insert (one-liner)                     | `post = engine.insert(Post("Hi", dt.now()))`                        | `Post.create(name: "Hi")`                                        |
|   | Update by Id                           | `engine.update(Post, id, name="Apple")`                             | `Post.update(id, name: "Apple")`                                 |
|   | Update by relation                     | `engine.update(Post, Post.title == "snails", name="y2k")`           | `Book.where(title: "snails").update_all(name: "y2k")`            |
|   | Update by raw predicate                | `engine.update(Post, t"{Post.title} LIKE '%snails%'", name="y2k")`  | `Book.where("title LIKE ?", "%snails%").update_all(name: "y2k")` |
|   | Delete by Id                           | `engine.delete(Post, id)`                                           | `Post.delete(id)`                                                |
|   | Delete by relation                     | `engine.delete(Post, Post.title == "snails")`                       | `Book.where(title: "snails").delete_all`                         |
|   | Delete by raw predicate                | `engine.delete(Post, t"{Post.title} LIKE '%snails%'")`              | `Book.where("title LIKE ?", "%snails%").delete_all`              |
| * | Insert many                            | —                                                                   | `Post.insert_all([{a: 1}, {a: 2}])`                              |
| * | Upsert (single statement, UQ cols req) | —                                                                   | `Post.upsert({a: 1, b: 2}, unique_by: ['a'])`                    |
| * | Upsert (select + update/insert)        | —                                                                   | `Post.update_or_create_by(...)`                                  |
|   |                                        |                                                                     |                                                                  |
|   | **Schema Management**                  |                                                                     |                                                                  |
|   | Adhoc Table creation                   | `engine.ensure_table_created(Model)`                                | Rails migrations                                                 |
|   | Migrations                             | `sqlir-migrate` CLI + Python API                               | `rails generate migration`                                       |
|   | Foreign key constraints                | auto-generated from `Model \| None` FK fields                       | manual in migrations                                             |
|   |                                        |                                                                     |                                                                  |
|   | **Connection Management**              |                                                                     |                                                                  |
|   | Connection handling                    | explicit `Engine` instance                                          | implicit connection pool                                         |
|   | Transactions                           | `with engine.connection:`                                           | `Model.transaction do ... end`                                   |
|   | Connection pooling                     | discouraged; use ephemeral connections, separate RW and RO          | per-thread connection                                            |
|   |                                        |                                                                     |                                                                  |
|   | **Advanced Features**                  |                                                                     |                                                                  |
|   | Validations                            | not planned                                                         | built-in validations                                             |
|   | Callbacks/Hooks                        | not planned                                                         | before_save, after_create, etc.                                  |
|   | N+1 problem mitigation                 | fast SQLite mitigates concern                                       | `includes()` eager loading                                       |

Features marked with `*` are not implemented; see [TODO.md](TODO.md) for status.
