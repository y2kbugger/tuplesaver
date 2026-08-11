# WIP

# Bugs
- FK sql types should be INTEGER_ID not model name or something so that the type coming out is unabiguous.
- ruff complains about comparison to True, e.g. `if x == True` should be `if x` (is this supported by the Rel rendered)?
- FK constraints are declared (`REFERENCES Band(id)`), document on how users should set `PRAGMA foreign_keys = ON`, (and what are consequences of leaving it off). what tests /decision wdo we need to make regarding handling this?: `engine.delete(Band, id)` silently orphans every `BandMember.band`; the dangling id then surfaces later as a confusing `RecordNotFoundError` (or `None`?) at `Lazy` load time, far from the delete that caused it. Decide: enable the pragma in `Engine.__init__` (fail fast at delete with `ConstraintError`), or explicitly document/test the orphan behavior. Also decide what a `Lazy` load of a dangling FK should do.
- `engine.update` lacks the unpersisted-FK guard that `insert` has: `engine.update(Post, 1, band=unsaved_band)` flows through `adapt_value` → `value.id` → binds NULL — silent data corruption on a nullable FK column (and an opaque NOT NULL constraint error otherwise). Raise `UnpersistedRelationshipError` for parity with `insert`. Needs a test either way.


# Testing
All of these need test cases (or need it verified that a test already exists), either to capture and preserve existing behavior, or to define and enforce new behavior. Some of these are also more like "decisions to make", the codify with tests.
- `target=None` on `update`/`delete` silently no-ops (returns 0). Good as an accident guard, but it's invisible — a caller who *meant* "all rows" gets 0 and no signal, and a caller who passed `None` by bug (e.g. `row.id` that was never populated) also gets silence. Decide: keep-and-test the silent no-op, or raise on `None` and add an explicit opt-in for full-table update/delete (e.g. `target=sqlir.ALL`). Least-surprise leans toward raising. Yea we don't want to accidentally delete all, but then how to delete all?
- test types msgspec cannot encode raise at write time — confirm error message is clear and actionable
- test that everything works on when doing arbitrary adhoc model queries that select FK in as model relationships
- unit test for self join also
- find, if more than one result matched
- Test that non Fields greater than zero cannot be called id, even on row models. if you need to select an id in a row model, give it a different name, e.g. user_id.
- Test for cyclic data structures e.g. A -> B -> C -> A
- Test the foreign key may only be a union with None i.e. Optional BUT NOT with int or something else
- Investigate/ Test what Happens when specifying Model | int, should this raise??
- how handle unions of two valid types, e.g. int | str
  - Adapting would work fine, but conversion could be ambiguous
  - I think we should just raise on this
- Test can get using model with int as FK rather than Model to stop recursive loading
  e.g. int instead of Node in a Person_IntFK model
- Test you can have two field of same type,e.g. right_node, left_node
- Validate during Model compilation that FK related models are TableRow and now just Row.
- Test that you _may_ have a TableRow FK in a Row model.
- ensure that ID fields are always stored as integer affinity. i really think there are some landmines with tables having "text" in the the name. maybe all id columns should just be INT now that we do adapt/convert without relying on sql column types.
- Can a model use dataclass feature like "field"? should we make a custom one?
- Test that Row models DO NOT have __table_name__
    - Also why does find, and select etc, allow `Row` models currently? Once we do the `__select_query__` feature this makes more sense, but i don't think it could work right now.
- test that typechecker _knows_ that our models are immutable.
- test param overide works on both __select_query__ AND predicates
- test that one-to-one backrefs have the unique constraint.
- test param passing backref fields to on insert. (only happens in anti-pattern of trying to reinsert from a fetched model with backrefs, but it still concerns me)

- consider circular backref loops, right now we alleviated symptom by making backrefs not show up in repr, but shoulds we do something smarter? like cache orig value and point back to eliminate more queries when traversing? or alternatively, RAISE if someone navigate looplike? that seems harsh.

## testingmeta
- I want to instrument sqlite to log and profile queries.
- use the assert_type from typing to check type hints
  - Test types on engine.find/select/etc
- fix names / order of model_test.py, e.g. test_table_meta_... -> test_get_meta__....
- LLM based api fuzzying

## benchmarking
- automate a benchmark suite that outputs one large markdown results file, including all context needed to interpret the numbers
    - Add benchmarks for "theoretical max qps", to allow comparison of overhead as well spotting possible areas for improvement.
- Benchmark and test connection creation and closing
- benchmark model creation, field access, hashing, and memory footprint vs plain NamedTuple and dataclass baselines,
    - maybe resurect some of the old benchmarks for this.

# Next
- make rel template strings easily print as their resolved SQL for debugging
- migration interactive restore list too long. can you page restores or head results?
- update/delete by `RETURNING` — give `engine.update` a way to hand back the updated row(s) instead of just a change count (SQLite ≥ 3.35 `UPDATE ... RETURNING`). The htmx/form pattern of "update then re-render the row" currently costs an extra `find` round-trip and, worse, is a read-after-write that can race under concurrent writers; RETURNING makes it atomic. Maybe `engine.update(..., returning=True) -> list[R]` or a separate verb.



# Later
- Fix up example.ipynb to better structure relation and predicate separate from concept of `engine.select` and the `__select_query__` escape hatch.
- harmonize name rel, relation, pred and predicate, expr, binexpr, fieldexpr, and target in code and docs.
- harmonize use of storage type, vs sql type, vs column type, vs schema type in code and docs.
- harmonize and group exceptions better with a clearer hierarchy.
- exprs in update values???
- template string support for full queries, not just predicates? e.g. t'{MyModel:SELECT_FROM} WHERE {MyModel.field} = 1'
    - what about plucky or aggregation queries?
    - how to specify types when getting plucky. righ now this is all solved by just making a Row model. JUST forcing make a model leaves LESS things to remember. this seems like a will not implement.
    - This should come after the `__select_query__` feature, which will end up leveraging this.
- fix typing for rel combos like: `winners_today = t"date({MyModel.date}) == date('now')" & (MyModel.score > 99.95)  # ty:ignore[unsupported-operator]`
- Find and remove unused exceptions
- make sql*.py private also rel*.py probably...
- many-to-many style backrefs
- `engine.count(Model, target=None)` — `SELECT count(*)` with the same target/predicate machinery as `select`. Every internal CRUD list view needs "page N of M" / "showing X of Y"; today the workaround is a per-table `__select_query__` Row model or `len(fetchall())`, both silly for a count. Pairs naturally with the existing `limit`/`offset` pagination params.


## Shadow Swap Pattern for Zero-Downtime Table Rebuilds
Atomic table replacement that minimizes write-lock duration. Only the rename step holds the lock; all data loading and index building happen outside the critical section.

1. **Capture schema of T** — `SELECT sql FROM sqlite_schema` for the table, its indexes (`sql IS NOT NULL`), and triggers
2. **Create shadow table `T__new`** — rewrite captured `CREATE TABLE` replacing `T` → `T__new`
3. **Load data into `T__new`** — all ETL happens here; no production objects touched
4. **Build indexes on `T__new`** — rewrite each index: table ref `T` → `T__new`, name `idx` → `idx__new`
5. **Atomic swap** (only critical section):
   ```sql
   BEGIN IMMEDIATE;
   ALTER TABLE T RENAME TO T__old;
   ALTER TABLE T__new RENAME TO T;
   COMMIT;
   ```
   Rename updates FK/view references automatically (SQLite >= 3.26). Triggers and indexes move with `T__old`.
6. **Recreate triggers** — execute captured trigger SQL unchanged; they attach to the current `T`
7. **Optional cleanup** — `DROP TABLE T__old;` (or keep for rollback)

## Explain Model
I want to be able to explain model function. This would explain what the type annotation is., what the sqllite column type is, And why?. Like it would tell you that an INT is a built-in Python SQLite type., but a model is another model, And a list of a built-in type is stored as json., And then what it would attempt to pickle if there would be a pickle if it's unknown..
This would help distinguish between a list of model and a list of something else. 
This is cool cuz it blends casa no sql with SQL. We could probably even make a refactoring tool to switch between the two.
- Also want to explain querys from engine

## Migrations
- consider https://martinfowler.com/articles/evodb.html
- Generate ALTER instead of DROP/CREATE
- Generate SELECT-INTO for general alters
- pragma user_version and pragma application_id for something?

## Connection Management and Concurrency
- one connection per thread, like RoR AR
- Another options is to have two thread pools, one for reads and one for writes
  - This is more complex, but elimnates Busy errors
   https://kerkour.com/sqlite-for-servers
- SQLite supports concurrent reads but locks on writes.
  - Can be configured to block instead of raising an error on write contention
    https://sqlite.org/c3ref/busy_timeout.html
- https://kerkour.com/sqlite-for-servers
  - A 2024 guide to SQLite use and tuning on backend
  - `PRAGMA busy_timeout = 5000;` 5 seconds for app, 15 seconds for API
    - Allows waiting for a write lock to be released before raising an error

## Read about rich hickeys datomic
https://docs.datomic.com/datomic-overview.html

# One Day Maybe
- check for valid json on json fields
- support sql column defs with default values, e.g. `name: str = "default name"` and then have that be the default value for the column in the create table statement, and also have it be the default value for the field when creating a new instance of the model without specifying that field.
    - maybe we already have this via field
- Auto detect or provide a way to santize/escape LIKE params. e.g.  of % or _
- engine.exists (rails has relation.exists, e.g. Customer.where(first_name: "Ryan").exist
- scalar accessors, e.g. RoR AR's pick. get one value from one row and one. note: this is already built into apsw, engine.get
- RoR annotate (and sql comments so that later we can use it during observabilites)
- Ror-like scopes???
- FinalizedModel: disable lazy loading of fields, etc. e.g. guarantee immutability before passing to template etc. (needs better name i think)
- Scalar model type, e.g. RowMeta that is only allow to have a single field, and returns an unwrapped scalar value. (not sure this would work within the static typing.
- Raw execute escape hatch that returns untyped dicts instead of model instances. This would be for queries that don't fit the model paradigm at all, e.g. arbitrary joins, aggregations, CTEs, etc. This is basically the same as the current _query method, but with a better name and docs that make it clear this is an escape hatch for when the model-based API doesn't fit the use case. Can also be used for statements that mutate data also.


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
class XXX(TableRow):
    name: str
    place: str
    value: int

    _meta = Meta(unique_contraints=[("name", "place")])


engine.ensure_table_created(XXX)
engine.upsert(XXX(name="a", place="b", value=777))
engine.upsert(XXX(name="a", place="c", value=888))
```


## JSONB format
We will default to TEXT for simplicity and forward compat.
JSONB[T] would be a custom type that uses the json1 extension to store the data in a binary format, which is more efficient for large JSON objects and allows for indexing. e.g. it would be opt-in
## pickling type
make a type annotation like `Pickle[MyType]` that would automatically pickle and unpickle the field, and raise if it fails to pickle or unpickle. This fills a gap left by the expunging of custom adapt/convert pairs.


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
    move this to a benchmark script:
    ```python
    def insert_all[R: Row](self, Model: type[R], rows: Iterable[R]) -> None:
        """Insert multiple rows at once."""
        insert = get_meta(Model).insert
        assert insert is not None, "Insert statement should be defined for the model."
        with self.connection:
            self.connection.executemany(insert, rows)
    ```

## Cython implementation for faster model, must benchmark and actually know

# Probably Never
- a true cursor proxy and fetchoneonly helper/wrapper
  - cost penalty for get row benchmark (maybe test again later)
  - a pretty thin wrapper over native functionality

# Never, Will not Implement
- `TypedId` — a typed int subclass that encapsulated which Model an id belonged to
  - The engine API always takes the Model explicitly, so the id alone never needs to carry that information
  - Added complexity to model compilation (type-hint normalization) and to `TableRow.id` / `Model.Id` signatures for zero practical benefit
- switch to dict based queries, e.g. `engine.find(MyModel, {MyModel.name: "Bart"})`
  - Too much magic and increases boilerplate
  - CON to skipping: lose perfect "refactorability"
- `Any` for table models
  - it would automatically use the dynamic type adapter, but it would not know which converter to use to get it back to the original type
- Add passthrough for commit? e.g. engine.commit
  - just let the user use the existing connection, engine.connection.commit()?
  - Violates, only do what can't be done with the sqlite standard library
- `register_adapt_convert` / custom serialization registration API
  - Removed in favor of msgspec as a universal automatic fallback
  - msgspec handles all common types (bool, list, dict, Enum, UUID, datetime, date, time, set, frozenset, NamedTuple, dataclasses) with zero configuration
  - Adding back a registration layer re-introduces adapter/converter bookkeeping that msgspec makes unnecessary
  - For types msgspec can't handle, convert at the application layer before storing
- push lazy field making from cursor proxy to adaptconvert?
    - no, becuase the engine scope/ref isn't available in adapt/convert.
- `Annotated`-style backref metadata, e.g. `players: Annotated[Rows[Player], Player.squad] = backref()`
  - Motivation was to let the FK reference resolve lazily (via the deferred annotation) so the child could be defined *after* the parent, removing the child-first ordering constraint.
  - It works technically: in 3.14 the annotation isn't evaluated until `get_type_hints()`, so `Annotated[Rows[Child], Child.parent]` resolves fine at compile time even with forward refs, and the metadata is readable via `__metadata__`.
  - REJECTED because the ergonomics are worse: every backref becomes `field: Annotated[Rows[Child], Child.fk] = backref()` — the FK is now duplicated conceptually (annotation slot + empty `backref()` call), the `backref()` call carries no information, and the line is noisier than `field: Rows[Child] = backref(fk=Child.fk)`.
  - The same lazy-resolution goal is covered more cheaply by also allowing the fully-qualified string form `backref(fk="Child.parent")`. That keeps the clean `backref(fk=...)` surface, mirrors the typed `Child.parent` spelling, and already covers the awkward cases (self-referential or parent-first declarations) without moving metadata into `Annotated`.
- `engine.save(row)` - the proper ergonomic decision is "immediate mode, directly mutate db, no state outside of DB"
