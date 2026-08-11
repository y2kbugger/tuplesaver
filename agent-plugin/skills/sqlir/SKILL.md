---
name: sqlir
description: >-
  Use sqlir to persist immutable dataclass-style models to SQLite via apsw.
  USE WHEN: defining `TableRow` / `Row` models, doing CRUD with an `Engine`
  (insert/find/select/update/delete/query), building relation predicates
  (`Model.field == value`, FK traversal, PEP 750 t-string SQL fragments),
  mapping Python types to SQLite storage, or running migrations with the
  `sqlir-migrate` CLI. Python 3.14+ only (PEP 649 lazy annotations).
---

# sqlir

sqlir persists immutable, frozen-dataclass models to SQLite using
[apsw](https://rogerbinns.github.io/apsw/) and [msgspec](https://jcristharif.com/msgspec/).
It is single-node / single-process, has no runtime dependencies beyond `apsw`
and `msgspec`, and requires **Python ≥ 3.14**.

## How to use this skill

Three reference files are bundled alongside this skill. Read them on demand:

- [API.md](API.md) — the authoritative API reference: model concepts
  (`TableRow` / `Row`), the `Engine` CRUD surface, relation predicates and
  t-strings, the full Python→SQLite type mapping, and web-framework error
  handling. Start here for signatures and semantics.
- [MIGRATIONS.md](MIGRATIONS.md) — the migration system: file layout, states
  and their transitions, `pyproject.toml` config, and the `sqlir-migrate` CLI.
  For CLI subcommands and flags, the source of truth is `sqlir-migrate --help`
  and `sqlir-migrate <COMMAND> --help`.
- [example.py](example.py) — a runnable, end-to-end tour in jupytext "percent"
  format (each `# %%` marks a cell). Covers models, CRUD, relations/joins,
  t-string predicates, advanced type persistence (datetime, Decimal, Enum,
  bytes, JSON fallbacks), raw-SQL escape hatches, and performance scenarios.
  Read this for concrete, copy-pasteable usage patterns.

> Prioritize reading `API.md` and `example.py` provided here, they are a very concentrated source of best practices. It should be a RARE occurrence that you need to venture into source code, for instance if you are troubleshooting a suspected implementation bug/quirk/edge-case.

## Core rules to honor when writing sqlir code

- **Models are immutable.** Use `dataclasses.replace(obj, field=value)` to make
  a modified copy before re-saving; never mutate in place.
- **`TableRow` field 0 is `id: int | None`** and is provided automatically — do
  not declare it yourself.
- **FK fields must be `Model | None`** (e.g. `manager: User | None`). Traversal
  (`User.manager.name`) lowers to EXISTS semi-joins.
- **`Row`** (no `id`) is for ad-hoc / view-shaped query results only; it cannot
  be used with `find` / `update` / `delete`.
- **Do not add `from __future__ import annotations`** — 3.14 PEP 649 lazy
  annotations are the baseline.
- **Table-model names may not contain `_`** (underscore is reserved).
- Embed model field references in raw SQL via PEP 750 t-strings
  (`t"{User.name} LIKE 'J%'"`), not plain f-strings, so they stay
  refactor-safe.
- remember Cursors are iterable, so avoid `fetchall()`; whenever possible, just iterate directly (`for user in engine.select(User, ...)`).
- when using looking up by id in find/update/delete, just pass the id as an int directly (`e.find(User, id)`), you do not need a predicate like `User.id == id` (though that also works but is NOT idiomatic).
- Dont use e._query, instead bind raw SQL to `Row` model definitions via M.__select_query__

## sqlir is UNDER ACTIVE DEVELOPMENT
- Please, summarize quirks/bugs that you had to work around after finishing your task, and share them with the maintainers.

> These files are auto-generated copies kept in sync with the repo's
> `API.md`, `MIGRATIONS.md`, and `example.ipynb` by `scripts/sync_plugin.py`
> (run via a pre-commit hook). Do not edit them by hand.
