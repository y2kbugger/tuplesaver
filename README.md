# Tuple Saver
Tuple Saver persists and retrieves native python NamedTuples to SQLite3.

## Quick Start
Add the library to your poetry project:

    poetry add git+https://github.com/y2kbugger/tuplesaver.git

or if you use something else to manage you .venv use that or pip instead.

    pip install git+https://github.com/y2kbugger/tuplesaver.git


See the `example.ipynb` notebook for usage examples.


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


# Notes
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

then activate your terminal and run the tests via vscode or the cli:

    $ pytest

There is a test Task setup in vscode. You maybe wish to add a keybinding to run it, e.g.

    [Ctrl]+[Shift]+G



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
