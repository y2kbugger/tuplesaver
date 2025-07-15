# Tuple Saver
Tuple Saver persists and retrieves python `NamedTuple`s to SQLite3.

## Quick Start
Add the library to your poetry project:

    poetry add git+https://github.com/y2kbugger/tuplesaver.git

or if you use something else to manage you `.venv` use that or pip instead.

    pip install git+https://github.com/y2kbugger/tuplesaver.git


See the `example.ipynb` notebook for usage examples.


## Target Applications Constraints
- Single node `python` + `sqlite`
- ONLY the App will access the db, api for external access

This is viable for many web apps, including a large fraction internal enterprise apps.

## Why
`python` + `sqlite` provides tangible benefits.
- Simplified application infrastructure, no need for a separate db server
- Latency of persistance becomes negligible
  - eliminating unpersisted state becomes simple
- True consistancy between `devlocal`, `qa`, and `production` environments.
  - Migrations become easier to automate and test.
- If noone else accesses your db directly
  - you maintain the freedom to refactor the db schema

## Library Goals
- Correct static type hinting on both sides of persistance
- Improve refactorability
  - Eliminate stringly referenced columns
  - Migrations distilled to thier essential complexity

## Design Principles
- truely simple, not seemingly simple
- minimize library specific knowledge requirements
  - use standard types, type hints, and features
  - never wrap native functionality
- minimize boilerplate
- between "more magic" and "more boilerplate", choose "more boilerplate"
- principle of least surprise
- library specific knowlege should be self revealing
  - e.g. through attributes, type hints, or parameters
- no dependencies

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
then simulate what precommit would do (to warm up cpu, cause noise on sytem, etc),
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
