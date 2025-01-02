# Development
Use poetry to install the dependencies:

    $ poetry install --with dev

Install pre-commit hooks:

    $ pre-commit install --hook-type pre-commit --hook-type pre-push

then activate and run the tests via vscode or the cli:

    $ poetry shell
    $ pytest

There is a test Task setup in vscode that, you can add a keybinding to run it, e.g.

    [Ctrl]+[Shift]+G

Interactively code with the python API in the `example.ipynb` notebook. This should include many examples.
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
- get row
- update row
- delete row

# Bugs

# Backlog
- query
  - limit row count
  - order by
  - filter on field
- upsert
- pull in object from other table as field
- verify columns of created tables with option to delete table if mis-matched or fail instead, e.g. force=True
- Optional columns types

## Engineering
- benchmark performance
- try compiled queries for performance
