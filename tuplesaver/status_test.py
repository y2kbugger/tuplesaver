"""Tests for compact status output (status_lines + format_status)."""

from __future__ import annotations

from .migrate import CheckResult, TableSchema, format_status


def _ts(table_name: str, expected: str, actual: str | None, *, model_name: str | None = None) -> TableSchema:
    """Shorthand for building a TableSchema."""
    return TableSchema(
        table_name=table_name,
        model_name=model_name or table_name,
        expected_sql=expected,
        actual_sql=actual,
    )


# ── status_lines ─────────────────────────────────────────────────────


def test_status_lines__current_no_models():
    """Empty DB, no models, no files → no lines at all."""
    result = CheckResult()
    assert result.status_lines() == []


def test_status_lines__current_with_applied():
    """All migrations applied, schema matches → all hidden."""
    result = CheckResult(
        all_filenames=["001.create_user.sql", "002.add_email.sql"],
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY)", "CREATE TABLE User (id INTEGER PRIMARY KEY)")},
    )
    assert result.status_lines() == [
        (False, " ", " ", " ", "001.create_user.sql"),
        (False, " ", " ", " ", "002.add_email.sql"),
        (False, " ", " ", " ", "User"),
    ]


def test_status_lines__drift_untracked_model():
    """Fresh DB with model, no migrations → model shows U (untracked)."""
    result = CheckResult(
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT NOT NULL)", None)},
    )
    assert result.status_lines() == [
        (True, " ", " ", "U", "User"),
    ]


def test_status_lines__pending_local():
    """Migration on disk not applied locally → P in local column."""
    result = CheckResult(
        pending=["001.create_user.sql"],
        ref_pending=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert result.status_lines() == [
        (True, "P", "P", " ", "001.create_user.sql"),
    ]


def test_status_lines__pending_ref_only():
    """Migration applied locally but not in ref → P in ref column only."""
    result = CheckResult(
        ref_pending=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert result.status_lines() == [
        (True, "P", " ", " ", "001.create_user.sql"),
    ]


def test_status_lines__conflicted():
    """Migration differs from ref → C in ref column."""
    result = CheckResult(
        conflicted=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert result.status_lines() == [
        (True, "C", " ", " ", "001.create_user.sql"),
    ]


def test_status_lines__diverged():
    """Migration differs from local DB → D in local column."""
    result = CheckResult(
        divergent=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert result.status_lines() == [
        (True, " ", "D", " ", "001.create_user.sql"),
    ]


def test_status_lines__conflicted_and_diverged():
    """Same file conflicted (ref) and diverged (local)."""
    result = CheckResult(
        conflicted=["001.create_user.sql"],
        divergent=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert result.status_lines() == [
        (True, "C", "D", " ", "001.create_user.sql"),
    ]


def test_status_lines__mixed_scenario():
    """Multiple files and models in various states."""
    result = CheckResult(
        pending=["002.add_email.sql"],
        ref_pending=["002.add_email.sql"],
        conflicted=["001.create_user.sql"],
        all_filenames=["001.create_user.sql", "002.add_email.sql"],
        schema={
            "User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT)", "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT)"),
            "Post": _ts("Post", "CREATE TABLE Post (id INTEGER PRIMARY KEY, title TEXT)", None),
        },
    )
    assert result.status_lines() == [
        (True, "C", " ", " ", "001.create_user.sql"),
        (True, "P", "P", " ", "002.add_email.sql"),
        (True, " ", " ", "U", "Post"),
        (False, " ", " ", " ", "User"),
    ]


def test_status_lines__greenfield_all_pending():
    """No ref DB at all → all migrations are ref-pending. No special case."""
    result = CheckResult(
        pending=["001.create_user.sql", "002.add_post.sql"],
        ref_pending=["001.create_user.sql", "002.add_post.sql"],
        all_filenames=["001.create_user.sql", "002.add_post.sql"],
        schema={
            "User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY)", None),
            "Post": _ts("Post", "CREATE TABLE Post (id INTEGER PRIMARY KEY)", None),
        },
    )
    assert result.status_lines() == [
        (True, "P", "P", " ", "001.create_user.sql"),
        (True, "P", "P", " ", "002.add_post.sql"),
        (True, " ", " ", "U", "Post"),
        (True, " ", " ", "U", "User"),
    ]


def test_status_lines__model_modified():
    """Model schema changed → M in model column."""
    result = CheckResult(
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT, email TEXT)", "CREATE TABLE User (id INTEGER PRIMARY KEY, name TEXT)")},
    )
    assert result.status_lines() == [
        (True, " ", " ", "M", "User"),
    ]


def test_status_lines__missing_files():
    """Missing file from ref (conflicted_missing) and local (divergent_missing)."""
    result = CheckResult(
        conflicted_missing=["001.create_user.sql"],
        divergent_missing=["002.add_email.sql"],
        all_filenames=["001.create_user.sql", "002.add_email.sql"],
    )
    assert result.status_lines() == [
        (True, "C", " ", " ", "001.create_user.sql"),
        (True, " ", "D", " ", "002.add_email.sql"),
    ]


# ── canonical name priority ──────────────────────────────────────────


def test_status_lines__name_priority_ref_wins():
    """When ref, disk, and local all have different names for the same number,
    the ref name is shown (ref > disk > local)."""
    # Migration #1: ref="001.init.sql", disk="001.setup.sql", local="001.bootstrap.sql"
    # The ref name should win.
    result = CheckResult(
        conflicted=["001.setup.sql"],  # disk name, content differs from ref
        divergent=["001.setup.sql"],  # disk name, content differs from local
        all_filenames=["001.init.sql"],  # canonical name is the ref name
    )
    assert result.status_lines() == [
        (True, "C", "D", " ", "001.init.sql"),
    ]


def test_status_lines__name_priority_disk_over_local():
    """When disk and local have different names (no ref), disk name is shown."""
    # Migration #1: disk="001.setup.sql", local="001.bootstrap.sql"
    result = CheckResult(
        divergent=["001.setup.sql"],
        all_filenames=["001.setup.sql"],  # canonical name is the disk name
    )
    assert result.status_lines() == [
        (True, " ", "D", " ", "001.setup.sql"),
    ]


def test_status_lines__name_priority_local_only():
    """When only local DB has the migration (file deleted), local name is shown."""
    # Migration #1 only in local DB (file missing from disk and ref)
    result = CheckResult(
        divergent_missing=["001.bootstrap.sql"],
        all_filenames=["001.bootstrap.sql"],
    )
    assert result.status_lines() == [
        (True, " ", "D", " ", "001.bootstrap.sql"),
    ]


# ── format_status ────────────────────────────────────────────────────


def test_format_status__current():
    """All up-to-date → simple message."""
    result = CheckResult()
    assert format_status(result) == "Current: schema is up to date"


def test_format_status__current_with_hidden():
    """Applied files exist but all hidden → still shows current message."""
    result = CheckResult(
        all_filenames=["001.create_user.sql"],
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY)", "CREATE TABLE User (id INTEGER PRIMARY KEY)")},
    )
    assert format_status(result) == "Current: schema is up to date"


def test_format_status__pending_no_color():
    """Pending migration, no color → plain indicators."""
    result = CheckResult(
        pending=["001.create_user.sql"],
        ref_pending=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
    )
    assert format_status(result, color=False) == 'State: PENDING\nPP  001.create_user.sql'


def test_format_status__drift_no_color():
    """Untracked model, no color → plain U indicator."""
    result = CheckResult(
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY)", None)},
    )
    assert format_status(result, color=False) == "State: DRIFT\n  U User"


def test_format_status__errors_shown_first():
    """Errors appear before status lines."""
    result = CheckResult(
        errors=["Duplicate migration number 1: 001.a.sql, 001.b.sql"],
        pending=["001.a.sql"],
        all_filenames=["001.a.sql"],
    )
    out = format_status(result, color=False)
    lines = out.split("\n")
    assert lines[1] == "E Duplicate migration number 1: 001.a.sql, 001.b.sql"


def test_format_status__with_color():
    """Colored output uses ANSI escape sequences."""
    import colors as clr

    result = CheckResult(
        pending=["001.create_user.sql"],
        all_filenames=["001.create_user.sql"],
        schema={"User": _ts("User", "CREATE TABLE User (id INTEGER PRIMARY KEY)", None)},
    )
    out = format_status(result, color=True)
    # Should contain ANSI escape sequences
    assert "\x1b[" in out
    # The indicators should be colored
    assert clr.yellow("P") in out
    assert clr.red("U") in out
