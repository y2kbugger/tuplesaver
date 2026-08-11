#!/usr/bin/env python
"""Keep the agent plugin's bundled docs in sync with the source of truth.

Source of truth:
- ``API.md``        -> ``agent-plugin/skills/sqlir/API.md`` (verbatim copy)
- ``MIGRATIONS.md`` -> ``agent-plugin/skills/sqlir/MIGRATIONS.md`` (verbatim copy)
- ``example.ipynb`` -> ``agent-plugin/skills/sqlir/example.py`` (jupytext
  "percent" format, code only, no outputs)

This script is idempotent: running it twice produces identical output. It is
wired into ``.pre-commit-config.yaml`` so the bundled copies can never drift
from the originals (pre-commit fails the commit if a regenerated file differs
from what is staged).

Regenerate manually with::

    python scripts/sync_plugin.py
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_DIR = REPO_ROOT / "agent-plugin" / "skills" / "sqlir"

API_SRC = REPO_ROOT / "API.md"
API_DST = SKILL_DIR / "API.md"

MIGRATIONS_SRC = REPO_ROOT / "MIGRATIONS.md"
MIGRATIONS_DST = SKILL_DIR / "MIGRATIONS.md"

NOTEBOOK_SRC = REPO_ROOT / "example.ipynb"
EXAMPLE_DST = SKILL_DIR / "example.py"

GENERATED_BANNER = (
    "AUTO-GENERATED — DO NOT EDIT. Regenerate with: python scripts/sync_plugin.py"
)


def notebook_to_percent(notebook_path: Path) -> str:
    """Convert a notebook to a jupytext 'percent' (.py) script, sans outputs."""
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))

    blocks: list[str] = [f"# {GENERATED_BANNER}", f"# Source: {notebook_path.name}", ""]

    for cell in notebook.get("cells", []):
        source = "".join(cell.get("source", []))
        cell_type = cell.get("cell_type")

        if cell_type == "markdown":
            blocks.append("# %% [markdown]")
            for line in source.splitlines() or [""]:
                blocks.append(f"# {line}".rstrip())
        elif cell_type == "code":
            blocks.append("# %%")
            blocks.append(source.rstrip("\n"))
        else:
            continue

        blocks.append("")

    return "\n".join(blocks).rstrip("\n") + "\n"


def copy_api(api_src: Path, api_dst: Path) -> None:
    body = api_src.read_text(encoding="utf-8")
    header = f"<!-- {GENERATED_BANNER} Source: {api_src.name} -->\n\n"
    api_dst.write_text(header + body, encoding="utf-8")


def main() -> None:
    SKILL_DIR.mkdir(parents=True, exist_ok=True)
    copy_api(API_SRC, API_DST)
    copy_api(MIGRATIONS_SRC, MIGRATIONS_DST)
    EXAMPLE_DST.write_text(notebook_to_percent(NOTEBOOK_SRC), encoding="utf-8")
    print(f"synced -> {API_DST.relative_to(REPO_ROOT)}")
    print(f"synced -> {MIGRATIONS_DST.relative_to(REPO_ROOT)}")
    print(f"synced -> {EXAMPLE_DST.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
