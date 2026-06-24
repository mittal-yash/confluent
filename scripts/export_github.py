"""One-off: copy gridsentinel/ to ../gridsentinel-github/ for public release."""
from __future__ import annotations

import shutil
from pathlib import Path

SRC = Path(__file__).resolve().parents[1]
DST = Path(__file__).resolve().parents[2] / "gridsentinel-github"

SKIP_DIRS = {"__pycache__", ".venv", "venv", "docs", "promo", "agents"}
SKIP_FILES = {
    ".env",
    "vertex-sa.json",
    "vertex-sa.minified.txt",
    "scripts/_build_full_schema.py",
    "scripts/_build_full_pipeline_sql.py",
    "scripts/_lineage_labeled.py",
    "scripts/_enhance_lineage.py",
    "scripts/_stitch_lineage.py",
    "scripts/_push_workflow.py",
    "scripts/_dropidx.py",
    "scripts/_dimcheck.py",
    "scripts/fix_flink_vertex_embed.py",
    "scripts/apply_cursor_ollama.py",
    "scripts/export_github.py",
    "BUSINESS_CASE.md",
}


def should_skip(rel: Path) -> bool:
    parts = rel.parts
    if parts and parts[0] in SKIP_DIRS:
        return True
    if rel.name == ".env":
        return True
    return rel.as_posix() in SKIP_FILES or rel.name.startswith(".env") and rel.name != ".env.example"


def main() -> None:
    if DST.exists():
        try:
            shutil.rmtree(DST)
        except OSError:
            pass
    DST.mkdir(parents=True, exist_ok=True)

    for path in SRC.rglob("*"):
        rel = path.relative_to(SRC)
        if should_skip(rel):
            continue
        if path.is_dir():
            continue
        dest = DST / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)

    print(f"Copied {SRC} -> {DST}")


if __name__ == "__main__":
    main()
