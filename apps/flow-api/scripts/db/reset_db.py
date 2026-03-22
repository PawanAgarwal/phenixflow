#!/usr/bin/env python3
"""Drop local SQLite database file and recreate schema + seed data."""

from __future__ import annotations

from common import init_database, resolve_db_path


def main() -> int:
    db_path = resolve_db_path()
    if db_path.exists():
        db_path.unlink()
    init_database(db_path)
    print(f"Database reset and initialized: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
