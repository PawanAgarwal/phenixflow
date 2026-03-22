#!/usr/bin/env python3
"""Initialize seed/default rows in local SQLite tables."""

from __future__ import annotations

from common import resolve_db_path, seed_tables


def main() -> int:
    db_path = resolve_db_path()
    seed_tables(db_path)
    print(f"Initialized seed data in: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
