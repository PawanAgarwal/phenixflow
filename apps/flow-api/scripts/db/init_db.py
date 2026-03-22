#!/usr/bin/env python3
"""Create database file, schema, and seed data in one command."""

from __future__ import annotations

from common import init_database, resolve_db_path


def main() -> int:
    db_path = resolve_db_path()
    init_database(db_path)
    print(f"Database initialized: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
