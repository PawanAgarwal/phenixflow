#!/usr/bin/env python3
"""Create an empty local SQLite database file."""

from __future__ import annotations

from common import create_db_file, resolve_db_path


def main() -> int:
    db_path = resolve_db_path()
    create_db_file(db_path)
    print(f"Created database file: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
