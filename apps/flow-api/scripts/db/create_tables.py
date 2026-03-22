#!/usr/bin/env python3
"""Create all SQLite tables for Phenix local development."""

from __future__ import annotations

from common import create_tables, resolve_db_path


def main() -> int:
    db_path = resolve_db_path()
    create_tables(db_path)
    print(f"Created/updated tables in: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
