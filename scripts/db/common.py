#!/usr/bin/env python3
"""Common helpers for local SQLite lifecycle scripts."""

from __future__ import annotations

import sqlite3
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
DEFAULT_DB_PATH = DATA_DIR / "phenixflow.sqlite"
SQL_DIR = Path(__file__).resolve().parent / "sql"


def resolve_db_path() -> Path:
    return Path(os.environ.get("PHENIX_DB_PATH", str(DEFAULT_DB_PATH))).expanduser().resolve()


def ensure_data_dir(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def execute_sql_file(conn: sqlite3.Connection, filename: str) -> None:
    sql_path = SQL_DIR / filename
    sql_text = sql_path.read_text(encoding="utf-8")
    conn.executescript(sql_text)


def create_db_file(db_path: Path) -> None:
    ensure_data_dir(db_path)
    conn = connect(db_path)
    conn.close()


def create_tables(db_path: Path) -> None:
    ensure_data_dir(db_path)
    conn = connect(db_path)
    try:
        execute_sql_file(conn, "001_schema.sql")
        execute_sql_file(conn, "002_indexes.sql")
        conn.commit()
    finally:
        conn.close()


def seed_tables(db_path: Path) -> None:
    ensure_data_dir(db_path)
    conn = connect(db_path)
    try:
        execute_sql_file(conn, "003_seed.sql")
        conn.commit()
    finally:
        conn.close()


def init_database(db_path: Path) -> None:
    create_db_file(db_path)
    create_tables(db_path)
    seed_tables(db_path)
