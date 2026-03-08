# ClickHouse On External 4TB (Phenix4TB)

This project can run ClickHouse with data + logs stored on:

- `/Volumes/Phenix4TB/clickhouse/lib`
- `/Volumes/Phenix4TB/clickhouse/log`

## 1) Install and start ClickHouse

```bash
bash scripts/clickhouse/install-clickhouse.sh
bash scripts/clickhouse/start-clickhouse.sh
```

## 2) Check server status

```bash
bash scripts/clickhouse/status-clickhouse.sh
```

## 3) Initialize schema

```bash
bash scripts/clickhouse/init-options-schema.sh
```

## 4) Import from SQLite

Full import:

```bash
SQLITE_DB=/Users/pawanagarwal/github/phenixflow/data/options_storage/curated/curated/sqlite/options_trade_quote.sqlite \
bash scripts/clickhouse/import-from-sqlite.sh
```

Single day import:

```bash
DAY=2026-03-04 \
SQLITE_DB=/Users/pawanagarwal/github/phenixflow/data/options_storage/curated/curated/sqlite/options_trade_quote.sqlite \
bash scripts/clickhouse/import-from-sqlite.sh
```
