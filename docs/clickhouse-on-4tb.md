# ClickHouse On External Volume

This project can run ClickHouse with data + logs stored on:

- `$CH_VOLUME/clickhouse/lib`
- `$CH_VOLUME/clickhouse/log`

By default the scripts expect:

- `CH_VOLUME_NAME=Phenix4TB`
- `CH_VOLUME=/Volumes/$CH_VOLUME_NAME`

You can override either env var when moving to another machine or using a differently named external drive.

## 1) Bootstrap a new machine

```bash
bash scripts/clickhouse/bootstrap-clickhouse-host.sh
```

This will:

- ensure ClickHouse is installed,
- prepare the external volume layout,
- generate a runtime config with dynamic memory/cache sizing,
- start ClickHouse,
- print server status.

Set `INIT_SCHEMA=1` if you want schema initialization included:

```bash
INIT_SCHEMA=1 bash scripts/clickhouse/bootstrap-clickhouse-host.sh
```

## 2) Prepare the external drive layout only

```bash
bash scripts/clickhouse/prepare-external-volume.sh
```

This creates a portable ClickHouse layout on the mounted volume so you can plug the drive into another machine and reuse the same structure.

## 3) Install and start manually

```bash
bash scripts/clickhouse/install-clickhouse.sh
bash scripts/clickhouse/start-clickhouse.sh
```

The runtime config is generated at `$CH_ROOT/run/generated-config.xml`. By default memory settings are computed from host RAM:

- `CH_MEMORY_LIMIT_PERCENT=70`
- `CH_MARK_CACHE_PERCENT_OF_LIMIT=18`
- `CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT=9`

You can override them per host if needed.

## 4) Check server status

```bash
bash scripts/clickhouse/status-clickhouse.sh
```

## 5) Install a macOS LaunchDaemon (starts before login)

Use this if the machine should bring ClickHouse up on boot, before any user logs in.

```bash
bash scripts/clickhouse/install-launchdaemon.sh
```

What it does:

- installs a LaunchDaemon at `/Library/LaunchDaemons/com.phenixflow.clickhouse.plist`,
- installs a self-contained launcher under `/Library/Application Support/PhenixFlow/clickhouse/`,
- copies `config/clickhouse/users.xml` into that system directory,
- disables any older per-user `LaunchAgent` with the same label,
- bootstraps the new system daemon immediately.

Defaults:

- Launch label: `com.phenixflow.clickhouse`
- Run user: the current console user
- Run group: `staff`
- System install dir: `/Library/Application Support/PhenixFlow/clickhouse`

Useful overrides:

- `CLICKHOUSE_DAEMON_USER`
- `CLICKHOUSE_DAEMON_GROUP`
- `CLICKHOUSE_SYSTEM_DIR`
- `CLICKHOUSE_DAEMON_LABEL`

Verify the daemon:

```bash
launchctl print system/com.phenixflow.clickhouse | sed -n '1,40p'
clickhouse client --host 127.0.0.1 --port 9000 --query "SELECT version()"
```

Back up the installed system files:

```bash
bash scripts/clickhouse/export-launchdaemon-backup.sh
```

The backup script writes a timestamped copy under `$HOME/.config/phenixflow-clickhouse/backups/`.

## 6) Initialize schema

```bash
bash scripts/clickhouse/init-options-schema.sh
```

## 7) Import from SQLite

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
