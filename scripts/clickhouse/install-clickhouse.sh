#!/usr/bin/env bash
set -euo pipefail

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required but not found."
  exit 1
fi

if command -v clickhouse >/dev/null 2>&1; then
  echo "ClickHouse already installed."
else
  echo "Installing ClickHouse via Homebrew..."
  brew install clickhouse
fi

ACTION="${1:-status}"
case "$ACTION" in
  status)
    clickhouse --version || true
    ;;
  start)
    bash "$(cd "$(dirname "$0")" && pwd)/start-clickhouse.sh"
    ;;
  stop)
    bash "$(cd "$(dirname "$0")" && pwd)/stop-clickhouse.sh"
    ;;
  restart)
    bash "$(cd "$(dirname "$0")" && pwd)/stop-clickhouse.sh" || true
    bash "$(cd "$(dirname "$0")" && pwd)/start-clickhouse.sh"
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|status]"
    exit 1
    ;;
esac
