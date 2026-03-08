#!/usr/bin/env bash
set -euo pipefail

BREW_PREFIX="$(brew --prefix)"
CH_LIB_SRC="${CH_LIB_SRC:-$BREW_PREFIX/var/lib/clickhouse}"
CH_LOG_SRC="${CH_LOG_SRC:-$BREW_PREFIX/var/log/clickhouse-server}"
CH_VOLUME="${CH_VOLUME:-/Volumes/Phenix4TB}"
CH_ROOT="${CH_ROOT:-$CH_VOLUME/clickhouse}"
CH_LIB_DST="${CH_LIB_DST:-$CH_ROOT/lib}"
CH_LOG_DST="${CH_LOG_DST:-$CH_ROOT/log}"
START_AFTER_MOVE="${START_AFTER_MOVE:-1}"

timestamp() {
  date +"%Y%m%d-%H%M%S"
}

if [[ ! -d "$CH_VOLUME" ]]; then
  echo "Volume not mounted: $CH_VOLUME"
  exit 1
fi

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse binary not found. Install first with scripts/clickhouse/install-clickhouse.sh"
  exit 1
fi

mkdir -p "$CH_ROOT" "$CH_LIB_DST" "$CH_LOG_DST"

echo "Stopping clickhouse service..."
brew services stop clickhouse >/dev/null 2>&1 || true

echo "Syncing ClickHouse data dir to external volume..."
if [[ -e "$CH_LIB_SRC" && ! -L "$CH_LIB_SRC" ]]; then
  rsync -a --delete "$CH_LIB_SRC"/ "$CH_LIB_DST"/
  mv "$CH_LIB_SRC" "${CH_LIB_SRC}.bak.$(timestamp)"
fi
if [[ ! -L "$CH_LIB_SRC" ]]; then
  ln -s "$CH_LIB_DST" "$CH_LIB_SRC"
fi

echo "Syncing ClickHouse log dir to external volume..."
if [[ -e "$CH_LOG_SRC" && ! -L "$CH_LOG_SRC" ]]; then
  rsync -a --delete "$CH_LOG_SRC"/ "$CH_LOG_DST"/
  mv "$CH_LOG_SRC" "${CH_LOG_SRC}.bak.$(timestamp)"
fi
if [[ ! -L "$CH_LOG_SRC" ]]; then
  ln -s "$CH_LOG_DST" "$CH_LOG_SRC"
fi

if [[ "$START_AFTER_MOVE" == "1" ]]; then
  echo "Starting clickhouse service..."
  brew services start clickhouse >/dev/null 2>&1 || true
fi

echo "Done."
echo "Data path: $CH_LIB_SRC -> $CH_LIB_DST"
echo "Log path : $CH_LOG_SRC -> $CH_LOG_DST"
