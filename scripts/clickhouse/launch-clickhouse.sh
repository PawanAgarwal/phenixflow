#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATH="/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"

CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-/opt/homebrew/bin/clickhouse}"
CH_VOLUME_BASE="${CH_VOLUME_BASE:-/Volumes}"
CH_VOLUME_NAME="${CH_VOLUME_NAME:-Phenix4TB}"
CH_VOLUME="${CH_VOLUME:-$CH_VOLUME_BASE/$CH_VOLUME_NAME}"
CH_ROOT="${CH_ROOT:-$CH_VOLUME/clickhouse}"

CH_LIB_DIR="${CH_LIB_DIR:-$CH_ROOT/lib}"
CH_LOG_DIR="${CH_LOG_DIR:-$CH_ROOT/log}"
CH_TMP_DIR="${CH_TMP_DIR:-$CH_ROOT/tmp}"
CH_USER_FILES_DIR="${CH_USER_FILES_DIR:-$CH_ROOT/user_files}"
CH_FORMAT_SCHEMAS_DIR="${CH_FORMAT_SCHEMAS_DIR:-$CH_ROOT/format_schemas}"
CH_RUN_DIR="${CH_RUN_DIR:-$CH_ROOT/run}"

CONFIG_DIR="${CONFIG_DIR:-$SCRIPT_DIR}"
USERS_CONFIG="${USERS_CONFIG:-$CONFIG_DIR/users.xml}"
CONFIG_FILE="${CONFIG_FILE:-$CONFIG_DIR/generated-config.xml}"
PID_FILE="${PID_FILE:-$CONFIG_DIR/clickhouse-server.pid}"

CH_MEMORY_LIMIT_PERCENT="${CH_MEMORY_LIMIT_PERCENT:-70}"
CH_MARK_CACHE_PERCENT_OF_LIMIT="${CH_MARK_CACHE_PERCENT_OF_LIMIT:-18}"
CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT="${CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT:-9}"

log() {
  printf '%s %s\n' "$(/bin/date '+%Y-%m-%d %H:%M:%S')" "$*"
}

get_total_memory_bytes() {
  /usr/sbin/sysctl -n hw.memsize
}

percent_of() {
  local total="$1"
  local percent="$2"
  /usr/bin/awk -v total="$total" -v percent="$percent" 'BEGIN { printf "%.0f\n", (total * percent) / 100 }'
}

percent_to_ratio() {
  local percent="$1"
  /usr/bin/awk -v percent="$percent" 'BEGIN { printf "%.6f\n", percent / 100 }'
}

if [[ ! -x "$CLICKHOUSE_BIN" ]]; then
  log "ClickHouse binary not found at $CLICKHOUSE_BIN"
  exit 78
fi

if [[ ! -d "$CH_VOLUME" ]]; then
  log "ClickHouse volume is not mounted: $CH_VOLUME"
  exit 75
fi

if [[ ! -f "$USERS_CONFIG" ]]; then
  log "ClickHouse users config is missing: $USERS_CONFIG"
  exit 78
fi

/bin/mkdir -p \
  "$CONFIG_DIR" \
  "$CH_ROOT" \
  "$CH_LIB_DIR" \
  "$CH_LOG_DIR" \
  "$CH_TMP_DIR" \
  "$CH_USER_FILES_DIR" \
  "$CH_FORMAT_SCHEMAS_DIR" \
  "$CH_RUN_DIR"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$({ /bin/cat "$PID_FILE" 2>/dev/null || true; } | /usr/bin/tr -d '\n')"
  if [[ -n "$existing_pid" ]] && /bin/kill -0 "$existing_pid" >/dev/null 2>&1; then
    log "ClickHouse already running with pid=$existing_pid"
    exit 0
  fi
  /bin/rm -f "$PID_FILE"
fi

total_memory_bytes="$(get_total_memory_bytes)"
max_server_memory_usage="$(percent_of "$total_memory_bytes" "$CH_MEMORY_LIMIT_PERCENT")"
mark_cache_size="$(percent_of "$max_server_memory_usage" "$CH_MARK_CACHE_PERCENT_OF_LIMIT")"
uncompressed_cache_size="$(percent_of "$max_server_memory_usage" "$CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT")"
max_server_memory_ratio="$(percent_to_ratio "$CH_MEMORY_LIMIT_PERCENT")"

/bin/cat > "$CONFIG_FILE" <<EOF
<clickhouse>
  <logger>
    <level>information</level>
    <log>${CH_LOG_DIR}/clickhouse-server.log</log>
    <errorlog>${CH_LOG_DIR}/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>3</count>
    <console>true</console>
  </logger>

  <http_port>8123</http_port>
  <tcp_port>9000</tcp_port>
  <interserver_http_port>9009</interserver_http_port>
  <listen_host>127.0.0.1</listen_host>

  <max_connections>4096</max_connections>
  <keep_alive_timeout>3</keep_alive_timeout>
  <max_concurrent_queries>100</max_concurrent_queries>
  <max_server_memory_usage>${max_server_memory_usage}</max_server_memory_usage>
  <max_server_memory_usage_to_ram_ratio>${max_server_memory_ratio}</max_server_memory_usage_to_ram_ratio>
  <uncompressed_cache_size>${uncompressed_cache_size}</uncompressed_cache_size>
  <mark_cache_size>${mark_cache_size}</mark_cache_size>

  <path>${CH_LIB_DIR}/</path>
  <tmp_path>${CH_TMP_DIR}/</tmp_path>
  <user_files_path>${CH_USER_FILES_DIR}/</user_files_path>
  <format_schema_path>${CH_FORMAT_SCHEMAS_DIR}/</format_schema_path>

  <users_config>${USERS_CONFIG}</users_config>
  <default_profile>default</default_profile>
  <default_database>default</default_database>
</clickhouse>
EOF

log "Starting ClickHouse from $CLICKHOUSE_BIN"
exec "$CLICKHOUSE_BIN" server --config-file "$CONFIG_FILE" --pidfile "$PID_FILE"
