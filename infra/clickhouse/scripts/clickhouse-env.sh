#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

CLICKHOUSE_CONFIG_DIR="${CLICKHOUSE_CONFIG_DIR:-$REPO_ROOT/infra/clickhouse/config}"
CLICKHOUSE_USERS_CONFIG="${CLICKHOUSE_USERS_CONFIG:-$CLICKHOUSE_CONFIG_DIR/users.xml}"

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

PID_FILE="${PID_FILE:-$CH_RUN_DIR/clickhouse-server.pid}"
CONFIG_FILE="${CONFIG_FILE:-$CH_RUN_DIR/generated-config.xml}"

CH_MEMORY_LIMIT_PERCENT="${CH_MEMORY_LIMIT_PERCENT:-70}"
CH_MARK_CACHE_PERCENT_OF_LIMIT="${CH_MARK_CACHE_PERCENT_OF_LIMIT:-18}"
CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT="${CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT:-9}"

get_total_memory_bytes() {
  if [[ -n "${CH_TOTAL_MEMORY_BYTES:-}" ]]; then
    printf '%s\n' "$CH_TOTAL_MEMORY_BYTES"
    return 0
  fi

  case "$(uname -s)" in
    Darwin)
      sysctl -n hw.memsize
      ;;
    Linux)
      awk '/MemTotal:/ { print $2 * 1024; exit }' /proc/meminfo
      ;;
    *)
      echo "Unsupported OS for automatic memory detection: $(uname -s)" >&2
      return 1
      ;;
  esac
}

percent_of() {
  local total="$1"
  local percent="$2"
  awk -v total="$total" -v percent="$percent" 'BEGIN { printf "%.0f\n", (total * percent) / 100 }'
}

percent_to_ratio() {
  local percent="$1"
  awk -v percent="$percent" 'BEGIN { printf "%.6f\n", percent / 100 }'
}

compute_clickhouse_memory_limit_bytes() {
  if [[ -n "${CH_MAX_SERVER_MEMORY_BYTES:-}" ]]; then
    printf '%s\n' "$CH_MAX_SERVER_MEMORY_BYTES"
    return 0
  fi

  local total_memory_bytes
  total_memory_bytes="$(get_total_memory_bytes)"
  percent_of "$total_memory_bytes" "$CH_MEMORY_LIMIT_PERCENT"
}

compute_clickhouse_mark_cache_bytes() {
  if [[ -n "${CH_MARK_CACHE_SIZE_BYTES:-}" ]]; then
    printf '%s\n' "$CH_MARK_CACHE_SIZE_BYTES"
    return 0
  fi

  local memory_limit_bytes
  memory_limit_bytes="$(compute_clickhouse_memory_limit_bytes)"
  percent_of "$memory_limit_bytes" "$CH_MARK_CACHE_PERCENT_OF_LIMIT"
}

compute_clickhouse_uncompressed_cache_bytes() {
  if [[ -n "${CH_UNCOMPRESSED_CACHE_SIZE_BYTES:-}" ]]; then
    printf '%s\n' "$CH_UNCOMPRESSED_CACHE_SIZE_BYTES"
    return 0
  fi

  local memory_limit_bytes
  memory_limit_bytes="$(compute_clickhouse_memory_limit_bytes)"
  percent_of "$memory_limit_bytes" "$CH_UNCOMPRESSED_CACHE_PERCENT_OF_LIMIT"
}
