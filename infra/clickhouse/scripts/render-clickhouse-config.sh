#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/clickhouse-env.sh"

if [[ ! -f "$CLICKHOUSE_USERS_CONFIG" ]]; then
  echo "Missing ClickHouse users config: $CLICKHOUSE_USERS_CONFIG" >&2
  exit 1
fi

mkdir -p "$CH_RUN_DIR"

max_server_memory_usage="$(compute_clickhouse_memory_limit_bytes)"
mark_cache_size="$(compute_clickhouse_mark_cache_bytes)"
uncompressed_cache_size="$(compute_clickhouse_uncompressed_cache_bytes)"
max_server_memory_ratio="$(percent_to_ratio "$CH_MEMORY_LIMIT_PERCENT")"

cat > "$CONFIG_FILE" <<EOF
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

  <users_config>${CLICKHOUSE_USERS_CONFIG}</users_config>
  <default_profile>default</default_profile>
  <default_database>default</default_database>
</clickhouse>
EOF

printf '%s\n' "$CONFIG_FILE"
