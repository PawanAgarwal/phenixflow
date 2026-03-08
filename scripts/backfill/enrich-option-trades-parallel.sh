#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CPU_CORES="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
CPU_TARGET_PCT="${ENRICH_CPU_TARGET_PCT:-70}"
if ! [[ "$CPU_TARGET_PCT" =~ ^[0-9]+$ ]] || (( CPU_TARGET_PCT < 10 )) || (( CPU_TARGET_PCT > 95 )); then
  echo "ENRICH_CPU_TARGET_PCT must be an integer in [10,95] (got: $CPU_TARGET_PCT)"
  exit 1
fi
DEFAULT_WORKERS=$(( (CPU_CORES * CPU_TARGET_PCT + 99) / 100 ))
if (( DEFAULT_WORKERS < 1 )); then
  DEFAULT_WORKERS=1
fi
ENRICH_WORKERS="${ENRICH_WORKERS:-$DEFAULT_WORKERS}"
MAX_WORKERS="${ENRICH_MAX_WORKERS:-16}"
ENRICH_MEMORY_RESERVE_MB="${ENRICH_MEMORY_RESERVE_MB:-4096}"
ENRICH_MEMORY_PER_WORKER_MB="${ENRICH_MEMORY_PER_WORKER_MB:-2200}"
ENRICH_NODE_MAX_OLD_SPACE_MB="${ENRICH_NODE_MAX_OLD_SPACE_MB:-1024}"
REPORT_DIR="${ENRICH_REPORT_DIR:-$PROJECT_ROOT/artifacts/reports}"
TS="$(date +%Y%m%dT%H%M%S)"

detect_total_memory_mb() {
  local bytes=""
  local kb=""

  bytes="$(sysctl -n hw.memsize 2>/dev/null || true)"
  if [[ "$bytes" =~ ^[0-9]+$ ]] && (( bytes > 0 )); then
    echo $((bytes / 1024 / 1024))
    return
  fi

  if [[ -r /proc/meminfo ]]; then
    kb="$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || true)"
    if [[ "$kb" =~ ^[0-9]+$ ]] && (( kb > 0 )); then
      echo $((kb / 1024))
      return
    fi
  fi

  echo ""
}

if ! [[ "$ENRICH_WORKERS" =~ ^[0-9]+$ ]] || (( ENRICH_WORKERS < 1 )); then
  echo "ENRICH_WORKERS must be a positive integer (got: $ENRICH_WORKERS)"
  exit 1
fi

if ! [[ "$MAX_WORKERS" =~ ^[0-9]+$ ]] || (( MAX_WORKERS < 1 )); then
  echo "ENRICH_MAX_WORKERS must be a positive integer (got: $MAX_WORKERS)"
  exit 1
fi

if ! [[ "$ENRICH_MEMORY_RESERVE_MB" =~ ^[0-9]+$ ]]; then
  echo "ENRICH_MEMORY_RESERVE_MB must be a non-negative integer (got: $ENRICH_MEMORY_RESERVE_MB)"
  exit 1
fi

if ! [[ "$ENRICH_MEMORY_PER_WORKER_MB" =~ ^[0-9]+$ ]] || (( ENRICH_MEMORY_PER_WORKER_MB < 256 )); then
  echo "ENRICH_MEMORY_PER_WORKER_MB must be an integer >= 256 (got: $ENRICH_MEMORY_PER_WORKER_MB)"
  exit 1
fi

if ! [[ "$ENRICH_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( ENRICH_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "ENRICH_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $ENRICH_NODE_MAX_OLD_SPACE_MB)"
  exit 1
fi

if (( ENRICH_WORKERS > MAX_WORKERS )); then
  echo "Capping workers from $ENRICH_WORKERS to ENRICH_MAX_WORKERS=$MAX_WORKERS"
  ENRICH_WORKERS="$MAX_WORKERS"
fi

TOTAL_MEMORY_MB="$(detect_total_memory_mb)"
MEMORY_WORKER_CAP="$MAX_WORKERS"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  if (( TOTAL_MEMORY_MB > ENRICH_MEMORY_RESERVE_MB )); then
    MEMORY_WORKER_CAP=$(( (TOTAL_MEMORY_MB - ENRICH_MEMORY_RESERVE_MB) / ENRICH_MEMORY_PER_WORKER_MB ))
    if (( MEMORY_WORKER_CAP < 1 )); then
      MEMORY_WORKER_CAP=1
    fi
  else
    MEMORY_WORKER_CAP=1
  fi
fi

if (( ENRICH_WORKERS > MEMORY_WORKER_CAP )); then
  echo "Capping workers from $ENRICH_WORKERS to memory cap $MEMORY_WORKER_CAP"
  ENRICH_WORKERS="$MEMORY_WORKER_CAP"
fi

WORKER_NODE_OPTIONS="--max-old-space-size=${ENRICH_NODE_MAX_OLD_SPACE_MB}"
if [[ -n "${NODE_OPTIONS:-}" ]]; then
  WORKER_NODE_OPTIONS="${WORKER_NODE_OPTIONS} ${NODE_OPTIONS}"
fi

mkdir -p "$REPORT_DIR"

echo "Starting parallel enrichment with $ENRICH_WORKERS worker(s)..."
echo "CPU cores: $CPU_CORES"
echo "CPU target: ${CPU_TARGET_PCT}%"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  echo "Total memory detected: ${TOTAL_MEMORY_MB}MB"
else
  echo "Total memory detected: unknown"
fi
echo "Memory reserve: ${ENRICH_MEMORY_RESERVE_MB}MB"
echo "Memory per worker target: ${ENRICH_MEMORY_PER_WORKER_MB}MB"
echo "Memory worker cap: $MEMORY_WORKER_CAP"
echo "Node heap cap: ${ENRICH_NODE_MAX_OLD_SPACE_MB}MB"
echo "Reports: $REPORT_DIR"

pids=()
for idx in $(seq 0 $((ENRICH_WORKERS - 1))); do
  report_path="$REPORT_DIR/enrich-option-trades-worker${idx}-${TS}.json"
  (
    cd "$PROJECT_ROOT"
    NODE_OPTIONS="$WORKER_NODE_OPTIONS" \
    ENRICH_WORKER_TOTAL="$ENRICH_WORKERS" \
    ENRICH_WORKER_INDEX="$idx" \
    ENRICH_REPORT_PATH="$report_path" \
    node scripts/backfill/enrich-option-trades.js
  ) &
  pids+=("$!")
done

fail=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fail=1
  fi
done

if (( fail != 0 )); then
  echo "One or more enrichment workers failed."
  exit 1
fi

echo "Parallel enrichment complete."
