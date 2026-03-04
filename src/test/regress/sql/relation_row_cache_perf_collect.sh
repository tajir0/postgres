#!/usr/bin/env bash
set -euo pipefail

# 用法示例：
#   ./src/test/regress/sql/relation_row_cache_perf_collect.sh
#
# 可选环境变量：
#   PG_BIN="$HOME/pg_install/bin"
#   PGUSER="zhui"
#   PGDATABASE="postgres"
#   PGHOST="127.0.0.1"
#   PGPORT="5432"
#   PERF_BIN="/usr/lib/linux-tools-6.8.0-101/perf"
#   PERF_FREQ="99"
#   PERF_WINDOW_SEC="180"
#   PERF_ATTACH_DELAY_SEC="1.2"
#   OUT_DIR="$HOME/postgres/trace/perf_proc"

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-zhui}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"

PERF_BIN="${PERF_BIN:-perf}"
PERF_FREQ="${PERF_FREQ:-99}"
PERF_WINDOW_SEC="${PERF_WINDOW_SEC:-180}"
PERF_ATTACH_DELAY_SEC="${PERF_ATTACH_DELAY_SEC:-1.2}"
OUT_DIR="${OUT_DIR:-$HOME/postgres/trace/perf_proc}"

SCRIPT_DIR="${SCRIPT_DIR:-$HOME/postgres/src/test/regress/sql}"
CACHE_SQL="$SCRIPT_DIR/relation_row_cache_proc_cache.sql"
NOCACHE_SQL="$SCRIPT_DIR/relation_row_cache_proc_nocache.sql"

mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "psql not found: $PSQL" >&2
  exit 1
fi

if ! command -v "$PERF_BIN" >/dev/null 2>&1; then
  echo "perf not found in PATH: $PERF_BIN" >&2
  exit 1
fi

extract_metric() {
  local key="$1"
  local file="$2"
  grep -E "${key}" "$file" | tail -n1 | sed -E "s/.*(${key}[^,)]*).*/\1/" || true
}

run_one() {
  local mode="$1"
  local sql_file="$2"
  local psql_log="$OUT_DIR/${mode}.psql.log"
  local perf_data="$OUT_DIR/${mode}.perf.data"

  rm -f "$psql_log" "$perf_data"

  "$PSQL" -X -A -t -q \
    -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    -f "$sql_file" >"$psql_log" 2>&1 &
  local psql_pid=$!

  local backend_pid=""
  for _ in $(seq 1 100); do
    if [[ -s "$psql_log" ]]; then
      backend_pid="$(grep -m1 -E '^[0-9]+$' "$psql_log" || true)"
      if [[ -n "$backend_pid" ]]; then
        break
      fi
    fi
    sleep 0.1
  done

  if [[ -z "$backend_pid" ]]; then
    echo "[$mode] failed to get backend pid; see $psql_log" >&2
    wait "$psql_pid" || true
    return 1
  fi

  echo "[$mode] backend pid: $backend_pid"
  # SQL 脚本会在 CALL 前执行 pg_sleep(1.5)，这里延迟附着以避开 prepare 阶段。
  sleep "$PERF_ATTACH_DELAY_SEC"

  "$PERF_BIN" record -F "$PERF_FREQ" -g -p "$backend_pid" \
    -o "$perf_data" -- sleep "$PERF_WINDOW_SEC" >/dev/null 2>&1 &
  local perf_pid=$!

  wait "$psql_pid"
  local psql_rc=$?

  if kill -0 "$perf_pid" >/dev/null 2>&1; then
    kill -INT "$perf_pid" >/dev/null 2>&1 || true
  fi
  wait "$perf_pid" >/dev/null 2>&1 || true

  if [[ $psql_rc -ne 0 ]]; then
    echo "[$mode] psql failed, rc=$psql_rc; see $psql_log" >&2
    return "$psql_rc"
  fi

  local prepare_line
  local workload_line
  prepare_line="$(extract_metric 'stage=.*_prepare elapsed=' "$psql_log")"
  workload_line="$(extract_metric 'stage=.*_workload.*total_elapsed=' "$psql_log")"

  echo "[$mode] done:"
  echo "  psql log : $psql_log"
  echo "  perf data: $perf_data"
  echo "  summary  : ${prepare_line:-prepare=NA} | ${workload_line:-workload=NA}"
}

run_one cache "$CACHE_SQL"
run_one nocache "$NOCACHE_SQL"

echo "all done. you can build flamegraph from:"
echo "  $OUT_DIR/cache.perf.data"
echo "  $OUT_DIR/nocache.perf.data"
