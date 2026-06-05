#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# 多进程行缓存压测 — 指标与 relation_row_cache_proc_{cache,nocache}.sql 一致
#
# 每个 worker 独立执行：monotonic_ms + prepare(DO) + workload(DO)，
# NOTICE 格式与 proc 脚本相同：
#   stage=cache_prepare|nocache_prepare elapsed=<ms> ms
#   stage=cache_workload|nocache_workload rounds=<r> loops=<l> total_elapsed=<ms> ms
#
# 与 proc 的差异：无 pg_backend_pid / pg_sleep(1.5)；负载为内联 DO（等价于 CALL 存储过程体）
#
# 用法：
#   ./relation_row_cache_multiproc_proc_bench.sh [NPROCS]
#
# 环境变量：
#   NPROCS=4            并发进程数（也可第 1 个参数）
#   LOOPS=1000000       与 proc 中 \set loops 一致
#   ROUNDS=20           与 proc 中 \set rounds 一致
#   ROW_MOD=2000000     与 SQL 中 (g % ROW_MOD)+1 一致，须与 bench_tbl 行数一致
#   PG_BIN PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD
#   OUT_DIR             默认 $HOME/postgres/trace/multiproc_proc
#
# 前置：已执行 relation_row_cache_perf_load.sql（或 2col 时保持 ROW_MOD=2000000）
# 依赖：Linux /proc/uptime（与 proc 中 monotonic_ms 一致）
# ================================================================

NPROCS="${1:-${NPROCS:-4}}"
LOOPS="${LOOPS:-1000000}"
ROUNDS="${ROUNDS:-20}"
ROW_MOD="${ROW_MOD:-2000000}"

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-$HOME/postgres/trace/multiproc_proc}"
mkdir -p "$OUT_DIR"

if [[ -z "${PGPASSWORD:-}" ]] && [[ -z "${PGPASSFILE:-}" ]]; then
  echo "提示: 请设置 PGPASSWORD 或配置 ~/.pgpass（非交互 psql）。" >&2
  echo "用法: PGPASSWORD=... $0 [NPROCS]" >&2
  exit 1
fi

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

echo "=========================================="
echo " multiproc proc-equivalent bench"
echo " NPROCS=$NPROCS  LOOPS=$LOOPS  ROUNDS=$ROUNDS  ROW_MOD=$ROW_MOD"
echo "=========================================="

# mode: cache | nocache
generate_worker_sql() {
  local mode="$1"
  local out="$2"

  if [[ "$mode" == "cache" ]]; then
    cat >"$out" <<EOSQL
\\set ON_ERROR_STOP on
SET client_min_messages = notice;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

CREATE OR REPLACE FUNCTION monotonic_ms()
RETURNS double precision
LANGUAGE plpgsql
AS \$func\$
DECLARE
    content text;
BEGIN
    content := pg_read_file('/proc/uptime');
    RETURN split_part(content, ' ', 1)::double precision * 1000.0;
END;
\$func\$;

DO \$\$
DECLARE
    ms_prepare_start double precision;
    ms_prepare_end   double precision;
BEGIN
    ms_prepare_start := monotonic_ms();

    IF to_regprocedure('pg_load_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_load_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_load_relation_row_cache(...) does not exist';
    END IF;

    ms_prepare_end := monotonic_ms();
    RAISE NOTICE 'stage=cache_prepare elapsed=% ms', (ms_prepare_end - ms_prepare_start);
END
\$\$;

DO \$\$
DECLARE
    i            int;
    ms_work_start double precision;
    ms_work_end   double precision;
BEGIN
    ms_work_start := monotonic_ms();

    FOR i IN 1..${ROUNDS} LOOP
        PERFORM pg_column_size(q.r)
        FROM generate_series(1, ${LOOPS}) AS g
        CROSS JOIN LATERAL
        (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % ${ROW_MOD}) + 1)
        ) AS q;
    END LOOP;

    ms_work_end := monotonic_ms();

    RAISE NOTICE 'stage=cache_workload rounds=% loops=% total_elapsed=% ms',
        ${ROUNDS}, ${LOOPS}, (ms_work_end - ms_work_start);
END
\$\$;
EOSQL
  else
    cat >"$out" <<EOSQL
\\set ON_ERROR_STOP on
SET client_min_messages = notice;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

CREATE OR REPLACE FUNCTION monotonic_ms()
RETURNS double precision
LANGUAGE plpgsql
AS \$func\$
DECLARE
    content text;
BEGIN
    content := pg_read_file('/proc/uptime');
    RETURN split_part(content, ' ', 1)::double precision * 1000.0;
END;
\$func\$;

DO \$\$
DECLARE
    ms_prepare_start double precision;
    ms_prepare_end   double precision;
BEGIN
    ms_prepare_start := monotonic_ms();

    IF to_regprocedure('pg_drop_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_drop_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_drop_relation_row_cache(...) does not exist';
    END IF;

    ms_prepare_end := monotonic_ms();
    RAISE NOTICE 'stage=nocache_prepare elapsed=% ms', (ms_prepare_end - ms_prepare_start);
END
\$\$;

DO \$\$
DECLARE
    i            int;
    ms_work_start double precision;
    ms_work_end   double precision;
BEGIN
    ms_work_start := monotonic_ms();

    FOR i IN 1..${ROUNDS} LOOP
        PERFORM pg_column_size(q.r)
        FROM generate_series(1, ${LOOPS}) AS g
        CROSS JOIN LATERAL
        (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % ${ROW_MOD}) + 1)
        ) AS q;
    END LOOP;

    ms_work_end := monotonic_ms();

    RAISE NOTICE 'stage=nocache_workload rounds=% loops=% total_elapsed=% ms',
        ${ROUNDS}, ${LOOPS}, (ms_work_end - ms_work_start);
END
\$\$;
EOSQL
  fi
}

extract_prepare_ms() {
  grep -E 'stage=(cache|nocache)_prepare elapsed=' "$1" 2>/dev/null \
    | sed -n 's/.*elapsed=\([0-9.]*\) ms.*/\1/p' | tail -1
}

extract_workload_ms() {
  grep -E 'total_elapsed=' "$1" 2>/dev/null \
    | sed -n 's/.*total_elapsed=\([0-9.]*\) ms.*/\1/p' | tail -1
}

run_phase() {
  local mode="$1"
  local pids=()
  echo ""
  echo "--- phase [$mode] 启动 $NPROCS 个并行 psql ---"

  for i in $(seq 1 "$NPROCS"); do
    local sqlf="$OUT_DIR/${mode}_worker_${i}.sql"
    local logf="$OUT_DIR/${mode}_worker_${i}.log"
    generate_worker_sql "$mode" "$sqlf"
    "$PSQL" -X -v ON_ERROR_STOP=1 \
      -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
      -f "$sqlf" >"$logf" 2>&1 &
    pids+=($!)
  done

  local fail=0
  for pid in "${pids[@]}"; do
    wait "$pid" || fail=$((fail + 1))
  done

  if [[ "$fail" -gt 0 ]]; then
    echo "WARNING: $fail / $NPROCS workers failed, see $OUT_DIR/${mode}_worker_*.log" >&2
  fi

  echo ""
  echo "--- [$mode] 各 worker 指标（与 proc 脚本同源 NOTICE） ---"
  printf "%-8s %14s %14s\n" "Worker" "prepare(ms)" "workload(ms)"
  printf "%-8s %14s %14s\n" "------" "------------" "------------"

  local sumw=0.0
  local cnt=0
  for i in $(seq 1 "$NPROCS"); do
    local logf="$OUT_DIR/${mode}_worker_${i}.log"
    local prep wk
    prep=$(extract_prepare_ms "$logf")
    wk=$(extract_workload_ms "$logf")
    [[ -z "$prep" ]] && prep="NA"
    [[ -z "$wk" ]] && wk="NA"
    printf "%-8s %14s %14s\n" "#${i}" "$prep" "$wk"
    if [[ "$wk" != "NA" ]] && [[ "$wk" =~ ^[0-9.]+$ ]]; then
      sumw=$(awk -v s="$sumw" -v w="$wk" 'BEGIN{printf "%.6f", s+w}')
      cnt=$((cnt + 1))
    fi
  done

  local avgw="NA"
  if [[ "$cnt" -gt 0 ]]; then
    avgw=$(awk -v s="$sumw" -v c="$cnt" 'BEGIN{printf "%.3f", s/c}')
    echo ""
    echo "  avg workload (per worker): ${avgw} ms"
  fi

  echo "$mode $NPROCS $avgw" >>"$OUT_DIR/summary_proc_bench.txt"
}

rm -f "$OUT_DIR/summary_proc_bench.txt"

echo ""
echo "=== PHASE 1: nocache（等价 relation_row_cache_proc_nocache.sql 负载与 NOTICE） ==="
run_phase nocache

echo ""
echo "=== PHASE 2: cache（等价 relation_row_cache_proc_cache.sql 负载与 NOTICE） ==="
run_phase cache

echo ""
echo "汇总: $OUT_DIR/summary_proc_bench.txt"
echo "日志: $OUT_DIR/{nocache,cache}_worker_*.sql / *.log"
echo "done."
