#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# 多进程行缓存性能对比测试（awk 版，不依赖 bc）
#
# 用法：
#   ./relation_row_cache_multiproc_bench_awk.sh [NPROCS]
#
# 可选环境变量：
#   NPROCS=4            并发进程数（也可作为第一个参数传入）
#   LOOPS=1000000       每轮循环次数
#   ROUNDS=20           循环轮数
#   PG_BIN="$HOME/pg_install/bin"
#   PGHOST="127.0.0.1"  PGPORT="5432"
#   PGUSER="zhui"       PGDATABASE="postgres"
#   OUT_DIR="$HOME/postgres/trace/multiproc"
#
# 前置条件：
#   1) 已执行 relation_row_cache_perf_load.sql 装填数据
#   2) max_connections >= NPROCS + 5
# ================================================================

# --- 参数 ---
NPROCS="${1:-${NPROCS:-4}}"
LOOPS="${LOOPS:-1000000}"
ROUNDS="${ROUNDS:-20}"

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-zhui}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

if [[ -z "$PGPASSWORD" ]]; then
  echo "提示: 未设置 PGPASSWORD，后台 psql 无法交互输入密码。"
  echo "用法: PGPASSWORD=yourpass $0 $*"
  exit 1
fi

OUT_DIR="${OUT_DIR:-$HOME/postgres/trace/multiproc}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

echo "=========================================="
echo " 多进程行缓存对比测试"
echo " NPROCS=$NPROCS  LOOPS=$LOOPS  ROUNDS=$ROUNDS"
echo "=========================================="

# --- 辅助函数 ---

# 通过 psql 执行单条 SQL（管理连接）
psql_exec() {
  "$PSQL" -X -A -t -q -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "$1"
}

# 提前创建 monotonic_ms 函数，避免多 worker 并发 DDL 争锁
psql_exec "
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
"

# 生成每个 worker 要执行的 SQL 临时文件
generate_worker_sql() {
  local mode="$1"   # cache / nocache
  local tmpfile="$2"

  cat > "$tmpfile" <<EOSQL
SET client_min_messages = notice;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

-- 使用 shared advisory lock 做发令枪，所有 worker 同时放行
SELECT pg_advisory_lock_shared(999999);
SELECT pg_advisory_unlock_shared(999999);

DO \$\$
DECLARE
    ms_start double precision;
    ms_end   double precision;
    i        int;
BEGIN
    ms_start := monotonic_ms();

    FOR i IN 1..${ROUNDS} LOOP
        PERFORM pg_column_size(q.r)
        FROM generate_series(1, ${LOOPS}) AS g
        CROSS JOIN LATERAL
        (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q;
    END LOOP;

    ms_end := monotonic_ms();

    RAISE NOTICE 'stage=${mode}_workload rounds=${ROUNDS} loops=${LOOPS} total_elapsed=% ms',
        (ms_end - ms_start);
END
\$\$;
EOSQL
}

# 提取 total_elapsed 数值（毫秒）
extract_elapsed() {
  grep -oP 'total_elapsed=\K[0-9.]+' "$1" 2>/dev/null || echo "NA"
}

# --- 运行一个阶段（nocache 或 cache）---
run_phase() {
  local mode="$1"
  local worker_sql="$OUT_DIR/${mode}_worker.sql"
  local pids=()

  generate_worker_sql "$mode" "$worker_sql"

  echo ""
  echo "--- [$mode] 启动 $NPROCS 个并发进程 ---"

  # 用一个持续存活的后台 psql 会话持有 advisory lock 做"发令枪"。
  # 通过命名管道向它发送 SQL，会话不断开，锁就一直有效。
  local lock_pipe="$OUT_DIR/${mode}_lock_pipe"
  rm -f "$lock_pipe"
  mkfifo "$lock_pipe"

  # 后台 psql 从管道读取 SQL，先获取锁后阻塞等待下一条命令
  "$PSQL" -X -A -t -q \
    -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
    < "$lock_pipe" >"$OUT_DIR/${mode}_lock.log" 2>&1 &
  local lock_psql_pid=$!

  # 向管道写入加锁命令（写完不关闭 fd，保持会话存活）
  exec 7>"$lock_pipe"
  echo "SELECT pg_advisory_lock(999999);" >&7
  sleep 0.5  # 等待锁获取完成

  # 启动所有 worker — 每个 worker 会阻塞在 pg_advisory_lock(999999)
  for i in $(seq 1 "$NPROCS"); do
    local logfile="$OUT_DIR/${mode}_worker_${i}.log"
    "$PSQL" -X -A -t \
      -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
      -f "$worker_sql" >"$logfile" 2>&1 &
    pids+=($!)
  done

  # 等所有 worker 都连上并阻塞在 advisory lock
  sleep 2

  # 记录墙钟起点
  local wall_start
  wall_start=$(date +%s%3N)

  # 释放发令枪：在同一个持锁会话中 unlock，然后关闭管道让会话退出
  echo "SELECT pg_advisory_unlock(999999);" >&7
  exec 7>&-  # 关闭写端，持锁 psql 会话随之退出
  wait "$lock_psql_pid" 2>/dev/null || true
  rm -f "$lock_pipe"

  # 等待所有 worker 完成
  local fail=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      fail=$(( fail + 1 ))
    fi
  done

  local wall_end
  wall_end=$(date +%s%3N)
  local wall_ms=$(( wall_end - wall_start ))

  if [[ $fail -gt 0 ]]; then
    echo "WARNING: $fail / $NPROCS workers failed, check logs in $OUT_DIR" >&2
  fi

  # 收集结果
  echo ""
  echo "--- [$mode] 结果 (NPROCS=$NPROCS) ---"
  printf "%-10s %12s\n" "Worker" "Elapsed(ms)"
  printf "%-10s %12s\n" "------" "-----------"

  local sum=0
  local count=0
  local min=""
  local max=""

  for i in $(seq 1 "$NPROCS"); do
    local logfile="$OUT_DIR/${mode}_worker_${i}.log"
    local ms
    ms=$(extract_elapsed "$logfile")

    printf "%-10s %12s\n" "#${i}" "$ms"

    if [[ "$ms" != "NA" ]]; then
      sum=$(awk "BEGIN{printf \"%.4f\", $sum + $ms}")
      count=$(( count + 1 ))

      if [[ -z "$min" ]] || awk "BEGIN{exit !($ms < $min)}"; then
        min="$ms"
      fi
      if [[ -z "$max" ]] || awk "BEGIN{exit !($ms > $max)}"; then
        max="$ms"
      fi
    fi
  done

  echo ""
  if [[ $count -gt 0 ]]; then
    local avg
    avg=$(awk "BEGIN{printf \"%.1f\", $sum / $count}")
    local throughput
    throughput=$(awk "BEGIN{printf \"%.1f\", $NPROCS * $ROUNDS * $LOOPS / ($wall_ms / 1000.0)}")

    printf "  avg_per_worker : %s ms\n" "$avg"
    printf "  min_per_worker : %s ms\n" "$min"
    printf "  max_per_worker : %s ms\n" "$max"
    printf "  wall_clock     : %s ms\n" "$wall_ms"
    printf "  throughput     : %s rows/sec\n" "$throughput"
  fi

  # 把汇总写入文件供后续对比
  echo "$mode $NPROCS $wall_ms ${avg:-NA} ${min:-NA} ${max:-NA}" \
    >> "$OUT_DIR/summary.txt"
}

# --- 主流程 ---

rm -f "$OUT_DIR/summary.txt"

# 1) nocache 阶段
echo ""
echo "=== PHASE 1: nocache ==="
psql_exec "SELECT pg_drop_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);" \
  >/dev/null 2>&1 || true
run_phase nocache

# 2) cache 阶段
echo ""
echo "=== PHASE 2: cache ==="
echo "Loading row cache..."
psql_exec "SELECT pg_load_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);"
echo "Row cache loaded."
run_phase cache

# 3) 汇总对比
echo ""
echo "=========================================="
echo " 对比汇总"
echo "=========================================="

if [[ -f "$OUT_DIR/summary.txt" ]]; then
  nocache_wall=$(awk '$1=="nocache" {print $3}' "$OUT_DIR/summary.txt")
  cache_wall=$(awk '$1=="cache" {print $3}' "$OUT_DIR/summary.txt")
  nocache_avg=$(awk '$1=="nocache" {print $4}' "$OUT_DIR/summary.txt")
  cache_avg=$(awk '$1=="cache" {print $4}' "$OUT_DIR/summary.txt")

  if [[ -n "$nocache_wall" && -n "$cache_wall" && "$cache_wall" != "0" ]]; then
    speedup_wall=$(awk "BEGIN{printf \"%.2f\", $nocache_wall / $cache_wall}")
    speedup_avg=$(awk "BEGIN{printf \"%.2f\", $nocache_avg / $cache_avg}")

    printf "  NPROCS           : %s\n" "$NPROCS"
    printf "  nocache wall     : %s ms\n" "$nocache_wall"
    printf "  cache   wall     : %s ms\n" "$cache_wall"
    printf "  wall speedup     : %sx\n" "$speedup_wall"
    printf "  nocache avg/worker: %s ms\n" "$nocache_avg"
    printf "  cache   avg/worker: %s ms\n" "$cache_avg"
    printf "  avg speedup      : %sx\n" "$speedup_avg"
  fi
fi

echo ""
echo "详细日志: $OUT_DIR/"
echo "done."
