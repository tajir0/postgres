#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# V4 row cache — FOR UPDATE / 行锁 两会话确定性回归
#
# 缓存命中路径只提供"元组 + 真实 TID"做定位加速;加锁语义由上层
# LockRows 经 table_tuple_lock 重读真实堆完成(打 xmax / 追更新链 /
# EPQ)。本脚本用两个 psql 会话按固定时序编排三个场景,每个场景在
# cache / nocache 两种模式下各跑一遍,断言行为完全一致:
#
#   S1 真实锁:    A 对已缓存行 SELECT ... FOR UPDATE(缓存命中,
#      拿到的 TID 必须真实)。持锁期间 B 带 lock_timeout=500ms 的
#      UPDATE 必须锁超时失败;A 提交后 B 必须成功。
#      —— 若命中路径的 TID 缺失/错误,锁根本落不到堆行上,B 不会被挡。
#
#   S2 EPQ:       B 在开启事务里先 UPDATE 该行(heap_update 钩子此刻
#      已摘条目,故 A 的探测是 miss——保留 cache 模式用于钉死
#      探测→回退→EPQ 整条流水线)。A 的 FOR UPDATE 阻塞在 B 上,
#      B 提交后 A 必须返回 B 的新版本,绝不能返回旧版本。
#
#   S3 REPEATABLE READ:A 以缓存命中读取行建立 RR 快照,B 更新同行
#      并提交(不被阻塞),A 随后的 FOR UPDATE 必须报
#      40001 "could not serialize access due to concurrent update"。
#      —— 若缓存用自己的副本满足了加锁而不重读堆,该错误不会出现。
#
# 注:『探测命中后、LockRows 加锁前,行恰好被并发更新』这个微秒级
# 竞态无法从 shell 确定性编排,由 relation_row_cache_correctness.sh
# 里的 FOR UPDATE reader 压测 worker 做统计覆盖。
#
# Usage:
#   ./relation_row_cache_forupdate.sh
#
# Environment(同 relation_row_cache_correctness.sh):
#   PG_BIN PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD
#   OUT_DIR      default /tmp/rowcache_forupdate
#   HOLD_SEC     后台会话持锁/持事务时长, default 4
#   STAGGER_SEC  第二个会话的启动延迟, default 1.5
#
# Exit code 0 = PASS, non-zero = FAIL
# macOS 兼容:只用 sleep + pg_sleep,不用 timeout / date +%N。
# ================================================================

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_forupdate}"
HOLD_SEC="${HOLD_SEC:-4}"
STAGGER_SEC="${STAGGER_SEC:-1.5}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -A -t -q)
run_psql() { "$PSQL" "${PSQL_OPTS[@]}" "$@"; }

# 每个会话统一的执行环境:强制走普通 Index Scan(行缓存只从
# IndexNext 探测;IndexOnly / Bitmap / SeqScan 都不经过缓存)。
SETTINGS="
SET client_min_messages = warning;
SET search_path = row_cache_forupdate, public;
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
SET max_parallel_workers_per_gather = 0;
"

FAILURES=0
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
ok()   { echo "  ok:   $*"; }

# ---------------------------------------------------------------
# setup
# ---------------------------------------------------------------
echo "=========================================="
echo " row cache FOR UPDATE concurrency test"
echo " HOLD=${HOLD_SEC}s STAGGER=${STAGGER_SEC}s OUT_DIR=${OUT_DIR}"
echo "=========================================="

# 清掉上次运行可能残留的会话,避免 DROP SCHEMA 卡锁。
run_psql <<EOSQL >/dev/null 2>&1 || true
SELECT pg_terminate_backend(pid) FROM pg_stat_activity
WHERE pid <> pg_backend_pid() AND query ILIKE '%row_cache_forupdate%';
EOSQL
sleep 1

run_psql -v ON_ERROR_STOP=1 <<EOSQL >/dev/null
SET client_min_messages = warning;
SET lock_timeout = '15s';
DROP SCHEMA IF EXISTS row_cache_forupdate CASCADE;
CREATE SCHEMA row_cache_forupdate;
CREATE TABLE row_cache_forupdate.t (
    id      int4 PRIMARY KEY,
    version int4 NOT NULL,
    payload text NOT NULL
);
INSERT INTO row_cache_forupdate.t SELECT g, 0, 'v0' FROM generate_series(1, 1000) g;
ANALYZE row_cache_forupdate.t;
EOSQL
echo " setup OK (1000 rows)"

# 计划闸门:探测语句必须是 LockRows -> Index Scan,否则整个测试是
# 空转(探测点在 IndexNext,别的计划形状根本不经过缓存)。
PLAN=$(run_psql <<EOSQL
$SETTINGS
EXPLAIN (COSTS OFF) SELECT version FROM t WHERE id = 1 FOR UPDATE;
EOSQL
)
if ! grep -q "Index Scan" <<<"$PLAN" || ! grep -q "LockRows" <<<"$PLAN"; then
  echo "ERROR: probe plan is not LockRows -> Index Scan; test would be vacuous:"
  echo "$PLAN"
  exit 1
fi
echo " plan gate OK (LockRows -> Index Scan)"

reset_rows() {
  run_psql -v ON_ERROR_STOP=1 <<EOSQL >/dev/null
$SETTINGS
UPDATE t SET version = 0, payload = 'v0' WHERE id IN (42, 43, 44);
EOSQL
}

# 模式准备。注意顺序:先 reset(UPDATE 会经 DML 钩子摘条目),
# 再 load,保证 cache 模式下被探测行此刻确实在缓存里。
prep_mode() {  # $1 = cache | nocache
  run_psql -v ON_ERROR_STOP=1 <<EOSQL >/dev/null
SET client_min_messages = warning;
SELECT pg_drop_relation_row_cache('row_cache_forupdate.t');
EOSQL
  if [[ "$1" == "cache" ]]; then
    run_psql -v ON_ERROR_STOP=1 <<EOSQL >/dev/null
SET client_min_messages = warning;
SELECT pg_load_relation_row_cache('row_cache_forupdate.t');
EOSQL
  fi
}

# ---------------------------------------------------------------
# S1 真实锁:缓存命中的 FOR UPDATE 必须在真实堆行上持锁
# ---------------------------------------------------------------
scenario_real_lock() {  # $1 = mode
  local mode="$1" tag="s1_${mode}" a_pid
  reset_rows; prep_mode "$mode"

  cat >"$OUT_DIR/${tag}_a.sql" <<EOSQL
$SETTINGS
BEGIN;
SELECT 'A_LOCKED=' || version FROM t WHERE id = 44 FOR UPDATE;
SELECT pg_sleep(${HOLD_SEC});
COMMIT;
EOSQL
  ( exec "$PSQL" "${PSQL_OPTS[@]}" -f "$OUT_DIR/${tag}_a.sql" \
      >"$OUT_DIR/${tag}_a.log" 2>&1 ) &
  a_pid=$!
  sleep "$STAGGER_SEC"

  # B1:A 持锁期间必须锁超时。
  "$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/${tag}_b1.log" 2>&1 <<EOSQL || true
$SETTINGS
SET lock_timeout = '500ms';
UPDATE t SET version = 7, payload = 'v7' WHERE id = 44;
EOSQL

  wait "$a_pid" || true

  # B2:A 提交后同样的 UPDATE 必须成功。
  "$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$OUT_DIR/${tag}_b2.log" 2>&1 <<EOSQL
$SETTINGS
UPDATE t SET version = 99, payload = 'v99' WHERE id = 44;
SELECT 'B2_FINAL=' || version FROM t WHERE id = 44;
EOSQL

  grep -q "A_LOCKED=0" "$OUT_DIR/${tag}_a.log" \
    && ok "$tag: A 经 ${mode} 路径锁住已提交版本(version=0)" \
    || fail "$tag: A 未返回 version=0(见 ${tag}_a.log)"
  grep -qi "lock timeout" "$OUT_DIR/${tag}_b1.log" \
    && ok "$tag: B 被 A 的行锁挡住(堆锁真实)" \
    || fail "$tag: B 未被挡住 —— ${mode} 路径的 FOR UPDATE 没有拿到真实堆锁!"
  grep -q "B2_FINAL=99" "$OUT_DIR/${tag}_b2.log" \
    && ok "$tag: A 提交后锁释放,B 更新成功" \
    || fail "$tag: A 提交后 B 仍失败(见 ${tag}_b2.log)"
}

# ---------------------------------------------------------------
# S2 EPQ:加锁者必须看到并发提交的最新版本
# ---------------------------------------------------------------
scenario_epq() {  # $1 = mode
  local mode="$1" tag="s2_${mode}" b_pid
  reset_rows; prep_mode "$mode"

  cat >"$OUT_DIR/${tag}_b.sql" <<EOSQL
$SETTINGS
BEGIN;
UPDATE t SET version = 1, payload = 'v1' WHERE id = 42;
SELECT pg_sleep(${HOLD_SEC});
COMMIT;
EOSQL
  ( exec "$PSQL" "${PSQL_OPTS[@]}" -f "$OUT_DIR/${tag}_b.sql" \
      >"$OUT_DIR/${tag}_b.log" 2>&1 ) &
  b_pid=$!
  sleep "$STAGGER_SEC"

  # A:阻塞在 B 的未提交更新上;B 提交后 EPQ 必须给出新版本。
  "$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/${tag}_a.log" 2>&1 <<EOSQL || true
$SETTINGS
SET lock_timeout = '20s';
SELECT 'A_SAW=' || version FROM t WHERE id = 42 FOR UPDATE;
EOSQL

  wait "$b_pid" || true

  if grep -q "A_SAW=1" "$OUT_DIR/${tag}_a.log"; then
    ok "$tag: 加锁者经 EPQ 看到 B 的新版本(version=1)"
  elif grep -q "A_SAW=0" "$OUT_DIR/${tag}_a.log"; then
    fail "$tag: 加锁者返回了旧版本 version=0 —— EPQ 被绕过!"
  else
    fail "$tag: 输出异常(见 ${tag}_a.log)"
  fi
}

# ---------------------------------------------------------------
# S3 REPEATABLE READ:并发更新后 FOR UPDATE 必须报 40001
# ---------------------------------------------------------------
scenario_rr_serialization() {  # $1 = mode
  local mode="$1" tag="s3_${mode}" a_pid
  reset_rows; prep_mode "$mode"

  # 故意不设 ON_ERROR_STOP:40001 之后 psql 继续执行 COMMIT(回滚),
  # 错误文本留在日志里供断言。
  cat >"$OUT_DIR/${tag}_a.sql" <<EOSQL
$SETTINGS
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT 'A_SNAP=' || version FROM t WHERE id = 43;
SELECT pg_sleep(${HOLD_SEC});
SELECT 'A_LOCKED=' || version FROM t WHERE id = 43 FOR UPDATE;
COMMIT;
EOSQL
  ( exec "$PSQL" "${PSQL_OPTS[@]}" -f "$OUT_DIR/${tag}_a.sql" \
      >"$OUT_DIR/${tag}_a.log" 2>&1 ) &
  a_pid=$!
  sleep "$STAGGER_SEC"

  # B:A 只读了快照没持锁,B 的更新必须立即成功。
  "$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$OUT_DIR/${tag}_b.log" 2>&1 <<EOSQL
$SETTINGS
SET lock_timeout = '5s';
UPDATE t SET version = 1, payload = 'v1' WHERE id = 43;
EOSQL

  wait "$a_pid" || true

  grep -q "A_SNAP=0" "$OUT_DIR/${tag}_a.log" \
    && ok "$tag: RR 快照读返回 version=0(${mode} 模式下此读走探测路径)" \
    || fail "$tag: 快照读结果异常(见 ${tag}_a.log)"
  grep -q "could not serialize access due to concurrent update" "$OUT_DIR/${tag}_a.log" \
    && ok "$tag: FOR UPDATE 正确报出 40001" \
    || fail "$tag: 未报 40001 —— 加锁没有对真实堆做重检查?"
  grep -q "A_LOCKED=" "$OUT_DIR/${tag}_a.log" \
    && fail "$tag: FOR UPDATE 竟然返回了行" \
    || ok "$tag: FOR UPDATE 未返回行(符合预期)"
}

# ---------------------------------------------------------------
# 主流程:两种模式 × 三个场景
# ---------------------------------------------------------------
for mode in nocache cache; do
  echo "------------------------------------------"
  echo " mode: $mode"
  echo "------------------------------------------"
  scenario_real_lock "$mode"
  scenario_epq "$mode"
  scenario_rr_serialization "$mode"
done

run_psql <<EOSQL >/dev/null 2>&1 || true
SELECT pg_drop_relation_row_cache('row_cache_forupdate.t');
EOSQL

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS — FOR UPDATE 语义在 cache / nocache 两种模式下完全一致"
  exit 0
else
  echo " FAIL — $FAILURES 项断言失败,日志见 $OUT_DIR"
  exit 1
fi
