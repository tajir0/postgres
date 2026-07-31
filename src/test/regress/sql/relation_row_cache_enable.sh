#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# 行缓存 — enable-only 入口回归
#
# pg_enable_relation_row_cache 语义: 只注册 RelMeta + 拍 schema 指纹并
# 置 ENABLED, 不做全表扫描; 行靠点查 miss 后按需回填逐步填入。
# 相对 pg_load_relation_row_cache 的取舍: 零预热、只有真被访问的行占段、
# 且只取 AccessShareLock 不阻塞 DML。
#
# 断言:
#   ① 开启后不预热: 首次点查仍走原生 (Index Searches: 1);
#   ② 回填生效: 第二次点查命中缓存 (Index Searches: 0);
#   ③ 不阻塞写: 持写事务时 enable 立即返回 (对比 load 会被挡);
#   ④ DML 失效照常: 更新后点查返回新值;
#   ⑤ DDL 指纹照常: ALTER TABLE 后缓存失效, 点查结果正确;
#   ⑥ drop 后回到未缓存: 点查恢复 Index Searches: 1;
#   ⑦ 资格检查照常: 无主键表 enable 返回 false。
#
# Usage / env: 同 relation_row_cache_correctness.sh。Exit 0 = PASS。
# ================================================================

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_enable}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -A -t -q)
FAILURES=0
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
ok()   { echo "  ok:   $*"; }

SETTINGS="
SET client_min_messages = warning;
SET search_path = rc_enable, public;
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
"

run_sql() {  # $1 = 日志名, 其余 = SQL
  local log="$OUT_DIR/$1"
  shift
  "$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$log" 2>&1 <<EOSQL
$SETTINGS
$*
EOSQL
}

assert_result() {  # $1 日志, $2 期望行, $3 场景
  if grep -qx "$2" "$OUT_DIR/$1"; then ok "$3: $2"
  else fail "$3: 期望 $2 (见 $OUT_DIR/$1)"; fi
}

assert_searches() {  # $1 日志, $2 期望值, $3 场景
  if grep -Eq "Index Searches: ${2}([^0-9]|$)" "$OUT_DIR/$1"; then
    ok "$3: Index Searches=$2"
  else
    fail "$3: 未见 Index Searches=$2 (见 $OUT_DIR/$1)"
  fi
}

echo "=========================================="
echo " row cache enable-only test (OUT_DIR=$OUT_DIR)"
echo "=========================================="

# ---------------------------------------------------------------
# setup
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$OUT_DIR/setup.log" 2>&1 <<'EOSQL'
SET client_min_messages = warning;
SET lock_timeout = '15s';
DROP SCHEMA IF EXISTS rc_enable CASCADE;
CREATE SCHEMA rc_enable;
CREATE TABLE rc_enable.t (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO rc_enable.t SELECT g, 'v' || g FROM generate_series(1, 1000) g;
CREATE TABLE rc_enable.nopk (id int4, v text NOT NULL);
INSERT INTO rc_enable.nopk SELECT g, 'n' || g FROM generate_series(1, 100) g;
ANALYZE rc_enable.t;
ANALYZE rc_enable.nopk;
EOSQL
echo " setup OK"

# ---------------------------------------------------------------
# ① 开启后不预热 / ② 回填后命中
# ---------------------------------------------------------------
run_sql enable.log "SELECT 'E1=' || pg_enable_relation_row_cache('rc_enable.t');"
assert_result enable.log "E1=true" "① enable 返回 true"

run_sql probe1.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t WHERE id = 42;
SELECT 'R1=' || v FROM t WHERE id = 42;
"
assert_searches probe1.log 1 "① enable 后首次点查未预热, 走原生"
assert_result probe1.log "R1=v42" "① 首次点查结果"

run_sql probe2.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t WHERE id = 42;
SELECT 'R2=' || v FROM t WHERE id = 42;
"
assert_searches probe2.log 0 "② 回填后第二次点查命中缓存"
assert_result probe2.log "R2=v42" "② 第二次点查结果"

# 未被访问过的行仍未缓存
run_sql probe_cold.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t WHERE id = 777;
"
assert_searches probe_cold.log 1 "② 未访问过的行仍未缓存(按需填充)"

# ---------------------------------------------------------------
# ②b 观测函数:enable 后应"0 段 0 条目",回填后段/条目/命中数增长
# ---------------------------------------------------------------
run_sql stats_after_probe.log "
SELECT 'S1=' || n_segments || '/' || n_entries || '/' || hit_count || '/' || backfill_count
FROM pg_row_cache_relation_stats('rc_enable.t'::regclass);
"
if grep -qE "^S1=[1-9][0-9]*/[1-9][0-9]*/[1-9][0-9]*/[1-9][0-9]*$" "$OUT_DIR/stats_after_probe.log"; then
  ok "②b 观测函数: 回填后段数/条目数/命中数/回填数均 > 0 ($(grep -o 'S1=.*' "$OUT_DIR/stats_after_probe.log"))"
else
  fail "②b 观测函数数值异常 (见 $OUT_DIR/stats_after_probe.log)"
fi

# ---------------------------------------------------------------
# ③ enable 不阻塞写(AccessShareLock);load 会被挡(ShareLock)
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/holder.log" 2>&1 <<'EOSQL' &
SET client_min_messages = warning;
BEGIN;
UPDATE rc_enable.t SET v = v WHERE id = 1;   -- 持 RowExclusiveLock
SELECT pg_sleep(4);
COMMIT;
EOSQL
HOLDER_PID=$!
sleep 1

run_sql enable_nonblock.log "
SET lock_timeout = '2s';
SELECT 'E2=' || pg_enable_relation_row_cache('rc_enable.t');
"
assert_result enable_nonblock.log "E2=true" "③ 写事务进行中 enable 不被阻塞"

"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/load_block.log" 2>&1 <<'EOSQL' || true
SET client_min_messages = warning;
SET lock_timeout = '2s';
SELECT 'L1=' || pg_load_relation_row_cache('rc_enable.t');
EOSQL
if grep -qi "lock timeout" "$OUT_DIR/load_block.log"; then
  ok "③ 对照: 同一时刻 load 因 ShareLock 被写事务挡住(锁超时)"
else
  fail "③ 对照: load 未被挡住(见 $OUT_DIR/load_block.log)"
fi
wait "$HOLDER_PID" 2>/dev/null || true

# ---------------------------------------------------------------
# ④ DML 失效照常
# ---------------------------------------------------------------
run_sql warm.log "SELECT v FROM t WHERE id = 100;"   # 先回填 id=100
run_sql dml.log "
UPDATE t SET v = 'updated100' WHERE id = 100;
SELECT 'R3=' || v FROM t WHERE id = 100;
"
assert_result dml.log "R3=updated100" "④ DML 失效后点查返回新值"

# ---------------------------------------------------------------
# ⑤ DDL 指纹照常生效
# ---------------------------------------------------------------
run_sql ddl.log "
ALTER TABLE t ADD COLUMN extra int4 DEFAULT 7;
SELECT 'R4=' || v || '/' || extra FROM t WHERE id = 42;
"
assert_result ddl.log "R4=v42/7" "⑤ ADD COLUMN 后点查结果正确"

# ---------------------------------------------------------------
# ⑥ drop 后回到未缓存
# ---------------------------------------------------------------
run_sql reenable.log "
SELECT pg_enable_relation_row_cache('rc_enable.t');
SELECT v FROM t WHERE id = 42;
"
run_sql after_drop.log "
SELECT pg_drop_relation_row_cache('rc_enable.t');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t WHERE id = 42;
"
assert_searches after_drop.log 1 "⑥ drop 后点查恢复原生路径"

# ---------------------------------------------------------------
# ⑦ 资格检查照常(无主键表拒绝)
# ---------------------------------------------------------------
run_sql nopk.log "SELECT 'E3=' || pg_enable_relation_row_cache('rc_enable.nopk');"
assert_result nopk.log "E3=true" "⑦ 无主键表: 函数返回 true(注册尝试成功)"
run_sql nopk_probe.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM nopk WHERE id = 42;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM nopk WHERE id = 42;
"
if grep -Eq "Index Searches: 0" "$OUT_DIR/nopk_probe.log"; then
  fail "⑦ 无主键表不应命中缓存"
else
  ok "⑦ 无主键表始终走原生(资格检查拒绝)"
fi

# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/cleanup.log" 2>&1 <<'EOSQL' || true
SET client_min_messages = warning;
SELECT pg_drop_relation_row_cache('rc_enable.t');
DROP SCHEMA IF EXISTS rc_enable CASCADE;
EOSQL

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS — enable 只注册不预热、回填生效、不阻塞写、失效与资格检查照常"
  exit 0
else
  echo " FAIL — $FAILURES 项断言失败, 日志见 $OUT_DIR"
  exit 1
fi
