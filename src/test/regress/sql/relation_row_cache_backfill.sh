#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# V4 行缓存 — S3 按需回填回归
#
# 观测手段:回填成功时的 DEBUG2 日志
#   "row cache: backfilled one row of relation ..."
# 配合 client_min_messages=debug2 做出现/缺席断言。
#
# 场景:
#   ① 回填发生且被后续命中:UPDATE 摘掉条目 → 第一次点查 miss、
#      回填(日志出现)→ 第二次点查命中(日志缺席——若第一次回填
#      没生效,第二次 miss 会再回填一次,日志会出现)。
#   ② GUC 开关:row_cache_backfill=off 时点查不回填(日志缺席),
#      打开后才回填。
#   ③ 回填后 DML 失效仍正确:回填的条目被 UPDATE 摘除,点查返回新值。
#   ④ 未 load 的表不回填(RelMeta 不存在,探测未武装)。
#
# 并发竞态(inval_counter 屏障)由 relation_row_cache_correctness.sh
# 压测覆盖(writer 高频摘条目 + reader 高频点查回填)。
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

OUT_DIR="${OUT_DIR:-/tmp/rowcache_backfill}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -A -t -q)

FAILURES=0
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
ok()   { echo "  ok:   $*"; }

BACKFILL_MSG="row cache: backfilled one row"

run_sql() {  # $1 = 日志文件名
  local log="$OUT_DIR/$1"
  shift
  "$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$log" 2>&1 <<EOSQL
SET client_min_messages = debug2;
SET search_path = row_cache_backfill, public;
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
$*
EOSQL
}

assert_backfilled() {  # $1 = 日志名, $2 = 场景
  if grep -q "$BACKFILL_MSG" "$OUT_DIR/$1"; then
    ok "$2: 回填发生"
  else
    fail "$2: 未见回填日志(见 $OUT_DIR/$1)"
  fi
}

assert_not_backfilled() {  # $1 = 日志名, $2 = 场景
  if grep -q "$BACKFILL_MSG" "$OUT_DIR/$1"; then
    fail "$2: 不应回填却回填了(见 $OUT_DIR/$1)"
  else
    ok "$2: 无回填(符合预期)"
  fi
}

assert_result() {  # $1 = 日志名, $2 = 期望行, $3 = 场景
  if grep -qx "$2" "$OUT_DIR/$1"; then
    ok "$3: 结果正确 ($2)"
  else
    fail "$3: 结果错误(期望 $2, 见 $OUT_DIR/$1)"
  fi
}

echo "=========================================="
echo " row cache backfill test (OUT_DIR=$OUT_DIR)"
echo "=========================================="

# ---------------------------------------------------------------
# setup
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$OUT_DIR/setup.log" 2>&1 <<'EOSQL'
SET client_min_messages = warning;
SET lock_timeout = '15s';
DROP SCHEMA IF EXISTS row_cache_backfill CASCADE;
CREATE SCHEMA row_cache_backfill;
CREATE TABLE row_cache_backfill.t (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO row_cache_backfill.t SELECT g, 'v' || g FROM generate_series(1, 100) g;
CREATE TABLE row_cache_backfill.unloaded (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO row_cache_backfill.unloaded SELECT g, 'u' || g FROM generate_series(1, 100) g;
ANALYZE row_cache_backfill.t;
ANALYZE row_cache_backfill.unloaded;
SELECT pg_load_relation_row_cache('row_cache_backfill.t');
EOSQL
echo " setup OK"

# ---------------------------------------------------------------
# ① 回填发生,且被第二次点查命中
# ---------------------------------------------------------------
run_sql prep1.log "UPDATE t SET v = 'w42' WHERE id = 42;"   # 摘掉条目

run_sql probe1.log "SELECT 'R1=' || v FROM t WHERE id = 42;"
assert_backfilled probe1.log "①a 摘条目后首次点查"
assert_result probe1.log "R1=w42" "①a 首次点查结果"

run_sql probe2.log "SELECT 'R2=' || v FROM t WHERE id = 42;"
assert_not_backfilled probe2.log "①b 第二次点查(命中回填条目, 不再回填)"
assert_result probe2.log "R2=w42" "①b 第二次点查结果"

# ---------------------------------------------------------------
# ② GUC 开关
# ---------------------------------------------------------------
run_sql prep2.log "UPDATE t SET v = 'w43' WHERE id = 43;"

run_sql off1.log "SET row_cache_backfill = off; SELECT 'R3=' || v FROM t WHERE id = 43;"
assert_not_backfilled off1.log "②a backfill=off 首次点查"
assert_result off1.log "R3=w43" "②a 结果"

run_sql off2.log "SET row_cache_backfill = off; SELECT 'R4=' || v FROM t WHERE id = 43;"
assert_not_backfilled off2.log "②b backfill=off 再次点查(持续 miss 也不回填)"

run_sql on1.log "SET row_cache_backfill = on; SELECT 'R5=' || v FROM t WHERE id = 43;"
assert_backfilled on1.log "②c 打开后回填"
assert_result on1.log "R5=w43" "②c 结果"

# ---------------------------------------------------------------
# ③ 回填后 DML 失效仍正确
# ---------------------------------------------------------------
run_sql dml.log "
UPDATE t SET v = 'x42' WHERE id = 42;
SELECT 'R6=' || v FROM t WHERE id = 42;
"
assert_result dml.log "R6=x42" "③ 回填条目被 DML 失效后点查"

# ---------------------------------------------------------------
# ④ 未 load 的表不回填
# ---------------------------------------------------------------
run_sql unloaded.log "SELECT 'R7=' || v FROM unloaded WHERE id = 42;"
assert_not_backfilled unloaded.log "④ 未 load 的表"
assert_result unloaded.log "R7=u42" "④ 结果"

# ---------------------------------------------------------------
# cleanup + verdict
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/cleanup.log" 2>&1 <<'EOSQL' || true
SET client_min_messages = warning;
SELECT pg_drop_relation_row_cache('row_cache_backfill.t');
DROP SCHEMA IF EXISTS row_cache_backfill CASCADE;
EOSQL

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS — 回填按需发生、可被命中、受 GUC 控制、失效语义不变"
  exit 0
else
  echo " FAIL — $FAILURES 项断言失败, 日志见 $OUT_DIR"
  exit 1
fi
