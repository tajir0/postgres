#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# V4 行缓存 — S2 DDL schema 指纹回归
#
# 验证"信号与裁决分离":relcache 失效是脏信号(VACUUM/ANALYZE 的
# pg_class inplace 更新、GRANT、建索引都会触发),真正的失效裁决由
# bind 慢路径的 schema 指纹 (relfilenumber, tupdesc_hash) 完成。
#
# 断言两个方向:
#   ① 例行维护(VACUUM / ANALYZE / GRANT / 建索引)之后:
#      - 不出现 "schema fingerprint mismatch" 日志(缓存零损失);
#      - 点查结果正确。
#   ② 真 DDL(ADD COLUMN / ALTER TYPE / TRUNCATE / VACUUM FULL)之后:
#      - 出现 mismatch 日志(缓存被正确失效);
#      - 点查结果正确(走原生路径);
#      - 重新 load 后恢复可用。
#   ③ indisunique 门槛:DROP CONSTRAINT pkey(指纹不变,缓存保持
#      ENABLED)后经同列"非唯一"索引查询,必须能看到新插入的重复键
#      (缓存单行命中不得吞掉第二行)。
#
# 观测手段:client_min_messages=debug1 + grep 失效日志。
#
# Usage / env: 同 relation_row_cache_correctness.sh
# Exit 0 = PASS
# ================================================================

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_ddl}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -A -t -q)

FAILURES=0
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }
ok()   { echo "  ok:   $*"; }

MISMATCH_MSG="schema fingerprint mismatch"

# 跑一段 SQL,stdout+stderr 进日志文件;client_min_messages=debug1
# 使指纹失效日志可见。
run_sql() {  # $1 = 日志文件名(OUT_DIR 下)
  local log="$OUT_DIR/$1"
  shift
  "$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$log" 2>&1 <<EOSQL
SET client_min_messages = debug1;
SET search_path = row_cache_ddl, public;
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
$*
EOSQL
}

assert_no_mismatch() {  # $1 = 日志名, $2 = 场景描述
  if grep -q "$MISMATCH_MSG" "$OUT_DIR/$1"; then
    fail "$2: 出现了指纹失效(例行维护被误伤)"
  else
    ok "$2: 无指纹失效, 缓存零损失"
  fi
}

assert_mismatch() {  # $1 = 日志名, $2 = 场景描述
  if grep -q "$MISMATCH_MSG" "$OUT_DIR/$1"; then
    ok "$2: 指纹失效正确触发"
  else
    fail "$2: 未触发指纹失效(真 DDL 漏检)"
  fi
}

assert_result() {  # $1 = 日志名, $2 = 期望行(精确), $3 = 场景描述
  if grep -qx "$2" "$OUT_DIR/$1"; then
    ok "$3: 结果正确 ($2)"
  else
    fail "$3: 结果错误(期望 $2, 见 $OUT_DIR/$1)"
  fi
}

echo "=========================================="
echo " row cache DDL fingerprint test"
echo " OUT_DIR=${OUT_DIR}"
echo "=========================================="

# ---------------------------------------------------------------
# setup
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" -v ON_ERROR_STOP=1 >"$OUT_DIR/setup.log" 2>&1 <<'EOSQL'
SET client_min_messages = warning;
SET lock_timeout = '15s';
DROP SCHEMA IF EXISTS row_cache_ddl CASCADE;
CREATE SCHEMA row_cache_ddl;
CREATE TABLE row_cache_ddl.t (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO row_cache_ddl.t SELECT g, 'v' || g FROM generate_series(1, 100) g;
ANALYZE row_cache_ddl.t;
SELECT pg_load_relation_row_cache('row_cache_ddl.t');
EOSQL
echo " setup OK"

# ---------------------------------------------------------------
# ① 例行维护:不得触发指纹失效
# ---------------------------------------------------------------
run_sql routine.log "
VACUUM t;
ANALYZE t;
GRANT SELECT ON t TO PUBLIC;
CREATE INDEX t_v_idx ON t (v);
SELECT 'R1=' || v FROM t WHERE id = 42;
"
assert_no_mismatch routine.log "VACUUM/ANALYZE/GRANT/建索引"
assert_result routine.log "R1=v42" "例行维护后点查"

# ---------------------------------------------------------------
# ② 真 DDL:必须触发指纹失效,且结果正确
# ---------------------------------------------------------------

# ADD COLUMN(tupdesc 变)
run_sql add_col.log "
ALTER TABLE t ADD COLUMN extra int4 DEFAULT 7;
SELECT 'R2=' || v || '/' || extra FROM t WHERE id = 42;
"
assert_mismatch add_col.log "ADD COLUMN"
assert_result add_col.log "R2=v42/7" "ADD COLUMN 后点查"

run_sql reload1.log "SELECT pg_load_relation_row_cache('row_cache_ddl.t'); SELECT 'R3=' || extra FROM t WHERE id = 42;"
assert_result reload1.log "R3=7" "DDL 后重新 load 可用"

# ALTER COLUMN TYPE(tupdesc 变;int4→int8 还会重写 → relfilenode 也变)
run_sql alter_type.log "
ALTER TABLE t ALTER COLUMN extra TYPE int8;
SELECT 'R4=' || extra FROM t WHERE id = 42;
"
assert_mismatch alter_type.log "ALTER COLUMN TYPE"
assert_result alter_type.log "R4=7" "ALTER TYPE 后点查"

# TRUNCATE(relfilenode 变)
run_sql reload2.log "SELECT pg_load_relation_row_cache('row_cache_ddl.t');"
run_sql truncate.log "
TRUNCATE t;
SELECT 'R5=' || count(*) FROM t;
"
assert_mismatch truncate.log "TRUNCATE"
assert_result truncate.log "R5=0" "TRUNCATE 后计数"

# VACUUM FULL(relfilenode 变,TID 全变)
run_sql refill.log "
INSERT INTO t SELECT g, 'v' || g, 7 FROM generate_series(1, 100) g;
SELECT pg_load_relation_row_cache('row_cache_ddl.t');
"
run_sql vacfull.log "
VACUUM FULL t;
SELECT 'R6=' || v FROM t WHERE id = 42;
"
assert_mismatch vacfull.log "VACUUM FULL"
assert_result vacfull.log "R6=v42" "VACUUM FULL 后点查"

# ---------------------------------------------------------------
# ③ indisunique 门槛:删主键约束后经非唯一索引不得漏读重复键
#    (指纹不变——relfilenode/tupdesc 都没动,缓存保持 ENABLED,
#    正确性由执行器侧"只允许唯一索引探测"保证。)
# ---------------------------------------------------------------
run_sql uniq_setup.log "
CREATE TABLE t2 (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO t2 SELECT g, 'v' || g FROM generate_series(1, 100) g;
SELECT pg_load_relation_row_cache('row_cache_ddl.t2');
SELECT 'W1=' || v FROM t2 WHERE id = 42;
"
assert_result uniq_setup.log "W1=v42" "t2 缓存预热"

run_sql uniq_gate.log "
ALTER TABLE t2 DROP CONSTRAINT t2_pkey;
CREATE INDEX t2_id_idx ON t2 (id);
INSERT INTO t2 VALUES (42, 'dup');
SELECT 'W2=' || count(*) FROM t2 WHERE id = 42;
"
assert_result uniq_gate.log "W2=2" "删主键+非唯一索引: 重复键两行都可见"

# ---------------------------------------------------------------
# cleanup + verdict
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/cleanup.log" 2>&1 <<'EOSQL' || true
SET client_min_messages = warning;
SELECT pg_drop_relation_row_cache('row_cache_ddl.t');
SELECT pg_drop_relation_row_cache('row_cache_ddl.t2');
DROP SCHEMA IF EXISTS row_cache_ddl CASCADE;
EOSQL

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS — 指纹只对真 DDL 失效, 例行维护零损失, 唯一性门槛有效"
  exit 0
else
  echo " FAIL — $FAILURES 项断言失败, 日志见 $OUT_DIR"
  exit 1
fi
