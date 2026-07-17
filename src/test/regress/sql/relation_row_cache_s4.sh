#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# 行缓存 S4 - 变长主键专项
#
# 覆盖:
#   1. text/varchar/bpchar/uuid/bytea 的短键、长键与 TOAST 键命中;
#   2. 复合变长键长度边界,避免 ('ab','c') / ('a','bc') 串键;
#   3. UPDATE/DELETE 失效与 miss 后按真实元组主键回填;
#   4. varchar 使用 text opclass、bpchar 忽略尾空格的语义;
#   5. numeric 与非确定性 collation 保持不缓存。
#
# 观测:PG 18 Index Scan 的 "Index Searches: 0" 表示行缓存在调用
# index AM 前返回;"Index Searches: 1" 表示回退原生 B-tree。
# ================================================================

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_s4}"
mkdir -p "$OUT_DIR"

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -A -t -q -v ON_ERROR_STOP=1)
FAILURES=0

ok()   { echo "  ok:   $*"; }
fail() { echo "  FAIL: $*"; FAILURES=$((FAILURES + 1)); }

SETTINGS="
SET client_min_messages = warning;
SET search_path = row_cache_s4, public;
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
"

run_sql() {
  local log="$OUT_DIR/$1"
  shift
  "$PSQL" "${PSQL_OPTS[@]}" >"$log" 2>&1 <<EOSQL
$SETTINGS
$*
EOSQL
}

assert_result() {
  local log="$OUT_DIR/$1" expected="$2" label="$3"
  if grep -qx "$expected" "$log"; then
    ok "$label: $expected"
  else
    fail "$label: 期望 $expected (见 $log)"
  fi
}

assert_index_searches() {
  local log="$OUT_DIR/$1" expected="$2" label="$3"
  if grep -Eq "Index Searches: ${expected}([^0-9]|$)" "$log"; then
    ok "$label: Index Searches=$expected"
  else
    fail "$label: 未观察到 Index Searches=$expected (见 $log)"
  fi
}

cleanup() {
  "$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/cleanup.log" 2>&1 <<'EOSQL' || true
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS row_cache_s4 CASCADE;
EOSQL
}
trap cleanup EXIT

echo "=========================================="
echo " row cache S4 test (OUT_DIR=$OUT_DIR)"
echo "=========================================="

"$PSQL" "${PSQL_OPTS[@]}" >"$OUT_DIR/setup.log" 2>&1 <<'EOSQL'
SET client_min_messages = warning;
DROP SCHEMA IF EXISTS row_cache_s4 CASCADE;
CREATE SCHEMA row_cache_s4;
SET search_path = row_cache_s4, public;

CREATE FUNCTION long_text_key() RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$
  SELECT string_agg(md5(g::text), '' ORDER BY g)
  FROM generate_series(1, 70) AS g
$$;

CREATE TABLE t_text (
  k text COLLATE "C" PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_text VALUES
  ('short', 'text-short'),
  (repeat('i', 28), 'text-inline-32'),
  (repeat('j', 29), 'text-tail-33'),
  (repeat('x', 200), 'text-long'),
  (long_text_key(), 'text-toast');

CREATE TABLE t_varchar (
  k varchar(300) COLLATE "C" PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_varchar VALUES
  ('varchar-short', 'varchar-short'),
  (repeat('v', 200), 'varchar-long');

CREATE TABLE t_bpchar (
  k char(12) COLLATE "C" PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_bpchar VALUES ('abc', 'bpchar-row');

CREATE TABLE t_uuid (
  k uuid PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_uuid VALUES
  ('11111111-2222-3333-4444-555555555555', 'uuid-row');

CREATE TABLE t_bytea (
  k bytea PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_bytea VALUES
  (decode(repeat('00ff', 80), 'hex'), 'bytea-row');

CREATE TABLE t_composite (
  a text COLLATE "C",
  b text COLLATE "C",
  v text NOT NULL,
  PRIMARY KEY (a, b)
);
INSERT INTO t_composite VALUES
  ('ab', 'c', 'left-boundary'),
  ('a', 'bc', 'right-boundary'),
  (repeat('m', 100), repeat('n', 100), 'composite-long');

CREATE TABLE t_numeric (
  k numeric PRIMARY KEY,
  v text NOT NULL
);
INSERT INTO t_numeric VALUES (1.25, 'numeric-fallback');

ANALYZE t_text;
ANALYZE t_varchar;
ANALYZE t_bpchar;
ANALYZE t_uuid;
ANALYZE t_bytea;
ANALYZE t_composite;
ANALYZE t_numeric;

SELECT pg_load_relation_row_cache('row_cache_s4.t_text');
SELECT pg_load_relation_row_cache('row_cache_s4.t_varchar');
SELECT pg_load_relation_row_cache('row_cache_s4.t_bpchar');
SELECT pg_load_relation_row_cache('row_cache_s4.t_uuid');
SELECT pg_load_relation_row_cache('row_cache_s4.t_bytea');
SELECT pg_load_relation_row_cache('row_cache_s4.t_composite');
SELECT pg_load_relation_row_cache('row_cache_s4.t_numeric');
EOSQL
echo " setup OK"

# 1. 单列 P1 类型:结果正确且真实命中。
run_sql text_short.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = 'short';
SELECT 'T1=' || v FROM t_text WHERE k = 'short';
"
assert_index_searches text_short.log 0 "text 短键命中"
assert_result text_short.log "T1=text-short" "text 短键结果"

run_sql text_inline_boundary.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = repeat('i', 28);
SELECT 'I1=' || v FROM t_text WHERE k = repeat('i', 28);
"
assert_index_searches text_inline_boundary.log 0 "text 规范化后 32 字节内联命中"
assert_result text_inline_boundary.log "I1=text-inline-32" "text 内联边界结果"

run_sql text_tail_boundary.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = repeat('j', 29);
SELECT 'I2=' || v FROM t_text WHERE k = repeat('j', 29);
"
assert_index_searches text_tail_boundary.log 0 "text 规范化后 33 字节尾随命中"
assert_result text_tail_boundary.log "I2=text-tail-33" "text 尾随边界结果"

run_sql text_long.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = repeat('x', 200);
SELECT 'T2=' || v FROM t_text WHERE k = repeat('x', 200);
"
assert_index_searches text_long.log 0 "text 长键尾随存储命中"
assert_result text_long.log "T2=text-long" "text 长键结果"

run_sql text_toast.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = long_text_key();
SELECT 'T3=' || v FROM t_text WHERE k = long_text_key();
"
assert_index_searches text_toast.log 0 "text TOAST 键 detoast 后命中"
assert_result text_toast.log "T3=text-toast" "text TOAST 键结果"

run_sql varchar.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_varchar WHERE k = repeat('v', 200);
SELECT 'V1=' || v FROM t_varchar WHERE k = repeat('v', 200);
"
assert_index_searches varchar.log 0 "varchar/text opclass 兼容命中"
assert_result varchar.log "V1=varchar-long" "varchar 结果"

run_sql bpchar.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_bpchar WHERE k = 'abc'::bpchar;
SELECT 'C1=' || v FROM t_bpchar WHERE k = 'abc'::bpchar;
"
assert_index_searches bpchar.log 0 "bpchar 去尾空格后命中"
assert_result bpchar.log "C1=bpchar-row" "bpchar 结果"

run_sql uuid.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_uuid
  WHERE k = '11111111-2222-3333-4444-555555555555'::uuid;
SELECT 'U1=' || v FROM t_uuid
  WHERE k = '11111111-2222-3333-4444-555555555555'::uuid;
"
assert_index_searches uuid.log 0 "uuid 固定 16 字节命中"
assert_result uuid.log "U1=uuid-row" "uuid 结果"

run_sql bytea.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_bytea WHERE k = decode(repeat('00ff', 80), 'hex');
SELECT 'B1=' || v FROM t_bytea
  WHERE k = decode(repeat('00ff', 80), 'hex');
"
assert_index_searches bytea.log 0 "bytea 长键命中"
assert_result bytea.log "B1=bytea-row" "bytea 结果"

# 2. 复合变长键必须有边界,两组相同拼接字节不得串行。
run_sql composite_left.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_composite WHERE a = 'ab' AND b = 'c';
SELECT 'K1=' || v FROM t_composite WHERE a = 'ab' AND b = 'c';
"
assert_index_searches composite_left.log 0 "复合键左边界命中"
assert_result composite_left.log "K1=left-boundary" "复合键左边界结果"

run_sql composite_right.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_composite WHERE a = 'a' AND b = 'bc';
SELECT 'K2=' || v FROM t_composite WHERE a = 'a' AND b = 'bc';
"
assert_index_searches composite_right.log 0 "复合键右边界命中"
assert_result composite_right.log "K2=right-boundary" "复合键右边界结果"

run_sql composite_long.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_composite
  WHERE a = repeat('m', 100) AND b = repeat('n', 100);
SELECT 'K3=' || v FROM t_composite
  WHERE a = repeat('m', 100) AND b = repeat('n', 100);
"
assert_index_searches composite_long.log 0 "复合长键命中"
assert_result composite_long.log "K3=composite-long" "复合长键结果"

# 3. DML 摘链、真实元组回填及再次命中。
run_sql dml_update.log "
UPDATE t_text SET v = 'text-toast-updated' WHERE k = long_text_key();
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = long_text_key();
SELECT 'D1=' || v FROM t_text WHERE k = long_text_key();
"
assert_index_searches dml_update.log 1 "TOAST 键 UPDATE 后首次回退"
assert_result dml_update.log "D1=text-toast-updated" "TOAST 键 UPDATE 结果"

run_sql dml_backfill.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_text WHERE k = long_text_key();
SELECT 'D2=' || v FROM t_text WHERE k = long_text_key();
"
assert_index_searches dml_backfill.log 0 "TOAST 键回填后再次命中"
assert_result dml_backfill.log "D2=text-toast-updated" "回填后结果"

run_sql dml_key_update.log "
UPDATE t_varchar SET k = repeat('w', 200), v = 'varchar-moved'
  WHERE k = repeat('v', 200);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_varchar WHERE k = repeat('v', 200);
SELECT 'D3=' || count(*) FROM t_varchar WHERE k = repeat('v', 200);
"
assert_index_searches dml_key_update.log 1 "变长主键 UPDATE 后旧键回退"
assert_result dml_key_update.log "D3=0" "变长主键旧键消失"

run_sql dml_new_key.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_varchar WHERE k = repeat('w', 200);
SELECT 'D4=' || v FROM t_varchar WHERE k = repeat('w', 200);
"
assert_index_searches dml_new_key.log 1 "变长主键新键首次回退并回填"
assert_result dml_new_key.log "D4=varchar-moved" "变长主键新键结果"

run_sql dml_new_key_hit.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_varchar WHERE k = repeat('w', 200);
"
assert_index_searches dml_new_key_hit.log 0 "变长主键新键回填后命中"

run_sql dml_delete.log "
DELETE FROM t_bytea WHERE k = decode(repeat('00ff', 80), 'hex');
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_bytea WHERE k = decode(repeat('00ff', 80), 'hex');
SELECT 'D5=' || count(*) FROM t_bytea
  WHERE k = decode(repeat('00ff', 80), 'hex');
"
assert_index_searches dml_delete.log 1 "bytea DELETE 后回退原生"
assert_result dml_delete.log "D5=0" "bytea DELETE 结果"

# 4. P2/P3 不在本阶段:numeric 继续不缓存。
run_sql numeric.log "
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
  SELECT v FROM t_numeric WHERE k = 1.25;
SELECT 'N1=' || v FROM t_numeric WHERE k = 1.25;
"
assert_index_searches numeric.log 1 "numeric 保持回退"
assert_result numeric.log "N1=numeric-fallback" "numeric 结果"

# 5. ICU 可用时验证非确定性 collation 明确拒绝。目录中可能保留 ICU
# provider 条目，因此以实际创建能力为准。
if run_sql nondet_setup.log "
  CREATE COLLATION nondet_ci (
    provider = icu,
    locale = 'und-u-ks-level2',
    deterministic = false
  );
  CREATE TABLE t_nondet (
    k text COLLATE nondet_ci PRIMARY KEY,
    v text NOT NULL
  );
  INSERT INTO t_nondet VALUES ('Alpha', 'nondet-row');
  ANALYZE t_nondet;
  SELECT pg_load_relation_row_cache('row_cache_s4.t_nondet');
  "; then
  run_sql nondet.log "
  EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
    SELECT v FROM t_nondet WHERE k = 'Alpha';
  SELECT 'N2=' || v FROM t_nondet WHERE k = 'alpha';
  "
  assert_index_searches nondet.log 1 "非确定性 collation 保持回退"
  assert_result nondet.log "N2=nondet-row" "非确定性 collation 原生语义"
elif grep -q "ICU is not supported" "$OUT_DIR/nondet_setup.log"; then
  ok "构建未启用 ICU,非确定性 collation 场景跳过"
else
  fail "非确定性 collation 测试准备失败 (见 $OUT_DIR/nondet_setup.log)"
fi

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS - S4 变长主键、TOAST、复合边界、DML/回填与范围闸门全部正确"
  exit 0
else
  echo " FAIL - $FAILURES 项断言失败,日志见 $OUT_DIR"
  exit 1
fi
