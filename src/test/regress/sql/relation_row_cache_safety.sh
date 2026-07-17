#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# V4 行缓存 — 键安全回归(review 四修复的行为固定)
#
#   ① 跨库隔离:模板克隆库中表 OID / relfilenode / tupdesc 完全相同,
#      schema 指纹无法区分——缓存键必须含数据库 OID。断言 db1 加载后
#      db2 点查得到 db2 自己的数据,两库缓存并存互不串。
#   ② 跨类型 ScanKey:int4 主键查 id = 4294967296(int8 字面量),
#      参数按缓存列宽截断会误命中 id=0。断言返回 0 行;同值跨类型
#      (id = 1::int8)回退原生仍返回正确行。
#   ③ 部分唯一索引:planner 会消除被索引谓词蕴含的 qual,缓存命中
#      绕过索引后无人复查谓词。断言 WHERE id=.. AND active 不返回
#      active=false 的缓存行。
#   ④ 延迟唯一索引:indimmediate=false 时事务内允许暂时重复,缓存
#      单行命中会漏行。断言事务内插入重复键后 count(*) = 2。
#   ⑤ 权限:load/drop 需要 MAINTAIN(owner 隐含);只读用户不得借
#      load 取得 ShareLock 阻塞 DML。
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
export PGUSER PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_safety}"
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
SET jit = off;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
"

run_db() {  # $1 = 数据库, $2 = 日志名, stdin = SQL
  local db="$1" log="$OUT_DIR/$2"
  shift 2
  "$PSQL" "${PSQL_OPTS[@]}" -d "$db" -v ON_ERROR_STOP=1 >"$log" 2>&1 <<EOSQL
$SETTINGS
$*
EOSQL
}

assert_result() {  # $1 = 日志名, $2 = 期望行, $3 = 场景
  if grep -qx "$2" "$OUT_DIR/$1"; then
    ok "$3: 结果正确 ($2)"
  else
    fail "$3: 结果错误(期望 $2, 见 $OUT_DIR/$1)"
  fi
}

echo "=========================================="
echo " row cache key-safety test (OUT_DIR=$OUT_DIR)"
echo "=========================================="

# ---------------------------------------------------------------
# ① 跨库隔离(模板克隆)
# ---------------------------------------------------------------
"$PSQL" "${PSQL_OPTS[@]}" -d "$PGDATABASE" >"$OUT_DIR/dbsetup.log" 2>&1 <<'EOSQL' || true
DROP DATABASE IF EXISTS rc_safety_db2;
DROP DATABASE IF EXISTS rc_safety_db1;
EOSQL

"$PSQL" "${PSQL_OPTS[@]}" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE rc_safety_db1;" >>"$OUT_DIR/dbsetup.log" 2>&1

run_db rc_safety_db1 db1_init.log "
CREATE TABLE t (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO t SELECT g, 'db1-' || g FROM generate_series(1, 100) g;
ANALYZE t;
"
sleep 1	# 模板克隆要求源库无活动连接

"$PSQL" "${PSQL_OPTS[@]}" -d "$PGDATABASE" -v ON_ERROR_STOP=1 \
  -c "CREATE DATABASE rc_safety_db2 TEMPLATE rc_safety_db1;" \
  >>"$OUT_DIR/dbsetup.log" 2>&1

# 克隆库中同名表的 OID / relfilenode 与源库相同;写入不同数据以分辨
run_db rc_safety_db2 db2_init.log "UPDATE t SET v = 'db2-' || id;"

# db1 加载缓存并点查
run_db rc_safety_db1 db1_load.log "
SELECT pg_load_relation_row_cache('t');
SELECT 'D1=' || v FROM t WHERE id = 42;
"
assert_result db1_load.log "D1=db1-42" "① db1 加载后点查"

# db2 点查:未加载,必须回退原生取到 db2 自己的数据
# (修复前:同 relid+pkey 命中 db1 的缓存条目,返回 db1-42)
run_db rc_safety_db2 db2_probe.log "SELECT 'D2=' || v FROM t WHERE id = 42;"
assert_result db2_probe.log "D2=db2-42" "① db2 点查不得串到 db1 缓存"

# db2 也加载:两库缓存并存,各查各的
run_db rc_safety_db2 db2_load.log "
SELECT pg_load_relation_row_cache('t');
SELECT 'D3=' || v FROM t WHERE id = 42;
"
assert_result db2_load.log "D3=db2-42" "① db2 加载后点查"

run_db rc_safety_db1 db1_recheck.log "SELECT 'D4=' || v FROM t WHERE id = 42;"
assert_result db1_recheck.log "D4=db1-42" "① 两库缓存并存, db1 不受影响"

# ---------------------------------------------------------------
# ② 跨类型 ScanKey(int8 参数打 int4 主键)
# ---------------------------------------------------------------
run_db "$PGDATABASE" xtype_setup.log "
DROP SCHEMA IF EXISTS rc_safety CASCADE;
CREATE SCHEMA rc_safety;
SET search_path = rc_safety;
CREATE TABLE tx (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO tx VALUES (0, 'zero');   -- 截断误命中的目标行
INSERT INTO tx SELECT g, 'v' || g FROM generate_series(1, 100) g;
ANALYZE tx;
SELECT pg_load_relation_row_cache('rc_safety.tx');
"

# 4294967296 = 2^32,int8 字面量;按 4 字节截断 = 0 → 修复前误命中 id=0
run_db "$PGDATABASE" xtype1.log "
SET search_path = rc_safety;
SELECT 'X1=' || count(*) FROM tx WHERE id = 4294967296;
"
assert_result xtype1.log "X1=0" "② int8 超界参数不得截断误命中"

run_db "$PGDATABASE" xtype2.log "
SET search_path = rc_safety;
SELECT 'X2=' || v FROM tx WHERE id = 1::int8;
"
assert_result xtype2.log "X2=v1" "② int8 同值参数回退原生, 结果正确"

run_db "$PGDATABASE" xtype3.log "
SET search_path = rc_safety;
SELECT 'X3=' || v FROM tx WHERE id = 1;
"
assert_result xtype3.log "X3=v1" "② 同类型点查仍正常"

# ---------------------------------------------------------------
# ③ 部分唯一索引
# ---------------------------------------------------------------
run_db "$PGDATABASE" partial_setup.log "
SET search_path = rc_safety;
CREATE TABLE tp (id int4 PRIMARY KEY, active bool NOT NULL, v text NOT NULL);
INSERT INTO tp VALUES (1, false, 'inactive');
INSERT INTO tp SELECT g, true, 'v' || g FROM generate_series(2, 100) g;
ANALYZE tp;
SELECT pg_load_relation_row_cache('rc_safety.tp');
ALTER TABLE tp DROP CONSTRAINT tp_pkey;   -- 指纹不变, 缓存保持 ENABLED
CREATE UNIQUE INDEX tp_partial ON tp (id) WHERE active;
"

# planner 会消除被谓词蕴含的 active 条件;修复前缓存命中返回 active=false 的行
run_db "$PGDATABASE" partial1.log "
SET search_path = rc_safety;
SELECT 'P1=' || count(*) FROM tp WHERE id = 1 AND active;
"
assert_result partial1.log "P1=0" "③ 部分唯一索引不得返回谓词外的缓存行"

# ---------------------------------------------------------------
# ④ 延迟唯一索引
# ---------------------------------------------------------------
run_db "$PGDATABASE" defer_setup.log "
SET search_path = rc_safety;
CREATE TABLE td (id int4 PRIMARY KEY, v text NOT NULL);
INSERT INTO td SELECT g, 'v' || g FROM generate_series(1, 100) g;
ANALYZE td;
SELECT pg_load_relation_row_cache('rc_safety.td');
ALTER TABLE td DROP CONSTRAINT td_pkey;
ALTER TABLE td ADD CONSTRAINT td_uk UNIQUE (id) DEFERRABLE INITIALLY DEFERRED;
"

# 事务内插入重复键(延迟检查允许):同键两行都可见,缓存单行命中会漏行
run_db "$PGDATABASE" defer1.log "
SET search_path = rc_safety;
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO td VALUES (1, 'dup');
SELECT 'F1=' || count(*) FROM td WHERE id = 1;
ROLLBACK;
"
assert_result defer1.log "F1=2" "④ 延迟唯一索引下事务内重复键两行都可见"

# ---------------------------------------------------------------
# cleanup + verdict
# ---------------------------------------------------------------
# ⑤ 权限:load 持 ShareLock 挡写,只读(SELECT)用户不得调用;
#    需要 MAINTAIN(owner 隐含)。drop 对称。
# ---------------------------------------------------------------
run_db "$PGDATABASE" priv_setup.log "
DROP ROLE IF EXISTS rc_safety_reader;
CREATE ROLE rc_safety_reader LOGIN;
GRANT USAGE ON SCHEMA rc_safety TO rc_safety_reader;
GRANT SELECT ON rc_safety.tx TO rc_safety_reader;
"
run_db "$PGDATABASE" priv_reader.log "
SET ROLE rc_safety_reader;
SELECT 'P1=' || pg_load_relation_row_cache('rc_safety.tx');
SELECT 'P2=' || pg_drop_relation_row_cache('rc_safety.tx');
RESET ROLE;
"
assert_result priv_reader.log "P1=false" "⑤ 只读用户 load 被拒(需 MAINTAIN)"
assert_result priv_reader.log "P2=false" "⑤ 只读用户 drop 被拒"
run_db "$PGDATABASE" priv_maintain.log "
GRANT MAINTAIN ON rc_safety.tx TO rc_safety_reader;
SET ROLE rc_safety_reader;
SELECT 'P3=' || pg_load_relation_row_cache('rc_safety.tx');
SELECT 'P4=' || pg_drop_relation_row_cache('rc_safety.tx');
RESET ROLE;
"
assert_result priv_maintain.log "P3=true" "⑤ MAINTAIN 授权后 load 放行"
assert_result priv_maintain.log "P4=true" "⑤ MAINTAIN 授权后 drop 放行"
run_db "$PGDATABASE" priv_cleanup.log "
REVOKE ALL ON rc_safety.tx FROM rc_safety_reader;
REVOKE USAGE ON SCHEMA rc_safety FROM rc_safety_reader;
DROP ROLE rc_safety_reader;
" || true

# ---------------------------------------------------------------
run_db "$PGDATABASE" cleanup.log "
SELECT pg_drop_relation_row_cache('rc_safety.tx');
SELECT pg_drop_relation_row_cache('rc_safety.tp');
SELECT pg_drop_relation_row_cache('rc_safety.td');
DROP SCHEMA IF EXISTS rc_safety CASCADE;
" || true
"$PSQL" "${PSQL_OPTS[@]}" -d "$PGDATABASE" >>"$OUT_DIR/cleanup.log" 2>&1 <<'EOSQL' || true
DROP DATABASE IF EXISTS rc_safety_db2;
DROP DATABASE IF EXISTS rc_safety_db1;
EOSQL

echo "=========================================="
if [[ $FAILURES -eq 0 ]]; then
  echo " PASS — 跨库隔离 / 跨类型回退 / 部分与延迟唯一索引资格全部正确"
  exit 0
else
  echo " FAIL — $FAILURES 项断言失败, 日志见 $OUT_DIR"
  exit 1
fi
