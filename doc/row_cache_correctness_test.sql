\set ON_ERROR_STOP on
SET client_min_messages = notice;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

DROP SCHEMA IF EXISTS row_cache_correctness CASCADE;
CREATE SCHEMA row_cache_correctness;
SET search_path = row_cache_correctness, public;

-- 1) 建宽表，覆盖多种数据类型
CREATE TABLE test_tbl
(
    id    bigint PRIMARY KEY,
    c_int integer,
    c_txt text,
    c_num numeric(12,2),
    c_bool boolean,
    c_ts  timestamptz,
    c_json jsonb
);

-- 2) 插入测试数据，包含正常值、NULL、空字符串、边界值
INSERT INTO test_tbl (id, c_int, c_txt, c_num, c_bool, c_ts, c_json) VALUES
    (1, 42, 'hello world', 123.45, true, '2024-01-01 00:00:00+00', '{"a":1}'),
    (2, -1, '', 0.00, false, '1970-01-01 00:00:00+00', '[]'),
    (3, NULL, NULL, NULL, NULL, NULL, NULL),
    (4, 2147483647, repeat('x', 1000), 9999999999.99, true, '2099-12-31 23:59:59+00', '{"nested":{"deep":true}}'),
    (5, -2147483648, 'special chars: \t\n''quotes"', -9999999999.99, false, '2000-06-15 12:30:00+08', 'null'),
    (6, 0, 'a', 0.01, true, '2024-06-15 00:00:00+00', '[1,2,3]'),
    (7, 100, 'payload-7', 77.77, false, '2024-03-01 08:00:00+00', '{"k":"v"}'),
    (8, 200, 'payload-8', 88.88, true, '2024-03-02 09:00:00+00', '{}'),
    (9, 300, NULL, 99.99, NULL, '2024-03-03 10:00:00+00', '"string_json"'),
    (10, NULL, 'has-int-null', NULL, false, NULL, '{"x":null}');

-- 再灌一批正常数据
INSERT INTO test_tbl (id, c_int, c_txt, c_num, c_bool, c_ts, c_json)
SELECT g,
       (g % 1000)::int,
       'row-' || g || '-' || substr(md5(g::text), 1, 16),
       ((g % 100000)::numeric / 100.0)::numeric(12,2),
       ((g % 2) = 0),
       timestamptz '2024-01-01 00:00:00+00' + (g % 86400) * interval '1 second',
       jsonb_build_object('id', g, 'mod', g % 10)
FROM generate_series(11, 10000) AS g;

ANALYZE test_tbl;

-- 3) 定义一组探测 id（覆盖手工插入的边界行 + 随机正常行）
CREATE TEMP TABLE probe_ids (id bigint);
INSERT INTO probe_ids VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),
    (11),(100),(500),(999),(1234),(5000),(7777),(9999),(10000);

-- ============================================================
-- 测试一：全列对比（SELECT *）
-- ============================================================

-- 无缓存：通过索引点查取全部行
SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.*
INTO TEMP nocache_full
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

-- 加载缓存后再查一遍
SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.*
INTO TEMP cache_full
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

-- 对比：两个方向的 EXCEPT 都应返回 0 行
SELECT 'full_columns' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM nocache_full EXCEPT SELECT * FROM cache_full) d
UNION ALL
SELECT 'full_columns', 'cache_minus_nocache', count(*)
FROM (SELECT * FROM cache_full EXCEPT SELECT * FROM nocache_full) d;

-- ============================================================
-- 测试二：单列对比（只取 text 列，验证变长 Datum）
-- ============================================================

SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.id, t.c_txt
INTO TEMP nocache_txt
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.id, t.c_txt
INTO TEMP cache_txt
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

SELECT 'text_column' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM nocache_txt EXCEPT SELECT * FROM cache_txt) d
UNION ALL
SELECT 'text_column', 'cache_minus_nocache', count(*)
FROM (SELECT * FROM cache_txt EXCEPT SELECT * FROM nocache_txt) d;

-- ============================================================
-- 测试三：pg_column_size 对比（验证元组结构一致）
-- ============================================================

SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.id, pg_column_size(t.*) AS colsize
INTO TEMP nocache_size
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.id, pg_column_size(t.*) AS colsize
INTO TEMP cache_size
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
ORDER BY t.id;

SELECT 'column_size' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM nocache_size EXCEPT SELECT * FROM cache_size) d
UNION ALL
SELECT 'column_size', 'cache_minus_nocache', count(*)
FROM (SELECT * FROM cache_size EXCEPT SELECT * FROM nocache_size) d;

-- ============================================================
-- 测试四：大批量 LATERAL 点查对比（1 万行全覆盖）
-- ============================================================

SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.*
INTO TEMP nocache_bulk
FROM generate_series(1, 10000) AS g
JOIN test_tbl t ON t.id = g
ORDER BY t.id;

SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
SELECT t.*
INTO TEMP cache_bulk
FROM generate_series(1, 10000) AS g
JOIN test_tbl t ON t.id = g
ORDER BY t.id;

SELECT 'bulk_10k' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM nocache_bulk EXCEPT SELECT * FROM cache_bulk) d
UNION ALL
SELECT 'bulk_10k', 'cache_minus_nocache', count(*)
FROM (SELECT * FROM cache_bulk EXCEPT SELECT * FROM nocache_bulk) d;

-- ============================================================
-- 测试五:FOR UPDATE(单会话语义)
--   缓存命中路径只提供"元组 + 真实 TID";加锁由上层 LockRows 重读
--   真实堆完成。本节验证单会话内 cache/nocache 的 FOR UPDATE 结果
--   等价,以及同事务自更新的可见性。
--   两会话并发场景(真实行锁 / EPQ / RR 40001)不可能在单会话 SQL
--   里表达,见 src/test/regress/sql/relation_row_cache_forupdate.sh。
-- ============================================================

-- 5a) 无缓存 FOR UPDATE 基线
SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
CREATE TEMP TABLE nocache_fu (LIKE test_tbl);
BEGIN;
INSERT INTO nocache_fu
SELECT t.*
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
FOR UPDATE OF t;
COMMIT;

-- 5b) 缓存命中 FOR UPDATE
SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
CREATE TEMP TABLE cache_fu (LIKE test_tbl);
BEGIN;
INSERT INTO cache_fu
SELECT t.*
FROM probe_ids p
JOIN test_tbl t ON t.id = p.id
FOR UPDATE OF t;
COMMIT;

SELECT 'for_update' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
FROM (SELECT * FROM nocache_fu EXCEPT SELECT * FROM cache_fu) d
UNION ALL
SELECT 'for_update', 'cache_minus_nocache', count(*)
FROM (SELECT * FROM cache_fu EXCEPT SELECT * FROM nocache_fu) d;

-- 5c) 同事务自更新可见性:UPDATE 执行时 DML 钩子已摘条目,同会话的
--     点查与 FOR UPDATE 必须看到自己的新值;ROLLBACK 后必须恢复旧值
--     (钩子在执行时摘条目,回滚后只是多付一次 miss,不会出错值)。
SELECT pg_load_relation_row_cache('row_cache_correctness.test_tbl');
BEGIN;
UPDATE test_tbl SET c_int = 424242 WHERE id = 7;
SELECT 'fu_self_update' AS test, 'plain_read_sees_own' AS direction,
       CASE WHEN count(*) = 1 THEN 0 ELSE 1 END AS diff_rows
FROM test_tbl WHERE id = 7 AND c_int = 424242;
SELECT 'fu_self_update' AS test, 'for_update_sees_own' AS direction,
       CASE WHEN count(*) = 1 THEN 0 ELSE 1 END AS diff_rows
FROM (SELECT c_int FROM test_tbl WHERE id = 7 FOR UPDATE) s
WHERE s.c_int = 424242;
ROLLBACK;
SELECT 'fu_self_update' AS test, 'rollback_restores' AS direction,
       CASE WHEN count(*) = 1 THEN 0 ELSE 1 END AS diff_rows
FROM test_tbl WHERE id = 7 AND c_int = 100;

-- ============================================================
-- 汇总
-- ============================================================
\echo ''
\echo '=== Correctness Test Summary ==='
\echo 'All diff_rows should be 0.'
\echo ''

SELECT * FROM (
    SELECT 'full_columns' AS test, 'nocache_minus_cache' AS direction, count(*) AS diff_rows
    FROM (SELECT * FROM nocache_full EXCEPT SELECT * FROM cache_full) d
    UNION ALL SELECT 'full_columns', 'cache_minus_nocache', count(*)
    FROM (SELECT * FROM cache_full EXCEPT SELECT * FROM nocache_full) d
    UNION ALL SELECT 'text_column', 'nocache_minus_cache', count(*)
    FROM (SELECT * FROM nocache_txt EXCEPT SELECT * FROM cache_txt) d
    UNION ALL SELECT 'text_column', 'cache_minus_nocache', count(*)
    FROM (SELECT * FROM cache_txt EXCEPT SELECT * FROM nocache_txt) d
    UNION ALL SELECT 'column_size', 'nocache_minus_cache', count(*)
    FROM (SELECT * FROM nocache_size EXCEPT SELECT * FROM cache_size) d
    UNION ALL SELECT 'column_size', 'cache_minus_nocache', count(*)
    FROM (SELECT * FROM cache_size EXCEPT SELECT * FROM nocache_size) d
    UNION ALL SELECT 'bulk_10k', 'nocache_minus_cache', count(*)
    FROM (SELECT * FROM nocache_bulk EXCEPT SELECT * FROM cache_bulk) d
    UNION ALL SELECT 'bulk_10k', 'cache_minus_nocache', count(*)
    FROM (SELECT * FROM cache_bulk EXCEPT SELECT * FROM nocache_bulk) d
    UNION ALL SELECT 'for_update', 'nocache_minus_cache', count(*)
    FROM (SELECT * FROM nocache_fu EXCEPT SELECT * FROM cache_fu) d
    UNION ALL SELECT 'for_update', 'cache_minus_nocache', count(*)
    FROM (SELECT * FROM cache_fu EXCEPT SELECT * FROM nocache_fu) d
) summary
ORDER BY test, direction;

-- 清理
SELECT pg_drop_relation_row_cache('row_cache_correctness.test_tbl');
