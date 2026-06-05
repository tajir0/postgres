\set ON_ERROR_STOP on
\timing on

-- relation_row_cache_perf.sql
-- 目的：对比“未使用行缓存”与“使用两级行缓存”时的查询耗时。
-- 注意：行缓存是后端本地结构，请在同一个 psql 会话中完整执行本脚本。

SET client_min_messages = warning;
SET jit = off;
SET work_mem = '64MB';

DROP SCHEMA IF EXISTS row_cache_perf CASCADE;
CREATE SCHEMA row_cache_perf;
SET search_path = row_cache_perf, public;

-- 1) 构造测试数据（该阶段不计入性能对比）
CREATE TABLE bench_tbl
(
    id      integer PRIMARY KEY,
    payload text NOT NULL,
    filler  text NOT NULL
);

INSERT INTO bench_tbl (id, payload, filler)
SELECT g,
       'payload-' || g,
       repeat(md5(g::text), 2)
FROM generate_series(1, 300000) AS g;

ANALYZE bench_tbl;

-- 2) 基准函数：执行大量“按主键点查”，返回总耗时（毫秒）
CREATE OR REPLACE FUNCTION bench_index_lookup(loop_count integer)
RETURNS double precision
LANGUAGE plpgsql
AS $$
DECLARE
    i          integer;
    probe_id   integer;
    ts_begin   timestamptz;
    ts_end     timestamptz;
BEGIN
    ts_begin := clock_timestamp();

    FOR i IN 1..loop_count LOOP
        probe_id := 1 + ((random() * 299999)::integer);
        PERFORM payload FROM bench_tbl WHERE id = probe_id;
    END LOOP;

    ts_end := clock_timestamp();
    RETURN EXTRACT(epoch FROM (ts_end - ts_begin)) * 1000.0;
END;
$$;

-- 3) 确认查询形态（应为 Index Scan）
EXPLAIN (ANALYZE, COSTS OFF, BUFFERS)
SELECT payload FROM bench_tbl WHERE id = 42;

-- 4) A/B 对比
-- A: 不使用行缓存
SELECT pg_drop_relation_row_cache('row_cache_perf.bench_tbl');
SELECT pg_sleep(0.1);

CREATE TEMP TABLE perf_no_cache
(
    run_no integer,
    elapsed_ms double precision
) ON COMMIT DROP;

INSERT INTO perf_no_cache
SELECT s, bench_index_lookup(120000)
FROM generate_series(1, 5) AS s;

-- B: 使用行缓存
-- 关键：加载动作放在计时前，明确不计入对比
SELECT pg_load_relation_row_cache('row_cache_perf.bench_tbl');
SELECT pg_sleep(0.1);

CREATE TEMP TABLE perf_with_cache
(
    run_no integer,
    elapsed_ms double precision
) ON COMMIT DROP;

INSERT INTO perf_with_cache
SELECT s, bench_index_lookup(120000)
FROM generate_series(1, 5) AS s;

-- 5) 输出结果
SELECT 'no_cache' AS mode, run_no, round(elapsed_ms::numeric, 3) AS elapsed_ms
FROM perf_no_cache
UNION ALL
SELECT 'with_cache' AS mode, run_no, round(elapsed_ms::numeric, 3) AS elapsed_ms
FROM perf_with_cache
ORDER BY mode, run_no;

WITH n AS
(
    SELECT avg(elapsed_ms) AS avg_ms FROM perf_no_cache
),
c AS
(
    SELECT avg(elapsed_ms) AS avg_ms FROM perf_with_cache
)
SELECT
    round(n.avg_ms::numeric, 3) AS avg_no_cache_ms,
    round(c.avg_ms::numeric, 3) AS avg_with_cache_ms,
    round((n.avg_ms / NULLIF(c.avg_ms, 0))::numeric, 3) AS speedup_ratio
FROM n, c;

-- 6) 清理（可选）
DROP FUNCTION bench_index_lookup(integer);
RESET search_path;
