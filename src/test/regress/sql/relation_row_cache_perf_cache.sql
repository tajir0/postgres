\set ON_ERROR_STOP on
\timing on
\set loops 3000000

-- 行缓存性能脚本�?-- 1) 先把目标表加载进行缓存（加载时间不计入）
-- 2) 执行同一条查询，输出纯查询耗时

SET client_min_messages = warning;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;

DO $$
BEGIN
    IF to_regprocedure('pg_load_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_load_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_load_relation_row_cache(...) 不存�?;
    END IF;
END
$$;

-- 预热，减少首次执行抖�?SELECT sum(v)
FROM
(
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, 50000) AS g
) t;

DROP TABLE IF EXISTS perf_cache_runs;

CREATE TEMP TABLE perf_cache_runs
(
    run_no     int,
    started_at timestamptz,
    finished_at timestamptz,
    elapsed    interval,
    bench_sum  bigint
);

SELECT clock_timestamp() AS ts \gset
SELECT sum(v) AS bench_sum
FROM (
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, :loops) AS g
) t \gset
SELECT clock_timestamp() AS te \gset
INSERT INTO perf_cache_runs VALUES (1, :'ts', :'te', :'te'::timestamptz - :'ts'::timestamptz, :bench_sum);

SELECT clock_timestamp() AS ts \gset
SELECT sum(v) AS bench_sum
FROM (
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, :loops) AS g
) t \gset
SELECT clock_timestamp() AS te \gset
INSERT INTO perf_cache_runs VALUES (2, :'ts', :'te', :'te'::timestamptz - :'ts'::timestamptz, :bench_sum);

SELECT clock_timestamp() AS ts \gset
SELECT sum(v) AS bench_sum
FROM (
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, :loops) AS g
) t \gset
SELECT clock_timestamp() AS te \gset
INSERT INTO perf_cache_runs VALUES (3, :'ts', :'te', :'te'::timestamptz - :'ts'::timestamptz, :bench_sum);

SELECT clock_timestamp() AS ts \gset
SELECT sum(v) AS bench_sum
FROM (
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, :loops) AS g
) t \gset
SELECT clock_timestamp() AS te \gset
INSERT INTO perf_cache_runs VALUES (4, :'ts', :'te', :'te'::timestamptz - :'ts'::timestamptz, :bench_sum);

SELECT clock_timestamp() AS ts \gset
SELECT sum(v) AS bench_sum
FROM (
    SELECT (
        SELECT
            (q.r).id
        FROM (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q
    ) AS v
    FROM generate_series(1, :loops) AS g
) t \gset
SELECT clock_timestamp() AS te \gset
INSERT INTO perf_cache_runs VALUES (5, :'ts', :'te', :'te'::timestamptz - :'ts'::timestamptz, :bench_sum);

TABLE perf_cache_runs;

SELECT avg(extract(epoch FROM elapsed)) AS avg_seconds_cache,
       min(extract(epoch FROM elapsed)) AS min_seconds_cache,
       max(extract(epoch FROM elapsed)) AS max_seconds_cache
FROM perf_cache_runs;



