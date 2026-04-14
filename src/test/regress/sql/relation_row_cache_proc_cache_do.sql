-- 与 relation_row_cache_proc_cache.sql 同源，但负载用匿名块 DO（内联 FOR+点查），
-- 不创建 row_cache_perf.bench_cache_proc，也无 CALL。
-- 依赖 Linux /proc/uptime（monotonic_ms）。
--
-- psql 不会对 DO $$ ... $$ 内的 :var 做替换，故用临时表在 dollar 外写入 :loops / :rounds。

\set ON_ERROR_STOP on
\set loops 1000000
\set rounds 20

SET client_min_messages = notice;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

CREATE OR REPLACE FUNCTION monotonic_ms()
RETURNS double precision
LANGUAGE plpgsql
AS $func$
DECLARE
    content text;
BEGIN
    content := pg_read_file('/proc/uptime');
    RETURN split_part(content, ' ', 1)::double precision * 1000.0;
END;
$func$;

-- 准备阶段：装入行缓存（计时见 stage=cache_prepare）
DO $$
DECLARE
    ms_prepare_start double precision;
    ms_prepare_end   double precision;
BEGIN
    ms_prepare_start := monotonic_ms();

    IF to_regprocedure('pg_load_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_load_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_load_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_load_relation_row_cache(...) does not exist';
    END IF;

    ms_prepare_end := monotonic_ms();
    RAISE NOTICE 'stage=cache_prepare elapsed=% ms', (ms_prepare_end - ms_prepare_start);
END
$$;

SELECT pg_backend_pid();
SELECT pg_sleep(1.5);

-- 在 dollar 引号外替换 :loops / :rounds，供下一 DO 读取（避免 psql 不替换 $$ 内变量）
DROP TABLE IF EXISTS _row_cache_bench_params;
CREATE TEMP TABLE _row_cache_bench_params (loops int, rounds int);
INSERT INTO _row_cache_bench_params VALUES (:loops, :rounds);

-- 负载阶段：仅本 DO 内 FOR+点查计入 total_elapsed
DO $$
DECLARE
    i             int;
    p_loops       int;
    p_rounds      int;
    ms_work_start double precision;
    ms_work_end   double precision;
BEGIN
    SELECT loops, rounds INTO STRICT p_loops, p_rounds FROM _row_cache_bench_params;

    ms_work_start := monotonic_ms();

    FOR i IN 1..p_rounds LOOP
        PERFORM pg_column_size(q.r)
        FROM generate_series(1, p_loops) AS g
        CROSS JOIN LATERAL
        (
            SELECT b AS r
            FROM row_cache_perf.bench_tbl AS b
            WHERE b.id = ((g % 2000000) + 1)
        ) AS q;
    END LOOP;

    ms_work_end := monotonic_ms();

    RAISE NOTICE 'stage=cache_workload rounds=% loops=% total_elapsed=% ms',
        p_rounds, p_loops, (ms_work_end - ms_work_start);
END
$$;
