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

DO $$
DECLARE
    ms_prepare_start double precision;
    ms_prepare_end   double precision;
BEGIN
    ms_prepare_start := monotonic_ms();

    IF to_regprocedure('pg_drop_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_drop_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_drop_relation_row_cache(...) does not exist';
    END IF;

    ms_prepare_end := monotonic_ms();
    RAISE NOTICE 'stage=nocache_prepare elapsed=% ms', (ms_prepare_end - ms_prepare_start);
END
$$;

CREATE OR REPLACE PROCEDURE row_cache_perf.bench_nocache_proc(p_loops int, p_rounds int)
LANGUAGE plpgsql
AS $$
DECLARE
    i            int;
    ms_work_start double precision;
    ms_work_end   double precision;
BEGIN
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

    RAISE NOTICE 'stage=nocache_workload rounds=% loops=% total_elapsed=% ms',
        p_rounds, p_loops, (ms_work_end - ms_work_start);
END
$$;

SELECT pg_backend_pid();
SELECT pg_sleep(1.5);
CALL row_cache_perf.bench_nocache_proc(:loops, :rounds);
