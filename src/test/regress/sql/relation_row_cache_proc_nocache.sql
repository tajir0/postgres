\set ON_ERROR_STOP on
\set loops 1000000
\set rounds 20

SET client_min_messages = notice;
SET search_path = row_cache_perf, public;
SET jit = off;
SET max_parallel_workers_per_gather = 0;
SET enable_indexonlyscan = off;

DO $$
DECLARE
    t_prepare_start  timestamptz;
    t_prepare_end    timestamptz;
BEGIN
    t_prepare_start := clock_timestamp();

    IF to_regprocedure('pg_drop_relation_row_cache(text)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl');
    ELSIF to_regprocedure('pg_drop_relation_row_cache(oid)') IS NOT NULL THEN
        PERFORM pg_drop_relation_row_cache('row_cache_perf.bench_tbl'::regclass::oid);
    ELSE
        RAISE EXCEPTION 'pg_drop_relation_row_cache(...) does not exist';
    END IF;

    t_prepare_end := clock_timestamp();
    RAISE NOTICE 'stage=nocache_prepare elapsed=%', (t_prepare_end - t_prepare_start);
END
$$;

CREATE OR REPLACE PROCEDURE row_cache_perf.bench_nocache_proc(p_loops int, p_rounds int)
LANGUAGE plpgsql
AS $$
DECLARE
    i                 int;
    t_work_start      timestamptz;
    t_work_end        timestamptz;
BEGIN
    t_work_start := clock_timestamp();

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

    t_work_end := clock_timestamp();

    RAISE NOTICE 'stage=nocache_workload rounds=% loops=% total_elapsed=%',
        p_rounds, p_loops, (t_work_end - t_work_start);
END
$$;

SELECT pg_backend_pid();
SELECT pg_sleep(1.5);
CALL row_cache_perf.bench_nocache_proc(:loops, :rounds);
