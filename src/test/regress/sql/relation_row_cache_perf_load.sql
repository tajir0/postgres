\set ON_ERROR_STOP on
\timing on

-- 加载数据脚本：
-- 1) 表存在则删除
-- 2) 重建并写入测试数据
-- 3) 仅用于准备数据，不包含性能测试

SET client_min_messages = warning;

DROP SCHEMA IF EXISTS row_cache_perf CASCADE;
CREATE SCHEMA row_cache_perf;

CREATE TABLE row_cache_perf.bench_tbl
(
    id   bigint PRIMARY KEY,
    k1   integer NOT NULL,
    k2   integer NOT NULL,
    t1   text NOT NULL,
    t2   text NOT NULL,
    t3   text NOT NULL,
    n1   numeric(12,2) NOT NULL,
    b1   boolean NOT NULL,
    ts1  timestamptz NOT NULL,
    j1   jsonb NOT NULL
);

INSERT INTO row_cache_perf.bench_tbl (id, k1, k2, t1, t2, t3, n1, b1, ts1, j1)
SELECT g,
       (g % 1000)::int,
       ((g * 7) % 10000)::int,
       substr(md5(g::text), 1, 16),
       substr(md5((g + 11)::text), 1, 24),
       substr(md5((g + 29)::text), 1, 32),
       ((g % 100000)::numeric / 100.0)::numeric(12,2),
       ((g % 2) = 0),
       timestamptz '2024-01-01 00:00:00+00' + (g % 86400) * interval '1 second',
       jsonb_build_array(g, g % 10, g % 100)
FROM generate_series(1, 2000000) AS g;

ANALYZE row_cache_perf.bench_tbl;

SELECT count(*) AS loaded_rows
FROM row_cache_perf.bench_tbl;
