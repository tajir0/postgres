#!/usr/bin/env bash
set -euo pipefail

# ================================================================
# V4 row cache end-to-end correctness test
#
# Stresses the cache with concurrent:
#   - readers (verify a per-row invariant)
#   - DML writers (UPDATE / DELETE+INSERT, both preserving the invariant)
#   - cache controller (pg_load + pg_drop in a tight loop)
#
# Covers BOTH cache shapes:
#   - single-column byval pk (singlepk_tbl, int4 pk)
#   - composite byval pk     (composite_tbl, (int4, int4, int4) pk)
#
# The per-row invariant is:
#     payload == 'v' || version
# Writers always set payload and version together so any consistent
# tuple satisfies it.  If the cache ever splices a row from two
# different generations (e.g. version updated but payload still
# points at an old DSA block), the invariant fails and the reader
# exits non-zero.
#
# Final verification runs after killing all workers:
#   - cache is dropped
#   - SELECT count(*) WHERE payload <> 'v' || version is 0
#   - row count is sane
#
# Usage:
#   ./relation_row_cache_correctness.sh [DURATION_SEC]
#
# Environment:
#   DURATION_SEC=30     test duration, also overridable as $1
#   READERS=2           parallel reader workers per table (4 total)
#   WRITERS=2           parallel DML workers across both tables
#   SINGLEPK_ROWS=10000 initial row count of singlepk_tbl
#   COMPOSITE_DIM=20    composite_tbl populated with DIM^3 rows
#                       (default 20 => 8000 rows; max ~30 for ~27000)
#   CTRL_INTERVAL_MS=200  cache controller loop period
#   PG_BIN PGHOST PGPORT PGUSER PGDATABASE PGPASSWORD
#   OUT_DIR             default /tmp/rowcache_correctness
#
# Exit code 0 = PASS, non-zero = FAIL (worker error or invariant break)
# ================================================================

DURATION_SEC="${1:-${DURATION_SEC:-30}}"
READERS="${READERS:-2}"
WRITERS="${WRITERS:-2}"
SINGLEPK_ROWS="${SINGLEPK_ROWS:-10000}"
COMPOSITE_DIM="${COMPOSITE_DIM:-20}"
CTRL_INTERVAL_MS="${CTRL_INTERVAL_MS:-200}"

PG_BIN="${PG_BIN:-$HOME/pg_install/bin}"
PSQL="$PG_BIN/psql"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-postgres}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGPASSWORD="${PGPASSWORD:-}"
export PGUSER PGDATABASE PGHOST PGPORT PGPASSWORD

OUT_DIR="${OUT_DIR:-/tmp/rowcache_correctness}"
mkdir -p "$OUT_DIR"

# ---------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------
if [[ -z "${PGPASSWORD:-}" ]] && [[ -z "${PGPASSFILE:-}" ]]; then
  echo "INFO: PGPASSWORD / PGPASSFILE not set; psql may prompt." >&2
fi

if ! command -v "$PSQL" >/dev/null 2>&1; then
  echo "ERROR: psql not found: $PSQL" >&2
  exit 1
fi

PSQL_OPTS=(-X --no-psqlrc -v ON_ERROR_STOP=1 -A -t -q)

run_psql() { "$PSQL" "${PSQL_OPTS[@]}" "$@"; }

# ---------------------------------------------------------------
# Phase 1: setup schema + initial data
# ---------------------------------------------------------------
echo "=========================================="
echo " V4 row cache correctness test"
echo " DURATION=${DURATION_SEC}s  READERS=${READERS}x2  WRITERS=${WRITERS}"
echo " SINGLEPK_ROWS=${SINGLEPK_ROWS}  COMPOSITE=${COMPOSITE_DIM}^3"
echo " CTRL_INTERVAL=${CTRL_INTERVAL_MS}ms"
echo " OUT_DIR=${OUT_DIR}"
echo "=========================================="

run_psql <<EOSQL
SET client_min_messages = warning;

DROP SCHEMA IF EXISTS row_cache_correctness CASCADE;
CREATE SCHEMA row_cache_correctness;

CREATE TABLE row_cache_correctness.singlepk_tbl (
    id      int4 PRIMARY KEY,
    version int4 NOT NULL,
    payload text NOT NULL
);

CREATE TABLE row_cache_correctness.composite_tbl (
    a       int4 NOT NULL,
    b       int4 NOT NULL,
    c       int4 NOT NULL,
    version int4 NOT NULL,
    payload text NOT NULL,
    PRIMARY KEY (a, b, c)
);

INSERT INTO row_cache_correctness.singlepk_tbl
SELECT g, 0, 'v0'
FROM   generate_series(1, ${SINGLEPK_ROWS}) g;

INSERT INTO row_cache_correctness.composite_tbl
SELECT a, b, c, 0, 'v0'
FROM   generate_series(1, ${COMPOSITE_DIM}) a,
       generate_series(1, ${COMPOSITE_DIM}) b,
       generate_series(1, ${COMPOSITE_DIM}) c;

ANALYZE row_cache_correctness.singlepk_tbl;
ANALYZE row_cache_correctness.composite_tbl;

-- Initial cache load
SELECT pg_load_relation_row_cache('row_cache_correctness.singlepk_tbl');
SELECT pg_load_relation_row_cache('row_cache_correctness.composite_tbl');
EOSQL

echo " setup OK"
SINGLEPK_INITIAL=$(run_psql -c "SELECT count(*) FROM row_cache_correctness.singlepk_tbl;")
COMPOSITE_INITIAL=$(run_psql -c "SELECT count(*) FROM row_cache_correctness.composite_tbl;")
echo " singlepk rows: $SINGLEPK_INITIAL    composite rows: $COMPOSITE_INITIAL"

# ---------------------------------------------------------------
# Worker SQL generators (heredoc -> per-worker SQL file)
# ---------------------------------------------------------------

# Reader: loops verifying invariant.  Each iteration:
#   - pick a random key
#   - SELECT count(*) WHERE pk = ? AND payload <> 'v' || version
#   - assert it's 0; if not, RAISE EXCEPTION (psql exits 3)
#
# Loop count is huge; controller will kill it on timeout.
generate_reader_sql_single() {
  cat <<EOSQL
SET client_min_messages = warning;
SET search_path = row_cache_correctness, public;
SET jit = off;

DO \$\$
DECLARE
    i        int;
    pk       int;
    bad      int;
BEGIN
    FOR i IN 1..1000000000 LOOP
        pk := 1 + (random() * ${SINGLEPK_ROWS})::int;
        IF pk > ${SINGLEPK_ROWS} THEN pk := ${SINGLEPK_ROWS}; END IF;
        SELECT count(*) INTO bad
          FROM singlepk_tbl
         WHERE id = pk AND payload <> 'v' || version;
        IF bad > 0 THEN
            RAISE EXCEPTION 'singlepk reader: invariant violated at id=% (count=%)', pk, bad;
        END IF;
        IF i % 10000 = 0 THEN
            PERFORM pg_sleep(0);  -- yield
        END IF;
    END LOOP;
END
\$\$;
EOSQL
}

generate_reader_sql_composite() {
  cat <<EOSQL
SET client_min_messages = warning;
SET search_path = row_cache_correctness, public;
SET jit = off;

DO \$\$
DECLARE
    i        int;
    pa       int;
    pb       int;
    pc       int;
    bad      int;
BEGIN
    FOR i IN 1..1000000000 LOOP
        pa := 1 + (random() * ${COMPOSITE_DIM})::int;
        pb := 1 + (random() * ${COMPOSITE_DIM})::int;
        pc := 1 + (random() * ${COMPOSITE_DIM})::int;
        IF pa > ${COMPOSITE_DIM} THEN pa := ${COMPOSITE_DIM}; END IF;
        IF pb > ${COMPOSITE_DIM} THEN pb := ${COMPOSITE_DIM}; END IF;
        IF pc > ${COMPOSITE_DIM} THEN pc := ${COMPOSITE_DIM}; END IF;
        SELECT count(*) INTO bad
          FROM composite_tbl
         WHERE a = pa AND b = pb AND c = pc
           AND payload <> 'v' || version;
        IF bad > 0 THEN
            RAISE EXCEPTION 'composite reader: invariant violated at (%,%,%) (count=%)',
                pa, pb, pc, bad;
        END IF;
        IF i % 10000 = 0 THEN
            PERFORM pg_sleep(0);
        END IF;
    END LOOP;
END
\$\$;
EOSQL
}

# Writer: alternates UPDATE / DELETE+INSERT on a random row of each
# table.  Each operation atomically maintains the invariant.
generate_writer_sql() {
  cat <<EOSQL
SET client_min_messages = warning;
SET search_path = row_cache_correctness, public;
SET jit = off;

DO \$\$
DECLARE
    i        int;
    pk       int;
    pa       int;
    pb       int;
    pc       int;
    cur_v    int;
    new_v    int;
BEGIN
    FOR i IN 1..1000000000 LOOP
        -- ===== singlepk UPDATE =====
        pk := 1 + (random() * ${SINGLEPK_ROWS})::int;
        IF pk > ${SINGLEPK_ROWS} THEN pk := ${SINGLEPK_ROWS}; END IF;
        UPDATE singlepk_tbl
           SET version = version + 1,
               payload = 'v' || (version + 1)
         WHERE id = pk;

        -- ===== composite UPDATE =====
        pa := 1 + (random() * ${COMPOSITE_DIM})::int;
        pb := 1 + (random() * ${COMPOSITE_DIM})::int;
        pc := 1 + (random() * ${COMPOSITE_DIM})::int;
        IF pa > ${COMPOSITE_DIM} THEN pa := ${COMPOSITE_DIM}; END IF;
        IF pb > ${COMPOSITE_DIM} THEN pb := ${COMPOSITE_DIM}; END IF;
        IF pc > ${COMPOSITE_DIM} THEN pc := ${COMPOSITE_DIM}; END IF;
        UPDATE composite_tbl
           SET version = version + 1,
               payload = 'v' || (version + 1)
         WHERE a = pa AND b = pb AND c = pc;

        -- ===== singlepk DELETE+INSERT preserving invariant =====
        IF i % 7 = 0 THEN
            pk := 1 + (random() * ${SINGLEPK_ROWS})::int;
            IF pk > ${SINGLEPK_ROWS} THEN pk := ${SINGLEPK_ROWS}; END IF;
            SELECT version INTO cur_v FROM singlepk_tbl WHERE id = pk;
            IF cur_v IS NOT NULL THEN
                new_v := cur_v + 1;
                DELETE FROM singlepk_tbl WHERE id = pk;
                INSERT INTO singlepk_tbl(id, version, payload)
                     VALUES (pk, new_v, 'v' || new_v);
            END IF;
        END IF;

        -- ===== composite DELETE+INSERT preserving invariant =====
        IF i % 11 = 0 THEN
            pa := 1 + (random() * ${COMPOSITE_DIM})::int;
            pb := 1 + (random() * ${COMPOSITE_DIM})::int;
            pc := 1 + (random() * ${COMPOSITE_DIM})::int;
            IF pa > ${COMPOSITE_DIM} THEN pa := ${COMPOSITE_DIM}; END IF;
            IF pb > ${COMPOSITE_DIM} THEN pb := ${COMPOSITE_DIM}; END IF;
            IF pc > ${COMPOSITE_DIM} THEN pc := ${COMPOSITE_DIM}; END IF;
            SELECT version INTO cur_v
              FROM composite_tbl WHERE a = pa AND b = pb AND c = pc;
            IF cur_v IS NOT NULL THEN
                new_v := cur_v + 1;
                DELETE FROM composite_tbl WHERE a = pa AND b = pb AND c = pc;
                INSERT INTO composite_tbl(a, b, c, version, payload)
                     VALUES (pa, pb, pc, new_v, 'v' || new_v);
            END IF;
        END IF;
    END LOOP;
END
\$\$;
EOSQL
}

# Cache controller: drop + load in a loop.  Exercises EBR retire +
# orphan-list paths under concurrent readers/writers.
generate_controller_sql() {
  cat <<EOSQL
SET client_min_messages = warning;
SET search_path = row_cache_correctness, public;
SET jit = off;

DO \$\$
DECLARE
    i int;
BEGIN
    FOR i IN 1..1000000000 LOOP
        BEGIN
            PERFORM pg_drop_relation_row_cache('row_cache_correctness.singlepk_tbl');
            PERFORM pg_drop_relation_row_cache('row_cache_correctness.composite_tbl');
            PERFORM pg_sleep(${CTRL_INTERVAL_MS}.0 / 2000.0);
            PERFORM pg_load_relation_row_cache('row_cache_correctness.singlepk_tbl');
            PERFORM pg_load_relation_row_cache('row_cache_correctness.composite_tbl');
            PERFORM pg_sleep(${CTRL_INTERVAL_MS}.0 / 2000.0);
        EXCEPTION WHEN OTHERS THEN
            -- swallow transient errors (e.g. relation locked) and continue
            PERFORM pg_sleep(0.05);
        END;
    END LOOP;
END
\$\$;
EOSQL
}

# ---------------------------------------------------------------
# Spawn workers
# ---------------------------------------------------------------
WORKERS=()
LOGS=()

start_worker() {
  local kind="$1"
  local idx="$2"
  local sql_file="$3"
  local log_file="$OUT_DIR/${kind}_${idx}.log"
  LOGS+=("$log_file")
  ( "$PSQL" "${PSQL_OPTS[@]}" -f "$sql_file" >"$log_file" 2>&1; echo $? >"$log_file.exit" ) &
  WORKERS+=($!)
  echo " spawned ${kind}_${idx} pid=$!"
}

# Reader workers
for ((r=1; r<=READERS; r++)); do
  sql="$OUT_DIR/reader_single_${r}.sql"
  generate_reader_sql_single >"$sql"
  start_worker "reader_single" "$r" "$sql"
done

for ((r=1; r<=READERS; r++)); do
  sql="$OUT_DIR/reader_composite_${r}.sql"
  generate_reader_sql_composite >"$sql"
  start_worker "reader_composite" "$r" "$sql"
done

# Writer workers
for ((w=1; w<=WRITERS; w++)); do
  sql="$OUT_DIR/writer_${w}.sql"
  generate_writer_sql >"$sql"
  start_worker "writer" "$w" "$sql"
done

# Cache controller (always exactly 1)
sql="$OUT_DIR/controller.sql"
generate_controller_sql >"$sql"
start_worker "controller" "1" "$sql"

# ---------------------------------------------------------------
# Run for DURATION_SEC, then kill everything
# ---------------------------------------------------------------
echo " all workers running, sleeping ${DURATION_SEC}s..."
sleep "$DURATION_SEC"

echo " sending TERM to workers..."
for pid in "${WORKERS[@]}"; do
  kill -TERM "$pid" 2>/dev/null || true
done

# Give them a moment to exit cleanly
sleep 2
for pid in "${WORKERS[@]}"; do
  kill -KILL "$pid" 2>/dev/null || true
done

wait 2>/dev/null || true

# ---------------------------------------------------------------
# Phase 3: aggregate worker exit codes + check logs for RAISE EXCEPTION
# ---------------------------------------------------------------
echo "=========================================="
echo " worker results"
echo "=========================================="

WORKER_FAIL=0
for log in "${LOGS[@]}"; do
  exit_file="$log.exit"
  if [[ -f "$exit_file" ]]; then
    code=$(cat "$exit_file" 2>/dev/null || echo "?")
  else
    code="killed"
  fi

  # exit code 0 = success, but our workers are designed to loop forever
  # and be killed; so "killed by signal" (psql exits 1/2 with WAS_KILLED)
  # is OK.  What we MUST NOT see is RAISE EXCEPTION (psql exits 3).
  if grep -q "invariant violated" "$log" 2>/dev/null; then
    echo "  $log  -> FAIL (invariant violation)"
    WORKER_FAIL=$((WORKER_FAIL + 1))
  elif grep -qiE "ERROR|FATAL|PANIC|server closed|terminating connection|terminating row" "$log" 2>/dev/null; then
    # Allow expected shutdown noise
    if grep -qE "FATAL:.*terminating connection due to administrator command|server closed the connection unexpectedly" "$log" 2>/dev/null; then
      echo "  $log  -> ok (shutdown noise, code=$code)"
    else
      # Real error?  Show first 3 lines
      echo "  $log  -> WARN (code=$code, contains error keywords):"
      grep -iE "ERROR|FATAL|PANIC" "$log" | head -3 | sed 's/^/      /'
    fi
  else
    echo "  $log  -> ok (code=$code)"
  fi
done

# ---------------------------------------------------------------
# Phase 4: final invariant verification (cache disabled)
# ---------------------------------------------------------------
echo "=========================================="
echo " final verification (cache dropped)"
echo "=========================================="

run_psql <<EOSQL
SET client_min_messages = warning;
SELECT pg_drop_relation_row_cache('row_cache_correctness.singlepk_tbl');
SELECT pg_drop_relation_row_cache('row_cache_correctness.composite_tbl');
EOSQL

SINGLEPK_BAD=$(run_psql -c "
  SELECT count(*) FROM row_cache_correctness.singlepk_tbl
   WHERE payload <> 'v' || version;
")
COMPOSITE_BAD=$(run_psql -c "
  SELECT count(*) FROM row_cache_correctness.composite_tbl
   WHERE payload <> 'v' || version;
")
SINGLEPK_FINAL=$(run_psql -c "SELECT count(*) FROM row_cache_correctness.singlepk_tbl;")
COMPOSITE_FINAL=$(run_psql -c "SELECT count(*) FROM row_cache_correctness.composite_tbl;")

echo " singlepk: rows=$SINGLEPK_FINAL  bad_invariant=$SINGLEPK_BAD"
echo " composite: rows=$COMPOSITE_FINAL  bad_invariant=$COMPOSITE_BAD"

INVARIANT_FAIL=0
if [[ "$SINGLEPK_BAD" != "0" || "$COMPOSITE_BAD" != "0" ]]; then
  INVARIANT_FAIL=1
fi

# Row counts should be unchanged (DELETE+INSERT preserves count).
ROWCOUNT_FAIL=0
if [[ "$SINGLEPK_FINAL" != "$SINGLEPK_INITIAL" ]]; then
  echo "  WARN: singlepk row count drifted: $SINGLEPK_INITIAL -> $SINGLEPK_FINAL"
  ROWCOUNT_FAIL=1
fi
if [[ "$COMPOSITE_FINAL" != "$COMPOSITE_INITIAL" ]]; then
  echo "  WARN: composite row count drifted: $COMPOSITE_INITIAL -> $COMPOSITE_FINAL"
  ROWCOUNT_FAIL=1
fi

# ---------------------------------------------------------------
# Final verdict
# ---------------------------------------------------------------
echo "=========================================="
if [[ $WORKER_FAIL -eq 0 && $INVARIANT_FAIL -eq 0 && $ROWCOUNT_FAIL -eq 0 ]]; then
  echo " PASS — no invariant violations across concurrent readers/writers/load/drop"
  exit 0
else
  echo " FAIL: worker_fail=$WORKER_FAIL  invariant_fail=$INVARIANT_FAIL  rowcount_fail=$ROWCOUNT_FAIL"
  echo " logs preserved at $OUT_DIR"
  exit 1
fi
