#!/usr/bin/env bash
# 行缓存 pgbench A/B 跑分器
#
# 三组对照(读多写少混合, 读≈88% / 写≈12%):
#   OFF       未缓存 + 回填关 = 纯原生路径(基线)
#   LOADONLY  三张读靶表全量装载, 回填关
#   ON        三张读靶表全量装载, 回填开
#
# 每组跑 ITERS 轮取中位数(单轮抖动大, 务必多轮), 输出两类结果:
#   1) 总混合读写 TPS 中位数;
#   2) 逐语句平均延迟中位数(read_stock 这行最可信, TPS 抖动大)。
#
# 用法:
#   PGBIN=/path/to/pg/bin PGPORT=5455 PGDB=benchmarksql PGUSER=postgres \
#   WAREHOUSES=10 DURATION=120 ITERS=5 CLIENTS=8 JOBS=4 \
#   bash run_ab.sh
#
# 前置: 目标实例是"行缓存 build", 已导入 BenchmarkSQL 数据(bmsql_* 表),
#       PGUSER 对三张表有 MAINTAIN 权限(owner 或超级用户即可)。
set -euo pipefail

PGBIN="${PGBIN:-/tmp/rc_pgsql/bin}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5455}"
PGDB="${PGDB:-benchmarksql}"
PGUSER="${PGUSER:-postgres}"
WAREHOUSES="${WAREHOUSES:-10}"      # 改成你的实际仓数
DURATION="${DURATION:-300}"         # 每轮秒数(5 分钟)
ITERS="${ITERS:-5}"                 # 每组轮数, 取中位数
CLIENTS="${CLIENTS:-100}"           # 客户端连接数
JOBS="${JOBS:-60}"                  # pgbench 线程数
DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$DIR/results}"
mkdir -p "$OUT"

PSQL="$PGBIN/psql"
PGBENCH="$PGBIN/pgbench"
export PGHOST PGPORT
CONN=(-h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB")

# 仓数注入脚本(read_stock/update_stock/read_customer 里的 random(1,10))
sed -i.bak "s/random(1, 10)/random(1, $WAREHOUSES)/g" \
  "$DIR/read_stock.sql" "$DIR/update_stock.sql" "$DIR/read_customer.sql" 2>/dev/null || true

load(){ "$PSQL" "${CONN[@]}" -Atc \
  "select pg_load_relation_row_cache('bmsql_item');
   select pg_load_relation_row_cache('bmsql_customer');
   select pg_load_relation_row_cache('bmsql_stock');" >/dev/null 2>&1; }
drop(){ "$PSQL" "${CONN[@]}" -Atc \
  "select pg_drop_relation_row_cache('bmsql_item');
   select pg_drop_relation_row_cache('bmsql_customer');
   select pg_drop_relation_row_cache('bmsql_stock');" >/dev/null 2>&1; }

# 一轮 pgbench(带 -r), 整份输出存到 $2
run(){
  PGOPTIONS="-c row_cache_backfill=$1" "$PGBENCH" "${CONN[@]}" \
    -f "$DIR/read_item.sql@60" -f "$DIR/read_stock.sql@20" -f "$DIR/read_customer.sql@8" \
    -f "$DIR/update_stock.sql@8" -f "$DIR/insert_item.sql@3" -f "$DIR/delete_item.sql@1" \
    -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -M prepared -r -n > "$2" 2>&1
}

tps_of(){ grep -oE 'tps = [0-9.]+ \(without' "$1" | grep -oE '[0-9.]+' | head -1; }
# 逐脚本 "latency average" -> 打印 "脚本名 延迟"
lat_of(){ awk '/SQL script/{f=$0; sub(/.*\//,"",f); sub(/\.sql.*/,"",f)}
               /latency average/{print f, $4}' "$1"; }
med_stdin(){ sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "-"}
             else print (NR%2)? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2 }'; }

DATA="$OUT/_latdata.tmp"; : > "$DATA"
declare -A MEDTPS

echo "== 行缓存 A/B  仓数=$WAREHOUSES  每轮=${DURATION}s x ${ITERS}轮  并发=$CLIENTS =="
for grp in OFF LOADONLY ON; do
  tpsvals=()
  for i in $(seq 1 "$ITERS"); do
    f="$OUT/${grp}_run${i}.txt"
    case "$grp" in
      OFF)      drop; run off "$f" ;;
      LOADONLY) load; run off "$f" ;;
      ON)       load; run on  "$f" ;;
    esac
    t=$(tps_of "$f"); tpsvals+=("$t")
    printf "  %-9s run%d  tps=%s\n" "$grp" "$i" "$t"
    lat_of "$f" | while read -r s v; do echo "$grp $s $v" >> "$DATA"; done
  done
  MEDTPS[$grp]=$(printf '%s\n' "${tpsvals[@]}" | med_stdin)
  echo "  --> $grp TPS 中位数 = ${MEDTPS[$grp]}"
done

echo
echo "== 总混合 TPS 中位数 =="
awk -v off="${MEDTPS[OFF]}" -v lo="${MEDTPS[LOADONLY]}" -v on="${MEDTPS[ON]}" 'BEGIN{
  printf "  %-10s %10s  %s\n","OFF",off,"(基线)"
  printf "  %-10s %10s  %+.1f%%\n","LOADONLY",lo,(lo/off-1)*100
  printf "  %-10s %10s  %+.1f%%\n","ON",on,(on/off-1)*100
}'

echo
echo "== 逐语句平均延迟中位数 (ms) =="
printf "  %-14s %9s %9s %9s\n" "脚本" "OFF" "LOADONLY" "ON"
for s in read_item read_stock read_customer update_stock insert_item delete_item; do
  o=$(grep "^OFF $s "      "$DATA" | awk '{print $3}' | med_stdin)
  l=$(grep "^LOADONLY $s " "$DATA" | awk '{print $3}' | med_stdin)
  n=$(grep "^ON $s "       "$DATA" | awk '{print $3}' | med_stdin)
  printf "  %-14s %9s %9s %9s\n" "$s" "$o" "$l" "$n"
done

echo
echo "原始逐轮报告: $OUT/{OFF,LOADONLY,ON}_run*.txt"
echo "提示: 若各组 TPS 波动带重叠, 以 read_stock 逐语句延迟为准信号;"
echo "      加大 DURATION / 调小 shared_buffers / 用 zipfian 分布放大信号。"
