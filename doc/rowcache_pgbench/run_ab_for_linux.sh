#!/usr/bin/env bash
# 行缓存 pgbench A/B 跑分器 (Linux / bash 4+)
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
#   WAREHOUSES=100 DURATION=300 ITERS=5 CLIENTS=100 JOBS=60 \
#   bash run_ab_for_linux.sh
#
# 前置: 目标实例是"行缓存 build"、已导入 BenchmarkSQL 数据(bmsql_* 表)、
#       PGUSER 对三张表有 MAINTAIN 权限(owner 或超级用户即可)、
#       max_connections >= CLIENTS + 余量。
#
# 说明: 刻意不用 `set -e` —— 单轮 pgbench 偶发非零(如个别失败事务)不应
#       让整个跑分静默消失; 错误由显式检查上报, 跑分继续。

set -uo pipefail

PGBIN="${PGBIN:-/usr/local/pgsql/bin}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5455}"
PGDB="${PGDB:-benchmarksql}"
PGUSER="${PGUSER:-postgres}"
WAREHOUSES="${WAREHOUSES:-100}"     # 你的实际仓数
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

die(){ echo "ERROR: $*" >&2; exit 1; }

# ---- 前置检查(避免跑一半才发现)----
command -v "$PGBENCH" >/dev/null 2>&1 || die "找不到 pgbench: $PGBENCH (设 PGBIN=)"
"$PSQL" "${CONN[@]}" -Atc "select 1" >/dev/null 2>&1 \
  || die "连不上 $PGHOST:$PGPORT/$PGDB (用户 $PGUSER)"
have_fn=$("$PSQL" "${CONN[@]}" -Atc \
  "select count(*) from pg_proc where proname='pg_load_relation_row_cache'" 2>/dev/null || echo 0)
[ "${have_fn:-0}" -ge 1 ] || die "目标实例没有行缓存函数 —— 连错实例了? (应连行缓存 build)"
maxc=$("$PSQL" "${CONN[@]}" -Atc "show max_connections" 2>/dev/null || echo 0)
if [ "${maxc:-0}" -lt "$((CLIENTS + 10))" ]; then
  echo "WARNING: max_connections=$maxc < CLIENTS($CLIENTS)+余量; -c $CLIENTS 可能报 too many clients" >&2
fi
for s in read_item read_stock read_customer update_stock insert_item delete_item; do
  [ -f "$DIR/$s.sql" ] || die "缺脚本 $DIR/$s.sql"
done

# ---- 仓数注入(幂等): 只改各脚本 `\set wid` 行的 random 上界, 不碰 iid/did/cid ----
# 用 -i.bak(GNU 与 BSD sed 都兼容; .bak 已 gitignore)
for f in read_stock update_stock read_customer; do
  [ -f "$DIR/$f.sql" ] && sed -i.bak "/set wid/s/random(1, *[0-9]*)/random(1, $WAREHOUSES)/" "$DIR/$f.sql"
done

# MODE=load(默认): 全量预热, 缓存立刻满。
# MODE=enable    : 只注册不预热, 行靠点查 miss 后回填爬入 —— 此时
#                  "LOADONLY(回填关)"组等价于"注册了但永远填不进去",
#                  该组吞吐应与 OFF 基线基本持平; 真正有意义的是 ON 组。
MODE="${MODE:-load}"
REGFN="pg_${MODE}_relation_row_cache"

load(){ "$PSQL" "${CONN[@]}" -Atc \
  "select $REGFN('bmsql_item');
   select $REGFN('bmsql_customer');
   select $REGFN('bmsql_stock');" >/dev/null 2>&1 || true; }

# 每轮结束后打印缓存实际状态(需要 pg_row_cache_relation_stats)
show_stats(){ "$PSQL" "${CONN[@]}" -Atc \
  "select '      [stats] '||relation::regclass||' segs='||n_segments||
          ' rows='||n_entries||' hit='||hit_count||' miss='||miss_count||
          ' hit%='||round(hit_ratio::numeric,1)||' backfill='||backfill_count
     from pg_row_cache_relation_stats(NULL) order by 1;" 2>/dev/null || true; }
drop(){ "$PSQL" "${CONN[@]}" -Atc \
  "select pg_drop_relation_row_cache('bmsql_item');
   select pg_drop_relation_row_cache('bmsql_customer');
   select pg_drop_relation_row_cache('bmsql_stock');" >/dev/null 2>&1 || true; }

# 一轮 pgbench(带 -r), 整份输出存到 $2; 返回 pgbench 退出码但不触发 set -e
run(){ PGOPTIONS="-c row_cache_backfill=$1" "$PGBENCH" "${CONN[@]}" \
    -f "$DIR/read_item.sql@60" -f "$DIR/read_stock.sql@20" -f "$DIR/read_customer.sql@8" \
    -f "$DIR/update_stock.sql@8" -f "$DIR/insert_item.sql@3" -f "$DIR/delete_item.sql@1" \
    -c "$CLIENTS" -j "$JOBS" -T "$DURATION" -M prepared -r -n > "$2" 2>&1; }

# 提取: 全用 awk 单进程, 无 `grep|head` 的 SIGPIPE 隐患
tps_of(){ awk '/tps = [0-9.]+ \(without/{print $3; exit}' "$1"; }
# 只取"每脚本"段里的 ` - latency average = NUM ms`(带前导 -, 数字在 = 之后);
# 跳过顶部无前导 - 的总览行。用 = 后取数, 不依赖字段序号。
lat_of(){ awk '
  /SQL script/{f=$0; sub(/.*\//,"",f); sub(/\.sql.*/,"",f); next}
  /^[[:space:]]*-[[:space:]]*latency average/ && f!="" {
    n=$0; sub(/.*=[[:space:]]*/,"",n); sub(/[[:space:]].*/,"",n); print f, n }
  ' "$1"; }
med_stdin(){ sort -n | awk '{a[NR]=$1} END{ if(NR==0){print "NA"}
             else print (NR%2)? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2 }'; }

DATA="$OUT/_latdata.tmp"; : > "$DATA"
TPSD="$OUT/_tpsdata.tmp"; : > "$TPSD"

echo "== 行缓存 A/B  模式=$MODE  仓数=$WAREHOUSES  每轮=${DURATION}s x ${ITERS}轮  并发=$CLIENTS 线程=$JOBS =="
echo "   (每组约 $((DURATION * ITERS / 60)) 分钟, 三组共约 $((DURATION * ITERS * 3 / 60)) 分钟)"
for grp in OFF LOADONLY ON; do
  for i in $(seq 1 "$ITERS"); do
    f="$OUT/${grp}_run${i}.txt"
    printf "  %-9s run%d/%d 运行中(%ds)... " "$grp" "$i" "$ITERS" "$DURATION"
    case "$grp" in
      OFF)      drop; run off "$f" ;;
      LOADONLY) load; run off "$f" ;;
      ON)       load; run on  "$f" ;;
    esac
    [ "$grp" = "OFF" ] || show_stats
    t=$(tps_of "$f")
    if [ -z "$t" ]; then
      echo "FAILED (无 tps, 见 $f)"; grep -iE "error|fatal|too many" "$f" | head -2 | sed 's/^/      /'
      continue
    fi
    echo "tps=$t"
    echo "$grp $t" >> "$TPSD"
    lat_of "$f" | while read -r s v; do echo "$grp $s $v" >> "$DATA"; done
  done
done

echo
echo "== 总混合 TPS 中位数 =="
off=$(awk '$1=="OFF"{print $2}' "$TPSD" | med_stdin)
lo=$(awk '$1=="LOADONLY"{print $2}' "$TPSD" | med_stdin)
on=$(awk '$1=="ON"{print $2}' "$TPSD" | med_stdin)
awk -v off="$off" -v lo="$lo" -v on="$on" 'BEGIN{
  printf "  %-10s %12s  %s\n","OFF",off,"(基线)"
  if(off+0>0 && lo!="NA") printf "  %-10s %12s  %+.1f%%\n","LOADONLY",lo,(lo/off-1)*100; else printf "  %-10s %12s\n","LOADONLY",lo
  if(off+0>0 && on!="NA") printf "  %-10s %12s  %+.1f%%\n","ON",on,(on/off-1)*100; else printf "  %-10s %12s\n","ON",on
}'

echo
echo "== 逐语句平均延迟中位数 (ms) =="
printf "  %-14s %9s %9s %9s\n" "脚本" "OFF" "LOADONLY" "ON"
for s in read_item read_stock read_customer update_stock insert_item delete_item; do
  o=$(awk -v g=OFF      -v s="$s" '$1==g&&$2==s{print $3}' "$DATA" | med_stdin)
  l=$(awk -v g=LOADONLY -v s="$s" '$1==g&&$2==s{print $3}' "$DATA" | med_stdin)
  n=$(awk -v g=ON       -v s="$s" '$1==g&&$2==s{print $3}' "$DATA" | med_stdin)
  printf "  %-14s %9s %9s %9s\n" "$s" "$o" "$l" "$n"
done

echo
echo "原始逐轮报告: $OUT/{OFF,LOADONLY,ON}_run*.txt"
echo "提示: 若各组 TPS 波动带重叠, 以 read_stock 逐语句延迟为准信号;"
echo "      加大 DURATION / 调小 shared_buffers / 用 zipfian 分布放大信号。"
