#!/usr/bin/env bash
# ================================================================
# 行缓存诊断: 高并发回填是否饿死 washer
#
# 待验证假设 H:
#   回填每次点查 miss 时持 rm->build_lock SHARED(整个 flatten+分配+
#   插链过程); washer 淘汰段时对 victim 表用 conditional EXCLUSIVE。
#   高并发下 SHARED 几乎常驻 -> washer 的 conditional 必然失败 ->
#   单表场景无第二个属主可换 -> SegEvictOne 返回 -1 -> 一段都洗不出来
#   -> 空闲链耗尽后再也补不上 -> 回填全部失败 -> 缓存内容冻结。
#
# 三个对照组(其余变量全部相同):
#   A 低并发   : 并发小 -> SHARED 持锁密度低 -> washer 应能正常洗段
#   B 高并发   : 复现现场 -> 若 H 成立, free 恒 0 且回收计数冻结
#   C 高并发+双表: 另加一张预热的表给 washer 当"退路"
#                  -> 若 H 成立(且瓶颈确在"单表无退路"), C 应优于 B
#
# 关键判据(两个, 必须同时看):
#   ① free 段数是否长期为 0        —— 空闲链有没有饿死
#   ② sum(seq_num) 是否停止增长     —— washer 到底有没有在洗段
#      (seq_num 每次段回收 +1, 是 washer 工作量的累计计数)
#   H 成立的特征: 高并发下 free 恒 0 **且** seq_num 总和冻结;
#                 低并发下两者都正常。
#
# 用法:
#   PGBIN=/path/pg/bin PGPORT=5432 PGDB=benchmarksql PGUSER=... \
#   PGPASSWORD=... WAREHOUSES=100 DURATION=60 \
#   LOW_CLIENTS=4 HIGH_CLIENTS=100 \
#   bash diag_washer_starvation.sh
#
# 前置: 行缓存 build(需 pg_row_cache_segments / _relation_stats /
#       pg_enable_relation_row_cache); 已导入 bmsql_stock、bmsql_item;
#       row_cache_size 明显小于 stock 缓存占用(否则不会触发淘汰)。
# ================================================================
set -uo pipefail

PGBIN="${PGBIN:-/usr/local/pgsql/bin}"
PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGDB="${PGDB:-benchmarksql}"
PGUSER="${PGUSER:-postgres}"
WAREHOUSES="${WAREHOUSES:-100}"
DURATION="${DURATION:-60}"
LOW_CLIENTS="${LOW_CLIENTS:-4}"
HIGH_CLIENTS="${HIGH_CLIENTS:-100}"
JOBS="${JOBS:-$( (command -v nproc >/dev/null && nproc) || echo 8 )}"
SAMPLE_SEC="${SAMPLE_SEC:-2}"
OUT="${OUT:-/tmp/rc_washer_diag}"
mkdir -p "$OUT"

PSQL="$PGBIN/psql"
PGBENCH="$PGBIN/pgbench"
export PGHOST PGPORT PGUSER PGDATABASE="$PGDB"
CONN=(-h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB")
Q(){ "$PSQL" "${CONN[@]}" -X -A -t -q -c "$1" 2>/dev/null; }
die(){ echo "ERROR: $*" >&2; exit 1; }

# ---- 前置检查 ----
command -v "$PGBENCH" >/dev/null 2>&1 || die "找不到 pgbench: $PGBENCH"
Q "select 1" >/dev/null || die "连不上 $PGHOST:$PGPORT/$PGDB"
[ "$(Q "select count(*) from pg_proc where proname='pg_row_cache_segments'")" = "1" ] \
  || die "目标实例缺少 pg_row_cache_segments —— 连错实例或未重新 initdb?"
[ "$(Q "select count(*) from pg_class where relname='bmsql_stock'")" -ge 1 ] \
  || die "找不到 bmsql_stock"
NSEG=$(Q "select count(*) from pg_row_cache_segments()")
POOL=$(Q "show row_cache_size")
# washer 是不接数据库的 bgworker, 不出现在 pg_stat_activity —— 用进程名查
if ! (ps -eo comm=,args= 2>/dev/null | grep -q "[r]ow cache washer"); then
  echo "WARNING: 未在进程列表中看到 'row cache washer'(若非本机运行 server 可忽略)" >&2
fi

# ---- pgbench 负载脚本(zipfian 读写, 与生产实验一致)----
cat > "$OUT/rs.sql" <<EOF
\\set wid random(1, $WAREHOUSES)
\\set r random_zipfian(1, 100000, 1.01)
\\set iid 1 + permute(:r - 1, 100000)
SELECT s_quantity, s_data FROM bmsql_stock WHERE s_w_id = :wid AND s_i_id = :iid;
EOF
cat > "$OUT/us.sql" <<EOF
\\set wid random(1, $WAREHOUSES)
\\set r random_zipfian(1, 100000, 1.01)
\\set iid 1 + permute(:r - 1, 100000)
UPDATE bmsql_stock SET s_ytd = s_ytd + 1 WHERE s_w_id = :wid AND s_i_id = :iid;
EOF

# ---- 采样: 每 SAMPLE_SEC 秒记一行 free/full/active/回收总数/条目数 ----
sample_loop(){  # $1 = csv 文件
  echo "elapsed,free,full,active,total_recycles,entries" > "$1"
  local t0 now
  t0=$(date +%s)
  while :; do
    now=$(( $(date +%s) - t0 ))
    line=$(Q "select count(*) filter (where state='free')||','||
                     count(*) filter (where state='full')||','||
                     count(*) filter (where state='active')||','||
                     coalesce(sum(seq_num),0)||','||
                     coalesce(sum(n_entries),0)
              from pg_row_cache_segments()")
    [ -n "$line" ] && echo "$now,$line" >> "$1"
    sleep "$SAMPLE_SEC"
  done
}

reset_cache(){   # 只留 stock(enable, 不预热)
  Q "select pg_drop_relation_row_cache('bmsql_stock')" >/dev/null
  Q "select pg_drop_relation_row_cache('bmsql_item')"  >/dev/null
  sleep 1
  Q "select pg_enable_relation_row_cache('bmsql_stock')" >/dev/null
}

run_phase(){  # $1=标签  $2=并发  $3=是否加第二张表(yes/no)
  local tag="$2c_$1" clients="$2" second="$3"
  local csv="$OUT/${1}.csv"

  echo
  echo "---------- 阶段 $1 : 并发=$clients  第二张表=$second ----------"
  reset_cache
  if [ "$second" = "yes" ]; then
    # 预热 item, 给 washer 一个可淘汰的"退路"属主(load 完再查段数)
    Q "select pg_load_relation_row_cache('bmsql_item')" >/dev/null
    local isegs; isegs=$(Q "select coalesce(n_segments,0) from pg_row_cache_relation_stats('bmsql_item'::regclass)")
    echo "  已 load bmsql_item 作为 washer 的退路 (占 ${isegs:-0} 段)"
  fi

  sample_loop "$csv" & local spid=$!
  PGOPTIONS='-c row_cache_backfill=on' "$PGBENCH" "${CONN[@]}" \
    -f "$OUT/rs.sql@90" -f "$OUT/us.sql@10" \
    -c "$clients" -j "$JOBS" -T "$DURATION" -M prepared -n \
    > "$OUT/${1}_pgbench.txt" 2>&1
  kill "$spid" 2>/dev/null; wait "$spid" 2>/dev/null

  local tps; tps=$(awk '/tps = [0-9.]+ \(without/{print $3; exit}' "$OUT/${1}_pgbench.txt")
  local st; st=$(Q "select hit_count||','||miss_count||','||round(hit_ratio::numeric,2)||','||backfill_count
                    from pg_row_cache_relation_stats('bmsql_stock'::regclass)")
  echo "  tps=$tps   stock hit/miss/hit%/backfill = $st"

  # 分析采样序列
  awk -F, -v tag="$1" 'NR>1{
      n++; if($2==0) z++; if(n==1){r0=$5; f0=$2} rn=$5; fmin=(n==1||$2<fmin)?$2:fmin; fmax=(n==1||$2>fmax)?$2:fmax
    } END{
      if(n==0){print "  (无采样数据)"; exit}
      printf "  free段: 首=%d 末段最小=%d 最大=%d  为0的采样占比=%.0f%% (%d/%d)\n", f0, fmin, fmax, 100*z/n, z, n
      printf "  回收计数 sum(seq_num): 起=%d 止=%d 增量=%d\n", r0, rn, rn-r0
      printf "%s,%d,%.0f,%d\n", tag, fmin, 100*z/n, rn-r0 >> "'"$OUT"'/summary.csv"
    }' "$csv"
}

: > "$OUT/summary.csv"
echo "=========================================="
echo " washer 饥饿诊断"
echo " pool=$POOL ($NSEG 段)  仓数=$WAREHOUSES  每阶段=${DURATION}s  采样=${SAMPLE_SEC}s"
echo " 低并发=$LOW_CLIENTS  高并发=$HIGH_CLIENTS  线程=$JOBS"
echo "=========================================="

run_phase A_low  "$LOW_CLIENTS"  no
run_phase B_high "$HIGH_CLIENTS" no
run_phase C_high_2tables "$HIGH_CLIENTS" yes

# ---- 判据 ----
echo
echo "=========================================="
echo " 判据汇总 (free最小值 / free为0占比 / 本阶段回收段数)"
echo "=========================================="
awk -F, '{printf "  %-18s free_min=%-5s zero=%-4s%% recycles=%s\n",$1,$2,$3,$4}' "$OUT/summary.csv"

A_REC=$(awk -F, '/A_low/{print $4}' "$OUT/summary.csv")
B_REC=$(awk -F, '/B_high,/{print $4}' "$OUT/summary.csv")
B_ZERO=$(awk -F, '/B_high,/{print $3}' "$OUT/summary.csv")
C_REC=$(awk -F, '/C_high_2tables/{print $4}' "$OUT/summary.csv")

echo
if [ -z "${A_REC:-}" ] || [ -z "${B_REC:-}" ]; then
  echo " 结论: 采样数据不完整, 检查各阶段 CSV: $OUT/*.csv"
elif [ "${B_REC}" -eq 0 ] && [ "${A_REC}" -gt 0 ]; then
  echo " 结论: 假设 H 成立(完全饥饿) —— 高并发下 washer 一段都洗不出来。"
  echo "       低并发回收 $A_REC 段, 高并发回收 0 段, free 为 0 的采样占 ${B_ZERO}%。"
  [ "${C_REC:-0}" -gt 0 ] && \
  echo "       加入第二张表后回收 $C_REC 段 > 0 —— 印证'单表无退路'是关键一环。"
elif [ "${A_REC}" -gt 0 ] && [ "$(( B_REC * 3 ))" -lt "${A_REC}" ]; then
  echo " 结论: 假设 H 部分成立(严重饥饿) —— 高并发洗段能力降到低并发的 1/3 以下"
  echo "       (低并发 $A_REC 段 vs 高并发 $B_REC 段), free 为 0 占 ${B_ZERO}%。"
elif [ "${B_REC}" -gt 0 ] && [ "${B_ZERO:-0}" -ge 70 ]; then
  echo " 结论: washer 仍在洗段($B_REC 段, 与低并发 $A_REC 段相当), 锁饥饿不成立;"
  echo "       但 free 长期为 0(${B_ZERO}%) —— 瓶颈是 washer 洗段吞吐跟不上回填消耗,"
  echo "       而非拿不到 build_lock。此时应从'提高 target 水位/加快洗段/降低回填速率'入手。"
else
  echo " 结论: 未复现饥饿 —— 高并发回收 $B_REC 段, free 为 0 占 ${B_ZERO}%, 缓存供给正常。"
fi
echo
echo " 明细: $OUT/{A_low,B_high,C_high_2tables}.csv  (elapsed,free,full,active,total_recycles,entries)"
echo " 提示: 用 'column -s, -t' 或导入表格查看 free 与 total_recycles 两列随时间的走势最直观。"
