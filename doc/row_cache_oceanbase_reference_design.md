# OceanBase 行缓存架构解析与 PG 行缓存借鉴改造方案

> 本文档基于 oceanbase-master 源码逐行核实(所有代码位置均为实际验证过的行号),
> 先完整描述 OceanBase 行缓存的设计方案,再给出 PG 行缓存(dev_rowcache 分支,V4 实现)
> 借鉴其架构的改造方案。
>
> 配套阅读:`doc/row_cache_design.md`(PG 当前 V4 实现的设计文档)。

---

# 第一部分 OceanBase 行缓存设计方案

## 1. 总体定位与存储模型

### 1.1 两级行缓存

OceanBase 的"行缓存"是两个独立缓存,均构建在统一 KVCache 框架上,由单例
`ObStorageCacheSuite`(宏 `OB_STORE_CACHE`)持有
(`src/storage/blocksstable/ob_storage_cache_suite.h:18`):


| 缓存    | 类                | 缓存内容                            | Key 组成                                                         |
| ----- | ---------------- | ------------------------------- | -------------------------------------------------------------- |
| 单表行缓存 | `ObRowCache`     | **单个 SSTable 内**的某行(含"行不存在"负缓存) | tenant_id + tablet_id + rowkey + **data_version** + table_type |
| 融合行缓存 | `ObFuseRowCache` | **跨 LSM 各层融合(fuse)后的最终行**       | tablet_id + rowkey + 快照版本信息                                    |


一次点查的完整缓存链:
**融合行缓存 → (每个 SSTable 上) 布隆过滤器缓存 → 单表行缓存 → 索引/数据微块缓存 → 磁盘**。

关键前提:**行缓存只服务 SSTable(不可变),MemTable(可变)永远现读、从不缓存**。
这是整套失效设计的根基(见 §5)。

### 1.2 Key/Value 结构

`ObRowCacheKey`(`ob_row_cache.h:19-51`):

- `tenant_id_ / tablet_id_ / data_version_ / table_type_ / rowkey_(ObDatumRowkey) / datum_utils`_;
- `data_version_` 与 `table_type_` **参与 hash 与 equal**(`ob_row_cache.cpp:53,72`)——
这是"版本对不上=自动失效"的实现载体;
- rowkey 为**变长深拷贝**(`deep_copy` 序列化实际字节进 memblock),
`datum_utils_` 携带类型感知比较——因此**支持任意类型、任意列数的主键**。

`ObRowCacheValue`(`ob_row_cache.h:53-79`):

- 展平的 `ObStorageDatum` 数组 + 列数 + `start_log_ts_`(所属 SSTable 的 start_scn)+ DML flag;
- **负缓存**:`set_row_not_exist()` 把 `size_=0` 作为"该 SSTable 里没有这行"的标记
(`ob_row_cache.h:62-63`),命中负缓存即可跳过下探。

### 1.3 KVCache 底座:全局哈希表

所有租户、所有缓存(最多 48 种)共用一张开链哈希表 `ObKVCacheMap`
(`ob_kvcache_map.h`),默认 1000 万桶:

- **二级桶数组**:`Bucket* buckets_` → `Node`* 子数组,按 4K/64K/1M/16M 分档分配
(`ob_kvcache_map.h:27-32`),避免一次性大块连续内存;
- **Node** 只存索引信息:hash_code(混入 cache_id)、key/value 指针(指向 memblock 内的深拷贝)、
`mb_handle_ + seq_num`_、get_cnt;
- **读路径完全无锁**(`ObKVCacheMap::get`,`ob_kvcache_map.cpp:273`):
hazard version 保护链表遍历;命中时 `protect(mb_handle, seq_num)` pin 住内存块并校验 seq_num,
**seq_num 不匹配 = 块已被洗 = 按 miss 处理**(惰性失效);命中零拷贝,返回句柄直接引用 memblock 内部;
- **写路径分段桶锁**(`ObBucketLock`,`ob_kvcache_map.cpp:164`):只锁单桶,头插 + overwrite 删旧,
顺带清死节点、搬碎片节点;
- **LRU→LFU 热点迁移**(`ob_kvcache_map.cpp:341`,阈值 `need_modify_cache`,`ob_kvcache_map.h:96`):
某 KV 的个体 get_cnt 超过"所在块平均 get + 2"且在 LRU 块时,重新 store 到该实例的 LFU 块。
热 KV 不断从冷块逃逸 → 冷块整体得分下降被整块淘汰。**这是"2MB 整块回收"不误杀热点的关键补丁**。

## 2. 内存管理策略

### 2.1 三层配额

1. **服务器级**:真正约束是 `memory_limit - memory_used` + 紧急预留内存;
2. **租户级**:`lower_limit ~ upper_limit`;每个 2MB 块都以租户名义向租户资源管理器申请
  (`alloc_cache_mb`,`ob_kvcache_store.cpp:1674`),记租户账、打"cache 内存"标记——**随时可被业务抢回**;
3. **实例级**(tenant × cache_id):`store_size`_ 实时统计、`hold_size`(洗后保底)、
  `mem_limit_pct`(占租户缓存内存上限比例)。

### 2.2 MemBlock:2MB 块 bump 分配

- KV 全部深拷贝进 `ObKVStoreMemBlock`(2MB,块头+payload);
- 块内分配是**无锁 bump-pointer**:64 位原子变量打包 `{offset, pairs}`,CAS 推进
(`ObKVStoreMemBlock::store`,`ob_kvcache_struct.cpp`);
- **块内永不删除单条 KV,回收只以整块为单位**;
- 每个缓存实例持有两个"当前写入块"(`handles_[LRU]`/`handles_[LFU]`),写满 CAS 换新块,
旧块置 FULL(`ob_kvcache_store.cpp:125-160`)——**只有 FULL 块可被淘汰**;
- 大 KV(>2MB payload)单独分配独占大块,写完立即 FULL(`ob_kvcache_store.cpp:101-124`);
- 句柄数组 `mb_handles_` 启动时按上限预分配,空闲句柄走无锁 FIFO 池,
批量补充(`try_supply_mb`,一次 128 个)——**分配热路径零动态元数据分配**。

### 2.3 淘汰打分:衰减 LFU × 种类优先级

```
score = score × 0.9 + recent_get_cnt × priority        (ob_kvcache_store.cpp:377)
```

- `priority` 是缓存注册时的种类权重——行缓存/微块缓存/布隆缓存**在同一打分体系里竞争**,
不划固定内存池,配比由负载自动决定;
- 淘汰依据是**整块得分**,单行的存活取决于所在块的整体冷热。

### 2.4 租户洗量计算(`compute_tenant_wash_size`,`ob_kvcache_store.cpp:1296`)

```
reserve_mem   = upper≤1GB ? upper/10 : log10(upper/1GB)×upper/20 + 100MB
min_wash_size = max(0, 租户内存使用 - upper_limit + reserve_mem)   # 必须洗
max_wash_size = max(0, 租户内存使用 - lower_limit + reserve_mem)   # 最多洗
```

服务器压力在 Σmin 与 Σmax 之间按各租户可洗空间比例分摊;
**"不值得洗就不洗"**:要洗量 < 缓存总量/256(clamp 8MB~256MB)时整轮跳过
(`is_tenant_wash_valid`,`ob_kvcache_store.cpp:1471`)。

## 3. 读触发点

### 3.1 拦截位置

`ObIndexTreePrefetcher::lookup_in_cache()`(`ob_index_tree_prefetcher.cpp:175`):
在 SSTable 索引树下探**之前**探测行缓存,命中则 `row_state_ = IN_ROW_CACHE`,
后续 `ObMicroBlockRowGetter::get_row()` 直接用缓存值投影,跳过索引树下探 + 数据微块 IO。

### 3.2 触发条件

- **迭代器类型**:`IteratorSingleGet`(`ob_index_tree_prefetcher.cpp:164`)与
`IteratorMultiGet`(回表批量取,`ob_index_tree_prefetcher.cpp:634`)走行缓存;
`**IteratorScan` 不走**(扫描不污染缓存);`IteratorRowLockCheck`(DML 唯一性/锁检查)不走;
- `**enable_get_row_cache()`**(`ob_table_access_context.h:150`):
`query_flag 允许 && !use_fuse_row_cache_ && !need_scn_ && 非内部 tablet && 无 mds filter`。
`**need_scn_` 是关键闸门**:需要事务版本信息的读(加锁检查、唯一性检查)一律绕开行缓存,
因为缓存值不携带完整事务状态;
- **命中后二次校验**:`row_value_->get_start_log_ts() == sstable_->start_scn`
(`ob_index_tree_prefetcher.cpp:195`),不符按 miss——防 tablet 迁移/重建后的错配。

### 3.3 融合行缓存读

`ObSingleMerge` 先查融合行缓存(`ob_single_merge.cpp:168`);命中且**读快照版本落在可用区间**
(无更新的表/无 MemTable 新版本)才算 final,否则判定不可用 → 回退重新融合。
`use_fuse_row_cache_` 为真时单表行缓存 get/put 均关闭(两层互斥,不重复缓存)。

## 4. 写触发点

### 4.1 单表行缓存:点查 miss 后回填

唯一写入点 `ObMicroBlockRowGetter::get_block_row()`
(`ob_micro_block_row_getter.cpp:385-402`),须同时满足三关:

1. `!use_fuse_row_cache_`(融合缓存路径跳过单表回填);
2. `enable_put_row_cache()`(点查标志 + `!need_scn_` 等,同 §3.2);
3. `read_with_same_schema()`(读 schema 列数 == 存储基线 schema 列数,
  `ob_table_access_param.cpp:359`)。

特性:

- **粒度=单行,时机=查询时(lazy)**:查哪行缓存哪行,没有整表灌入的入口;
- **负缓存同路径**:微块中无此行时 `get_not_exist_row` 造 `DF_NOT_EXIST` 行照常 put
(`ob_micro_block_row_getter.cpp:366-372,415`);
- **put 纯 best-effort**:`if (OB_SUCCESS == put_row(...))` 才计数,失败静默忽略、不打 WARN、
不阻塞查询(`ob_micro_block_row_getter.cpp:400`)。

### 4.2 融合行缓存:重新融合后回写

`ObSingleMerge::inner_get_next_row`(`ob_single_merge.cpp:289-297`):
缓存不可用 → 回退逐表读+融合(**重新融合本身不写缓存,是读+算**)→ 融合完成后满足
`!have_uncommited_row && need_update_fuse_cache && enable_put_fuse_row_cache()` 才回写。
**有未提交事务行时绝不回写**(未提交数据可能回滚)。

### 4.3 DML 不写

INSERT/UPDATE/DELETE 只写 MemTable,**不向任何行缓存 put**。新行要等落盘成 SSTable、
被后续点查 miss 后才逐行进入缓存。DML 内部的唯一性/锁检查读也被
`IteratorRowLockCheck + need_scn_` 双闸挡在缓存外(§3.2)。

## 5. 失效触发点

**核心哲学:能被动失效就不主动删。** `ObRowCache`/`ObFuseRowCache` 甚至不暴露 erase 接口。


| 失效场景          | 机制                                         | 性质     |
| ------------- | ------------------------------------------ | ------ |
| compaction 换代 | 新 SSTable data_version 变 → 旧 key 永远查不到     | 被动,零成本 |
| tablet 迁移/重建  | 命中后 start_log_ts 校验不符 → 按 miss             | 被动     |
| MemBlock 被洗   | `seq_num_++` → 哈希表老节点 protect 失败 → 按 miss  | 被动(惰性) |
| DML 更新/删除     | **无任何动作**(见下)                              | —      |
| DDL 变更        | put 闸门 + key 隔离(见 §7)                      | 被动     |
| 租户下线/运维       | `ObKVGlobalCache::erase_cache`(整缓存/整租户粗粒度) | 主动,仅兜底 |


**DML 零失效的原理**:单表行缓存缓存的是"row X 在 SSTable(版本 V)里的样子"——
一个**不可变事实,永不过期**。DML 的新版本进 MemTable;读路径中 MemTable **永远被读**、
参与融合,新版本覆盖缓存返回的旧版本 → 结果正确。负缓存同理:"K 不在 SSTable V 里"
不因 INSERT K(进的是 MemTable)而变错。
融合行缓存则靠**读时快照版本校验**发现自己过时并自动作废,同样不需要 DML 通知。

## 6. 空间回收策略

**回收与失效解耦**:compaction/DDL 只让旧项"逻辑失效"(查不到),
**物理空间回收完全由 KVCache wash 机制独立完成**。

### 6.1 三个触发入口


| 入口                | 触发                                                                               | 选块策略                                                      |
| ----------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------- |
| 后台 `wash()`       | 定时任务,默认 **200ms** 一轮(`_cache_wash_interval`,1ms~3s,`ob_parameter_seed.ipp:1361`) | 全量刷得分 → 每租户 Top-K 最冷堆(K=配额/2MB)                           |
| `sync_wash_mbs()` | 业务分配内存失败时同步调用                                                                    | 沿租户块链表扫,遇 FULL 就洗,**不挑冷热**,100ms 超时                       |
| put 分配失败          | 缓存回填拿不到 2MB 块                                                                    | `sync_wash` 洗一把重试一次,再失败放弃回填(`ob_kvcache_store.cpp:73-80`) |


### 6.2 两个回收时刻

1. **逻辑回收**(选中瞬间):`CAS(FULL→FREE) + seq_num_++`(`ob_kvcache_store.cpp:1504-1505`)
  ——新读者立即 miss,数据逻辑消失;**无条件、不等任何读者**;
2. **物理回收**(最后一个读者松手后):hazard pointer `reclaim` 确认无人 pin →
  `do_wash_mb` 析构块内 KV → 2MB 归还租户资源管理器(`ob_kvcache_store.cpp:1577`)。
   引用计数模式下 ref 减不到 0 就**跳过该块洗下一块**(`try_wash_mb`,`ob_kvcache_store.cpp:1561`),
   救急路径的延迟上限优先于回收率。
3. **句柄第三时刻**:mb_handle 摘链后进 RetireStation(QClock 分代),确认无遍历者才 reset 回池
  (普通线程攒 16 个、wash 线程攒 2048 个批量 purge,`ob_kvcache_store.cpp:1755`)。

**整条内存路径无一处无限等待**:put 失败丢弃、sync_wash 100ms 超时、洗块不等读者——
这是缓存与业务内存共池弹性共享的前提。

## 7. DML 与 DDL 对行缓存的影响汇总


| 操作                 | 写缓存? | 失效动作?              | 说明                                                                         |
| ------------------ | ---- | ------------------ | -------------------------------------------------------------------------- |
| INSERT             | 否    | 无                  | 新行进 MemTable;唯一性检查被 `RowLockCheck+need_scn` 挡在缓存外;负缓存无需清理                  |
| UPDATE/DELETE      | 否    | 无                  | 新版本进 MemTable,读时融合覆盖;SSTable 与缓存内容原地不动                                     |
| compaction         | 否    | 被动(data_version 变) | 旧项成死项等 wash;新 SSTable 缓存从零重新捂热                                             |
| 加列/改列等 DDL         | 否    | 被动                 | `read_with_same_schema` 闸门:schema 列数不符即不回填;key 内 datum_utils/版本隔离使新读者不命中旧项 |
| truncate/重建 tablet | 否    | 被动(tablet_id/版本变)  | 旧项成孤儿死项                                                                    |
| 租户删除/运维清缓存         | —    | 主动 `erase_cache`   | 唯一的主动清理路径,粗粒度(`ob_rpc_processor_simple.cpp:1637`)                          |


---

# 第二部分 PG 借鉴 OceanBase 的行缓存改造方案

## 8. 现状(V4)与差距

当前实现(`src/backend/lib/relation_row_cache.c`,详见 `doc/row_cache_design.md`):

- DSA 常驻的 `(relid, pkey)` 链地址哈希,128 个 LWLock 分区锁;
- 条目 `GlobalEntry` + `FlatCachedTuple` 合并**逐条 dsa_allocate**;
- 填充靠 `pg_load_relation_row_cache` **主动整表灌入**;
- 失效:DML 钩子(`heap_update`/`heap_delete`)按旧主键**逐条主动摘除**;
DDL 靠 relcache 回调置 `RelMeta.state = DISABLED` 整表短路;
- 主键仅支持 by-val、≤8 列、序列化 ≤32 字节;
- 命中路径:`ExecIndexNextRowCache` 在 IndexNext 之前探测,不可见/HOT 链回退原生路径。

对照 OceanBase,可借鉴与不可借鉴的边界必须先划清:

### 8.1 架构差异裁定(哪些能抄、哪些不能)


| OceanBase 做法                   | 能否移植到 PG        | 裁定理由                                                                                                                                                     |
| ------------------------------ | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2MB 块 bump 分配 + 整块淘汰           | ✅ 可             | 与存储引擎无关,纯内存组织                                                                                                                                            |
| seq_num 惰性失效 + 无锁读 pin         | ✅ 可             | 同上                                                                                                                                                       |
| 衰减 LFU 打分 + Top-K 洗块           | ✅ 可             | 同上                                                                                                                                                       |
| 负缓存("行不存在")                    | ✅ 可(需 DML 配合失效) | 见 §10.5                                                                                                                                                  |
| 点查 miss 按需回填 + best-effort put | ✅ 可             | 与 load 模型互补                                                                                                                                              |
| 变长任意类型主键                       | ✅ 可             | 序列化模型升级,见 §10.6                                                                                                                                          |
| **DML 零失效**                    | ❌ **不可**        | PG 堆表 UPDATE/DELETE **原地改旧元组 xmax**;缓存副本看不到这次原地修改,可见性判断会"自信地判错"(判旧版本可见)而**不触发回退**——静默脏读。OB 能免失效是因为 SSTable 真不可变 + MemTable 必读兜底;PG 没有必读新鲜层,命中即跳过堆表,失效不可省 |
| 缓存旧版本、靠读时合并纠正                  | ❌ 不可            | 同上:PG 新旧版本同居一张堆表同一索引,无法"只跳过旧的、廉价读新的";每次读堆验证 = 收益归零                                                                                                       |
| 只缓存已提交行 + 可见性判断回退              | ✅ 保留(已有)        | 但它只能处理"快照太旧/HOT 链"类问题,**不能替代 DML 失效**(xmax 盲区)                                                                                                           |


**一句话准则:内存架构抄 OceanBase,失效机制留 PG。**

## 9. 改造后的总体架构

```
共享内存布局(替换现有 DSA 逐条分配):
┌─────────────────────────────────────────────────┐
│ RowCacheControl(不变: RelMeta[64]、分区锁可先保留) │
├─────────────────────────────────────────────────┤
│ 哈希索引层: 桶数组 + Node{hash, pkey引用,          │
│   seg_id + seq_num, entry偏移, next}              │
├─────────────────────────────────────────────────┤
│ 数据层: N 个固定大小 Segment(建议 1MB~2MB)         │
│   段内 bump 分配 FlatCachedTuple(含 GlobalEntry 头) │
│   段头: {seq_num, status(FREE/USING/FULL),        │
│          pin_cnt, get_cnt, recent_get_cnt, score} │
└─────────────────────────────────────────────────┘
```

哈希节点不再拥有数据内存,只持有 `(seg_id, seq_num, offset)` 三元组;
数据全部住进段;回收以段为单位。

## 10. 分项改造方案

### 10.1 内存组织:逐条 dsa_allocate → 段式 bump 分配

- 装载/回填时向"当前写入段"CAS bump 出空间,写入 FlatCachedTuple(条目自带序列化 pkey,
段可自描述,便于回收时反查哈希节点或直接依赖惰性清理);
- 段写满置 FULL,换新段;超大行(宽表阈值外)单独分配独占段或维持现状拒缓存;
- **收益**:消除逐条 dsa_allocate/dsa_free 的分配器竞争与碎片;DML 失效不再同步 dsa_free
(见 10.3);load 大表时分配路径大幅变短。

**内存供给模式:固定池全预分配(决策)。**
澄清:OceanBase 本身是"元数据静态 + 数据块弹性"——句柄数组/桶数组启动时预分配,
但 2MB 数据块按需向租户资源管理器借、洗块时归还。该弹性的前提是缓存与业务内存
共享同一个租户池,PG 不具备这个对手方(各共享结构各自固定),故不仿照。

PG 采用**启动时全预分配的固定段池**(GUC `row_cache_size`,PGC_POSTMASTER,
决定段数;沿用 `row_cache_hash_buckets` 定桶数,桶数组同样挪到启动时分配):

- 运行期**零分配调用、零失败路径**:写入=段内 CAS bump,"池满"不是失败而是
淘汰触发(语义同 shared_buffers 的 eviction),从根上消除动态分配阻塞类问题;
- 符合 PG 共享内存"启动定大小"的惯例,DBA 心智模型同 shared_buffers;
- 代价:空闲时内存常驻、改容量需重启——对目标场景(常驻热点维表)可接受。

分配粒度对比:V4 每缓存一行一次 dsa_allocate(高频、动态、可失败);
改造后启动划池、运行期只有段内指针推进与整段复用(低频到零)。

### 10.2 读路径:seq_num 校验 + pin,迈向少锁

分两步走,避免一次性引入 hazard pointer 的复杂度:

- **第一步(保留分区锁)**:命中仍在分区共享锁内,但增加段 `seq_num` 校验与
`pin_cnt++`(原子),出临界区后**零拷贝**使用缓存元组,用完 `pin_cnt--`。
当前实现要在锁内完成拷贝/构造 slot,pin 化后锁持有时间缩短为"找到节点+校验+pin";
- **第二步(可选,无锁读)**:桶指针原子读 + 节点 retire 延迟释放(PG 侧可用
每 backend epoch/最小活跃计数模拟 QClock),完全去掉读侧分区锁。
风险较高,建议在第一步收益量化后再决策。

### 10.3 失效:同步摘链释放 → 逻辑失效 + 段回收

保留 DML 钩子(**必须**,§8.1),但失效动作降级为廉价操作:

- UPDATE/DELETE 钩子:按旧 pkey 找到节点,**摘链即可,不再 dsa_free 载荷**
——载荷留在段里成为死数据,等段回收;
- 段回收时不需要遍历清哈希:段 `seq_num_++` 后,指向该段的残余节点在读路径
protect 失败时顺手摘除(OB 的 `internal_map_erase` 惰性清理),
或由后台轻量任务扫描(对应 OB `clean_garbage_node`);
- **DDL:信号与裁决分离(schema 指纹复核)**。
relcache inval 是脏信号——VACUUM/ANALYZE(pg_class inplace 更新)、GRANT、
CREATE INDEX、sinval 队列溢出(relid=InvalidOid 全量失效)都会触发,
直接在回调里置 DISABLED 会被例行维护误杀(V4 实践已证实,当前回调中
该逻辑被注释,反而留下真 DDL 无防护的正确性洞)。改为:
  - RelMeta 存 **schema 指纹** = (relfilenumber, TupleDesc 摘要 hash,
  主键描述符):分别抓住 TRUNCATE/重写(TID 全变)、加删改列、换/删主键;
  - 回调只清 backend 本地 LastLookup 指针(含 InvalidOid 溢出分支),
  **不碰 state**;
  - relcache 重建自动清空 rd_rowcache_meta → 下次绑定
  (RelationRowCacheBindRelation)重算指纹比对:相同 → 保持 ENABLED
  (vacuum/analyze/grant 零损失);不同 → 置 DISABLED + 摘全部条目;
  - 安全性:凡改指纹的 DDL 均持 AccessExclusiveLock,后续查询拿锁时
  先 AcceptInvalidationMessages 再绑定复核,脏缓存无窗口可读;
  弱锁操作恰好都不改指纹,幸存即正确。
  这是 OB "read_with_same_schema 闸门 + key 版本隔离"(事件不可信,
  内容比对裁决)在 PG 侧的等价实现。

### 10.4 空间回收:衰减 LFU 打分 + 整段淘汰

- 段头维护 `recent_get_cnt`,后台(或借调 autovacuum-style worker / 定时钩子)周期执行:
`score = score × 0.9 + recent_get_cnt`;每关系可配 priority 权重;
- 内存水位超限时选 score 最低的 FULL 段:`seq_num_++`(逻辑回收)→
等 `pin_cnt` 归零 → 整段 reset 复用(物理回收);pin 不归零则跳过洗下一段;
- 实现 OB 的"两不等"纪律:**洗段不等读者、put 拿不到段就放弃回填**(best-effort),
彻底消除现有"共享内存不足时 load 阻塞"一类问题;
- 可选:实现 LRU→LFU 段间热点迁移(命中计数超过所在段均值时把该行重新 bump 进热段)。
建议后置——先验证整段淘汰的误杀率,TPC-C 型点查负载下热点集中,可能不需要。

### 10.5 负缓存

借鉴 OB 的 `DF_NOT_EXIST`:点查 miss 且索引/堆确认不存在时,写入一个 `size=0` 的负条目;
下次同主键点查直接返回"无行"。
**与 OB 的关键差异**:PG 的 INSERT 会让负缓存变错(OB 靠 MemTable 兜底,PG 没有),
所以 `**heap_insert` 钩子必须按新主键摘除负条目**——这是新增的失效点,成本是每 INSERT
一次哈希探测,建议做成 GUC 可开关,只对"大量查不存在键"的负载启用。

### 10.6 主键支持扩展(对齐 OB 的任意主键)

现状仅 by-val ≤32B(`CheckEligiblePkey`,`relation_row_cache.c:572-584`)。升级路径:

- **P1**:确定性 collation 的 text/varchar/char、uuid、bytea——
序列化改为 detoast 后拷贝 varlena 实际内容;短键(≤32B)仍内联,长键溢出到条目尾部
(段式布局天然支持变长条目);
- **P2**:更长复合键;
- **P3(谨慎)**:numeric(需规范化)与非确定性 collation(字节等≠逻辑等,建议维持拒绝)。

### 10.7 填充模型:load 为主,回填为辅

保留 `pg_load_relation_row_cache` 显式整表装载(适合近静态维表),
**新增按需回填**:原生路径取到行后(且行已提交、schema 稳定、非 HOT 争议行)
best-effort 写入缓存——对应 OB 的 `get_block_row` 回填三关。
使工作集大于共享内存的表也能自动收敛到热点驻留。

## 11. 实施阶段建议


| 阶段     | 内容                                   | 依赖       | 主要风险                        |
| ------ | ------------------------------------ | -------- | --------------------------- |
| S1     | 段式内存 + bump 分配 + 整段淘汰(打分先用简单 LRU 近似) | 无        | 段内死数据比例高时空间效率下降,需监控有效率      |
| S2     | seq_num 惰性失效 + DML 摘链不释放 + pin 化读路径  | S1       | pin 泄漏(异常路径必须 PG_ENSURE 释放) |
| S3     | 衰减 LFU 打分 + 后台洗段 + best-effort put   | S1       | 打分周期与 PG 后台任务框架的整合          |
| S4     | 变长主键(P1 类型)                          | S1(变长条目) | collation/detoast 正确性       |
| S5     | 负缓存 + insert 钩子                      | S2       | insert 热路径开销,GUC 开关         |
| S6(可选) | 无锁读、LRU→LFU 迁移                       | S2/S3    | 复杂度高,先量化 S2 收益再定            |


## 12. 正确性红线(不随架构改造动摇)

1. **DML 失效不可省**:UPDATE/DELETE 摘除旧 pkey 条目(可降级为摘链,不可取消)——
  缓存副本的 xmax 是拷贝时刻的快照,堆内原地打 xmax 后副本无法自证过期;
2. **只缓存已提交元组**(对应 OB 的 `!have_uncommited_row`);
3. **可见性判断 + 回退保留**,但仅作为 DML 失效的补充(处理快照差异/HOT 链),不是替代;
4. **事务状态判定必须对真实堆做**(对应 OB 的 `need_scn`_ 闸门,V4 已满足,机制分三层):
  ① 唯一性检查等 AM 内部读(SnapshotDirty)结构性够不着缓存(拦截点在执行器
   IndexNext 层)+ 探测入口 `IsMVCCSnapshot` 闸门双保险;
   ② SELECT FOR UPDATE / FOR SHARE / RI 加锁**可命中缓存**(缓存供元组+真实 TID
   做定位加速),但加锁由上层 LockRows `table_tuple_lock` 重读真实堆完成
   (打 xmax/追更新链/EPQ)——等价于 OB 的"MemTable 必读"兜底;
   ③ EPQ 重查经 ExecScanFetch 替换元组,不触缓存。
   改造中不得破坏:命中 slot 必须携带真实 TID;非 MVCC 快照探测必须拒绝;
5. **回收两不等**:洗段不等读者,put 不到内存就放弃——任何路径不得无限等待。

