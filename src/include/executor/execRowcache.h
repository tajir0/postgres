#ifndef EXEC_ROWCACHE_H
#define EXEC_ROWCACHE_H

#include "nodes/execnodes.h"

/*
 * ExecIndexNextRowCache 的返回结果。
 *
 *   ROW_CACHE_TUPLE_RETURNED:缓存命中 + 对当前快照可见 + 非 HOT 链头;
 *     slot 已填,函数已置 iss_ReachedEnd = true(模拟唯一索引等值)。
 *     调用方应直接返回该 slot 作为本次 IndexNext 的输出。
 *
 *   ROW_CACHE_FALLBACK:不走缓存(扫描未武装 / 已尝试过 / 运行期键未
 *     就绪 / NULL 搜索键 / 未命中 / 不可见 / 可能 HOT 链)。调用方按
 *     原生 B-树索引扫描路径继续。
 */
typedef enum
{
	ROW_CACHE_TUPLE_RETURNED,
	ROW_CACHE_FALLBACK,
} RowCacheProbeResult;

/*
 * 在 ExecInitIndexScan 末尾(等到 iss_ScanKeys 构建完之后)调用:
 *   1. 清零 IndexScanState 里所有 iss_RowCache* / iss_PkeyAttempted 字段;
 *   2. 把 rd_rowcache_meta 绑定到该关系;
 *   3. 静态形态资格判定(等值键 + attno 与缓存 pkey 列逐位对位)通过时,
 *      把RelMeta 指针锁定到 iss_RowCacheMeta。不合格则 iss_RowCacheMeta 保持 NULL,后续 IndexNext 走原生路径。
 */
extern void ExecInitIndexScanRowCache(IndexScanState *node);

/*
 * 在 IndexNext 顶部调用,尝试用行缓存。
 */
extern RowCacheProbeResult ExecIndexNextRowCache(IndexScanState *node,
												 EState *estate,
												 TupleTableSlot *slot);

/*
 * 在 ExecReScanIndexScan 中调用:恢复 per-scan 状态,使下一轮 IndexNext
 * 可以再次尝试行缓存。
 */
extern void ExecReScanIndexScanRowCache(IndexScanState *node);

#endif							/* EXEC_ROWCACHE_H */
