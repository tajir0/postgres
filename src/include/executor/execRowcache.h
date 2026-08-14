#ifndef EXEC_ROWCACHE_H
#define EXEC_ROWCACHE_H

#include "nodes/execnodes.h"

/*
 * 在 ExecInitIndexScan 末尾(等到 iss_ScanKeys 构建完之后)调用:
 *   1. 清零 IndexScanState 里所有 iss_RowCache* 字段;
 *   2. 把 rd_rowcache_meta 绑定到该关系;
 *   3. 静态形态资格判定(等值键 + attno 与缓存 pkey 列逐位对位)通过时,
 *      把RelMeta 指针锁定到 iss_RowCacheMeta。不合格则 iss_RowCacheMeta 保持 NULL,后续 IndexNext 走原生路径。
 */
extern void ExecInitIndexScanRowCache(IndexScanState *node);

/*
 * 在 IndexNext 顶部调用。true 表示行缓存已决定本次结果:
 * slot 非空时返回缓存行,slot 为空时表示本轮扫描结束;
 * false 表示应继续原生索引路径。
 */
extern bool ExecIndexNextRowCache(IndexScanState *node, EState *estate,
								  TupleTableSlot *slot);

/*
 * 在 ExecReScanIndexScan 中调用:恢复 per-scan 状态,使下一轮 IndexNext
 * 可以再次尝试行缓存。
 */
extern void ExecReScanIndexScanRowCache(IndexScanState *node);

/*
 * S3:点查 miss 后按需回填。IndexNext 在原生路径取到行、recheck 通过
 * 后、返回该行之前调用;best-effort,绝不影响查询结果与延迟。
 */
extern void ExecIndexRowCacheBackfill(IndexScanState *node,
									  TupleTableSlot *slot);

#endif							/* EXEC_ROWCACHE_H */
