#ifndef RELATION_ROW_CACHE_H
#define RELATION_ROW_CACHE_H

#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/rel.h"

/* 在 TopMemoryContext 中初始化后端级全局缓存状态。 */
extern void RelationRowCacheBackendInit(void);
/* 扫描指定关系的可见元组，重建该关系的缓存。 */
extern void RelationRowCacheLoadRelation(Relation rel);
/* 使用 slot->tts_tableOid + slot->tts_tid 从缓存回填现有 slot。 */
extern bool RelationRowCacheFillSlot(TupleTableSlot *slot);
/* 删除该关系的子上下文，从而整体释放该关系缓存。 */
extern void RelationRowCacheDropRelation(Oid relid);

#endif							/* RELATION_ROW_CACHE_H */
