#include "postgres.h"

#include "access/tableam.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/relation_row_cache.h"
#include "lib/tid_row_cache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct RelationRowCacheEntry
{
	char		status;
	Oid			relid;
	/* 每个关系独立子上下文，删除后可整体释放该关系的 tid 缓存。 */
	MemoryContext row_ctx;
	TidRowCache *tid_cache;
	int			natts;
} RelationRowCacheEntry;

#define SH_PREFIX relation_row_cache
#define SH_ELEMENT_TYPE RelationRowCacheEntry
#define SH_KEY_TYPE Oid
#define SH_KEY relid
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

static MemoryContext RelationRowCacheContext = NULL;
static relation_row_cache_hash *RelationRowCacheHash = NULL;

/* 计算一级 hash（relid）的哈希值。 */
static inline uint32
RelationRowCacheHashKey(Oid relid)
{
	return murmurhash32(relid);
}

/* 比较两个 relid 是否相等。 */
static inline bool
RelationRowCacheKeysEqual(Oid a, Oid b)
{
	return a == b;
}

#define SH_PREFIX relation_row_cache
#define SH_ELEMENT_TYPE RelationRowCacheEntry
#define SH_KEY_TYPE Oid
#define SH_KEY relid
#define SH_HASH_KEY(tb, key) RelationRowCacheHashKey(key)
#define SH_EQUAL(tb, a, b) RelationRowCacheKeysEqual(a, b)
#define SH_SCOPE static inline
#define SH_DEFINE
#include "lib/simplehash.h"

/*
 * 清理单个关系缓存条目。
 * 通过删除该关系的子上下文，一次性释放其二级 tid 缓存和所有元组副本。
 */
static void
RelationRowCacheResetEntry(RelationRowCacheEntry *entry)
{
	if (entry->row_ctx != NULL)
	{
		MemoryContextDelete(entry->row_ctx);
		entry->row_ctx = NULL;
		entry->tid_cache = NULL;
	}
	entry->natts = 0;
}

/*
 * 初始化后端级关系行缓存。
 * 创建一级 hash：relid -> RelationRowCacheEntry。
 */
void
RelationRowCacheBackendInit(void)
{
	if (RelationRowCacheHash != NULL)
		return;

	/* 后端生命周期根上下文，承载一级 hash。 */
	RelationRowCacheContext = AllocSetContextCreate(TopMemoryContext,
													"BackendRelationRowCache",
													ALLOCSET_DEFAULT_SIZES);
	RelationRowCacheHash = relation_row_cache_create(RelationRowCacheContext,
												 16,
												 NULL);
}

/*
 * 删除单个关系的缓存。
 * 先释放该关系子上下文，再从一级 hash 移除条目。
 */
void
RelationRowCacheDropRelation(Oid relid)
{
	RelationRowCacheEntry *entry;

	RelationRowCacheBackendInit();

	entry = relation_row_cache_lookup(RelationRowCacheHash, relid);
	if (entry == NULL)
		return;

	RelationRowCacheResetEntry(entry);
	relation_row_cache_delete(RelationRowCacheHash, relid);
}

/*
 * 将整张表导入缓存。
 * 若该关系已存在缓存，则先清理旧缓存，再重建二级 tid 缓存。
 */
void
RelationRowCacheLoadRelation(Relation rel)
{
	RelationRowCacheEntry *entry;
	TableScanDesc scan;
	TupleTableSlot *scan_slot;
	bool		found;
	bool		pushed_snapshot = false;

	Assert(rel != NULL);

	RelationRowCacheBackendInit();

	entry = relation_row_cache_insert(RelationRowCacheHash,
									  RelationGetRelid(rel),
									  &found);
	if (!found)
	{
		entry->row_ctx = NULL;
		entry->tid_cache = NULL;
		entry->natts = 0;
	}

	RelationRowCacheResetEntry(entry);

	/* 为该关系创建二级 tid 缓存专用子上下文。 */
	entry->row_ctx = AllocSetContextCreate(RelationRowCacheContext,
									   "RelationTidRowCache",
									   ALLOCSET_DEFAULT_SIZES);
	entry->tid_cache = TidRowCacheCreate(entry->row_ctx, 128);
	entry->natts = RelationGetDescr(rel)->natts;

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	scan_slot = table_slot_create(rel, NULL);

	/* 将整张表导入到二级 tid 缓存。 */
	while (table_scan_getnextslot(scan, ForwardScanDirection, scan_slot))
		TidRowCacheStoreFromSlot(entry->tid_cache, scan_slot, entry->row_ctx);

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(scan_slot);

	if (pushed_snapshot)
		PopActiveSnapshot();
}

/*
 * 按已有 slot 的 tableOid + tid 回填缓存内容。
 * 返回 true 表示命中并已回填，false 表示未命中或参数无效。
 */
bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	RelationRowCacheEntry *rel_entry;
	TidRowCacheEntry *tid_entry;

	Assert(slot != NULL);

	if (!OidIsValid(slot->tts_tableOid) || !ItemPointerIsValid(&slot->tts_tid))
		return false;

	RelationRowCacheBackendInit();

	/* 查询顺序：先按 relid 命中一级，再按 tid 命中二级。 */
	rel_entry = relation_row_cache_lookup(RelationRowCacheHash,
									  slot->tts_tableOid);
	if (rel_entry == NULL || rel_entry->tid_cache == NULL)
		return false;

	tid_entry = TidRowCacheLookupEntry(rel_entry->tid_cache, &slot->tts_tid);
	if (tid_entry == NULL)
		return false;

	return TidRowCacheFillSlot(tid_entry, rel_entry->relid, slot);
}
