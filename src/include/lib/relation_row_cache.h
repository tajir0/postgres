#ifndef RELATION_ROW_CACHE_H
#define RELATION_ROW_CACHE_H

#include "postgres.h"

#include "access/htup.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/*
 * GUC:行缓存哈希桶数(PGC_POSTMASTER,改需重启,必须是 2 的幂)。
 * 桶头数组首次用缓存时在 DSA 里一次性分配;实际生效值固化进 RowCacheControl。
 */
#define ROW_CACHE_DEFAULT_HASH_BUCKETS	(1 << 23)	/* 8,388,608 */
#define ROW_CACHE_MIN_HASH_BUCKETS		(1 << 10)	/* 1,024 */
#define ROW_CACHE_MAX_HASH_BUCKETS		(1 << 24)	/* 134,217,728 */
extern PGDLLIMPORT int row_cache_hash_buckets;

/* 共享内存大小与初始化(由 ipci.c 调用)。 */
extern Size RowCacheShmemSize(void);
extern void RowCacheShmemInit(void);

extern void RowCacheOnHeapUpdate(Relation rel,
								 HeapTuple oldtup,
								 HeapTuple newtup);
extern void RowCacheOnHeapDelete(Relation rel,
								 HeapTuple oldtup);

/* 加载 / 卸载一张关系的缓存。 */
extern void RelationRowCacheLoadRelation(Relation rel);
extern void RelationRowCacheDropRelation(Oid relid);

/*
 *
 * RelationRowCachePkeyFetch       — 单列 pkey 关系。
 * RelationRowCachePkeyFetchComposite — 复合 pkey 关系。
 */
extern bool RelationRowCachePkeyFetch(Oid relid,
									  Datum pkey_val,
									  Snapshot snapshot,
									  TupleTableSlot *slot,
									  bool *is_visible,
									  bool *has_hot_chain);
extern bool RelationRowCachePkeyFetchComposite(Oid relid,
											   const Datum *vals,
											   int nvals,
											   Snapshot snapshot,
											   TupleTableSlot *slot,
											   bool *is_visible,
											   bool *has_hot_chain);


extern AttrNumber RelationRowCachePkeyAttno(Oid relid);

#define ROWCACHE_NOT_CACHED		((struct RelMeta *) 0x1)

extern void RelationRowCacheBindRelation(Relation rel);

extern bool RelationRowCachePkeyFetchBound(struct RelMeta *rm,
										   Oid expected_relid,
										   const Datum *vals,
										   int nvals,
										   Snapshot snapshot,
										   TupleTableSlot *slot,
										   bool *is_visible,
										   bool *has_hot_chain);

extern int RelationRowCachePkeyDescriptor(Oid relid,
										   AttrNumber *out_attnos,
										   int max_attnos);


#endif							/* RELATION_ROW_CACHE_H */
