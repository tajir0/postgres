/*-------------------------------------------------------------------------
 *
 * execRowcache.c
 *	  执行器接入行缓存的适配层:
 *	    ExecInitIndexScanRowCache  — pkey资格判定 + 绑定 RelMeta
 *	    ExecIndexNextRowCache      — 缓存探测;命中即出 slot,否则回退
 *	    ExecReScanIndexScanRowCache — per-scan 状态恢复
 *
 *	  本文件依赖 IndexScanState / EState 等执行器结构体;关系级核心
 *	  lib/relation_row_cache.{c,h} 不依赖任何执行器细节。
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type_d.h"
#include "executor/execRowcache.h"
#include "executor/executor.h"
#include "lib/relation_row_cache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/*
 * ExecInitIndexScanRowCache
 *
 * rd_rowcache_meta 在 RelationBuildDesc 时已绑定;若该关系在缓存模块
 * attach 之前就建好(字段仍为 NULL),这里按需补绑一次(幂等)。
 * 绑定 / Drop 都会经 relcache 失效在命令边界刷新,故在此绑定不会
 * 漏掉对本查询可见的 Load。
 */
void
ExecInitIndexScanRowCache(IndexScanState *node)
{
	Relation		scanRel = node->ss.ss_currentRelation;
	struct RelMeta *rm;

	node->iss_RowCachePkeyShapeOk = false;
	node->iss_RowCachePkeyIndexNatts = 0;
	node->iss_PkeyAttempted = false;
	node->iss_RowCacheMeta = NULL;
	node->iss_RowCachePkeyNatts = 0;
	node->iss_RowCacheInvalGen = 0;
	node->iss_RowCacheBackfilled = false;

	RelationRowCacheBindRelation(scanRel);
	rm = scanRel->rd_rowcache_meta;

	if (rm != NULL && rm != ROWCACHE_NOT_CACHED &&
		node->iss_NumScanKeys >= 1 &&
		node->iss_NumScanKeys <= INDEX_MAX_KEYS &&
		node->iss_NumOrderByKeys == 0)
	{
		const int	disqualifying =
			SK_ROW_HEADER | SK_ROW_MEMBER | SK_ROW_END |
			SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL |
			SK_ORDER_BY;
		Relation	indexRel = node->iss_RelationDesc;
		bool		shape_ok = true;

		/*
		 * S2:只允许唯一索引探测缓存。schema 指纹(relfilenode +
		 * tupdesc)看不见"主键约束被删"——若之后经同列非唯一索引探测,
		 * 而堆里已插入重复键,缓存单行命中会漏读其余行。唯一性在此把关:
		 * 唯一索引 + 全列等值 ⇒ 至多一行,与缓存单行语义一致。
		 *
		 * 但 indisunique 本身不够,还必须:
		 *  - indimmediate:延迟唯一约束(DEFERRABLE)允许事务内暂时
		 *    重复,此时同键两行都可见,缓存单行命中会漏行;
		 *  - 非部分索引:planner 会把被索引谓词蕴含的 qual 从 filter
		 *    中消除,缓存命中绕过索引后无人复查谓词,可能返回谓词外
		 *    的行。indpred 是变长字段,经 rd_indextuple 零分配判空。
		 */
		if (!indexRel->rd_index->indisunique)
			shape_ok = false;
		if (!indexRel->rd_index->indimmediate)
			shape_ok = false;
		if (!heap_attisnull(indexRel->rd_indextuple, Anum_pg_index_indpred, NULL))
			shape_ok = false;
		if (indexRel->rd_index->indnkeyatts != node->iss_NumScanKeys)
			shape_ok = false;

		for (int i = 0; shape_ok && i < node->iss_NumScanKeys; i++)
		{
			ScanKey		sk = &node->iss_ScanKeys[i];
			AttrNumber	heap_attno;
			Oid			heap_type;

			if (sk->sk_strategy != BTEqualStrategyNumber ||
				(sk->sk_flags & disqualifying) != 0 ||
				sk->sk_attno != i + 1)
			{
				shape_ok = false;
				break;
			}

			heap_attno = indexRel->rd_index->indkey.values[i];
			if (heap_attno <= 0)
			{
				shape_ok = false;
				break;
			}

			heap_type = TupleDescAttr(RelationGetDescr(scanRel),
									  heap_attno - 1)->atttypid;

			/*
			 * 跨类型 ScanKey 仍回退,但以索引 opclass 的声明输入类型为
			 * 基准。varchar 默认使用 text opclass,二者 Datum 表示相同;
			 * int4 列 = int8 参数则不匹配,避免按错误物理宽度编码。
			 */
			if (sk->sk_subtype != InvalidOid &&
				sk->sk_subtype != indexRel->rd_opcintype[i])
			{
				shape_ok = false;
				break;
			}

			/* S4 字节等值只适用于确定性 collation。 */
			if (heap_type == TEXTOID || heap_type == VARCHAROID ||
				heap_type == BPCHAROID)
			{
				if (!OidIsValid(sk->sk_collation) ||
					!get_collation_isdeterministic(sk->sk_collation))
				{
					shape_ok = false;
					break;
				}
			}

			node->iss_RowCachePkeyIndexHeapAttnos[i] = heap_attno;
		}

		if (shape_ok)
		{
			node->iss_RowCachePkeyShapeOk = true;
			node->iss_RowCachePkeyIndexNatts = node->iss_NumScanKeys;

			/*
			 * shape 合格,再校验 cache 的 pkey 列与本索引键列逐列
			 * 对位匹配;匹配则锁定活 RelMeta 指针,IndexNext 即可
			 * 经 backend 本地桶头指针探测,全程不扫 RelMeta 数组。
			 */
			if (scanRel->rd_rowcache_pkey_n == node->iss_NumScanKeys)
			{
				bool		attnos_match = true;

				for (int i = 0; i < scanRel->rd_rowcache_pkey_n; i++)
				{
					if (scanRel->rd_rowcache_pkey_descs[i].attno !=
						node->iss_RowCachePkeyIndexHeapAttnos[i])
					{
						attnos_match = false;
						break;
					}
				}

				if (attnos_match)
				{
					node->iss_RowCacheMeta = rm;
					node->iss_RowCachePkeyNatts = scanRel->rd_rowcache_pkey_n;
				}
			}
		}
	}
}


/*
 * ExecIndexNextRowCache
 *
 * 行缓存 pkey 路径。
 *
 */
RowCacheProbeResult
ExecIndexNextRowCache(IndexScanState *node, EState *estate,
					  TupleTableSlot *slot)
{
	Datum		vals[INDEX_MAX_KEYS];
	int			natts;
	bool		visible = false;
	bool		has_hot_chain = false;
	bool		hit;

	if (node->iss_RowCacheMeta == NULL || node->iss_PkeyAttempted)
		return ROW_CACHE_FALLBACK;

	node->iss_PkeyAttempted = true;

	/*
	 * 回填竞态屏障 c1(S3):必须在原生路径读堆之前记下本表失效代数。
	 * 之后若走到回填,插入前比对 c2——期间本表有任何 DML 即放弃回填。
	 */
	node->iss_RowCacheInvalGen =
		RelationRowCacheInvalGen(node->iss_RowCacheMeta);

	/* 所有运行期 ScanKey 必须就绪(NestLoop 内层扫描)。 */
	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		return ROW_CACHE_FALLBACK;

	/*
	 * 从等值 ScanKey 收集每个缓存 pkey 列一个 Datum。attno 匹配已在
	 * ExecInit 校验过一次,所以这里只拒绝 null 搜索键。
	 */
	natts = node->iss_RowCachePkeyNatts;
	for (int i = 0; i < natts; i++)
	{
		ScanKey		sk = &node->iss_ScanKeys[i];

		if ((sk->sk_flags & SK_ISNULL) != 0)
			return ROW_CACHE_FALLBACK;
		vals[i] = sk->sk_argument;
	}

	hit = RelationRowCachePkeyFetchBound(node->iss_RowCacheMeta,
										 RelationGetRelid(node->ss.ss_currentRelation),
										 node->ss.ss_currentRelation->rd_rowcache_pkey_descs,
										 vals, natts,
										 estate->es_snapshot,
										 slot, &visible,
										 &has_hot_chain);

	if (hit && visible && !has_hot_chain)
	{
		node->iss_ReachedEnd = true;
		return ROW_CACHE_TUPLE_RETURNED;
	}

	/*
	 * miss,或缓存里那个 tuple 对我们的 snapshot 不可见、或处于一条
	 * HOT 链的链头。回退 B-树,走 HOT 链并对着堆重新检查可见性。
	 */
	return ROW_CACHE_FALLBACK;
}


void
ExecReScanIndexScanRowCache(IndexScanState *node)
{
	node->iss_PkeyAttempted = false;
	node->iss_RowCacheBackfilled = false;
}

/*
 * ExecIndexRowCacheBackfill
 *
 * 点查 miss 后按需回填(S3):原生路径取到行、recheck 通过后,由
 * IndexNext 在返回该行之前调用。缓存层从真实返回行重新提取主键,
 * 再做行状态三关检查、竞态屏障比对与插入。best-effort:任何环节不
 * 满足直接放弃,不影响查询结果与延迟。
 */
void
ExecIndexRowCacheBackfill(IndexScanState *node, TupleTableSlot *slot)
{
	if (node->iss_RowCacheMeta == NULL ||
		!node->iss_PkeyAttempted ||
		node->iss_RowCacheBackfilled)
		return;

	node->iss_RowCacheBackfilled = true;	/* 每个扫描实例至多一次 */

	if (node->iss_NumRuntimeKeys != 0 && !node->iss_RuntimeKeysReady)
		return;

	(void) RelationRowCacheBackfillBound(node->iss_RowCacheMeta,
										 RelationGetRelid(node->ss.ss_currentRelation),
										 slot,
										 node->iss_RowCacheInvalGen);
}
