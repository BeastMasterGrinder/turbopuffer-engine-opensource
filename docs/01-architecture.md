# 01 — Architecture: object storage as the source of truth

## The core bet

Incumbent search engines (Elasticsearch, classic vector DBs) store data on **RAM + triply-replicated
SSD**, because they inherited architectures built for low-latency *transactional* updates. turbopuffer's
insight: **search is not a transactional database.** Its write workload looks like a **data warehouse**
(high throughput, no transactions, relaxed write latency) — the one thing it needs that a warehouse
doesn't is **reads under ~100 ms**.

| Tier | Tech | Read | Write | Storage | $/GB |
|---|---|---|---|---|---|
| Caching | Redis | <500µs | <500µs | Memory | — |
| Relational | Postgres | <1ms | <1ms | Mem + replicated SSD | — |
| **Search** | ES / vector DBs | **<100ms** | <1s | Mem + replicated SSD | ~$0.6–2 |
| Warehouse | BigQuery | >1s | >1s | **Object storage** | ~$0.02 |

So turbopuffer pairs **warehouse-style storage (S3/GCS, ~$0.02/GB)** with **search-style read latency
(caching + a smart index)**. Object storage is ~100× cheaper than RAM for cold data. Embeddings are
20–30× larger than the source text and don't compress well, which makes RAM-resident vector search
economically brutal at scale — and makes S3 the obvious home.

This is **not tiering** (cold data lazily flushed to S3). It is an **object-storage-first storage
engine**, structured as an **LSM tree where object storage *is* the source of truth.**

> Source: turbopuffer founding blog, architecture & concepts pages.

## Namespaces = a prefix on object storage

A namespace is just a key prefix, e.g. `s3://bucket/{namespace}/`, containing a `wal/` directory and an
`index/` directory. Namespaces are **fully isolated** — separate WAL, separate indexes, separate prefix.
That isolation is exactly what makes **cheap, near-unlimited multi-tenancy** possible (one namespace per
user/tenant; customers run millions of them).

```
s3://bucket/{namespace}/
├── manifest.json          # the CAS-coordinated pointer — the "source of truth" head
├── wal/00000000000000000000.json    # committed write segments (the LSM log)
├── wal/00000000000000000001.json
└── index/v{epoch}/        # the built index for a given epoch
    ├── centroids.json     # centroid vectors + which cluster file holds each cluster
    ├── cluster-0.json     # vectors (+ binary codes) for cluster 0
    ├── bm25.json          # inverted index for full-text
    └── docs.json          # id -> attributes (filtering + return payloads)
```

## Coordination: CAS on a JSON file (no Raft, no Kafka, no Zookeeper)

This is the part people find surprising. **The only critical-path dependency is object storage.** There
is no external consensus system. Coordination — indexing-job dispatch, metadata updates, "leader"
election — is done by **atomically updating a single JSON object via compare-and-swap (CAS)**.

- S3 became **strongly consistent** in Dec 2020 and added **conditional writes** (CAS) afterward.
- A conditional `PUT` with `If-Match: <etag>` succeeds only if the object's current ETag matches;
  otherwise S3 returns **HTTP 412 Precondition Failed**. That is exactly `UPDATE ... WHERE version = N`.
- Every node stays **stateless**; all concurrency control is delegated to object storage.

**MinIO (what this clone uses) supports both `If-Match` and `If-None-Match`** — even better than AWS S3,
which only supports `If-None-Match` on PutObject. So our clone implements the real CAS model faithfully.

The CAS retry loop is the whole coordination story:

```
load manifest (GET → body + ETag)
mutate manifest in memory
PutObject(manifest, If-Match: ETag)
  ├─ 200 → success
  └─ 412 → someone else won the race; reload and retry
```

## The three-tier cache (hiding S3's latency)

Data migrates *up* the tiers as it's queried, so most queries never touch S3:

| Tier | Latency | Holds |
|---|---|---|
| DRAM | ~1ms | Hottest data — centroids, active namespaces |
| NVMe SSD | ~5–10ms | Warm namespaces (a ring-buffer file, ~200 lines, no LRU) |
| S3 (cold) | ~50–500ms | Everything (the source of truth) |

Crucially, turbopuffer does **not** load a whole namespace into cache before serving a query — it does
**small ranged reads directly against object storage**, so even a cold namespace answers in well under a
second. A consistent-hashing load balancer routes a namespace to the same node to keep caches warm, but
**any node can serve any namespace** (because the state is all on S3).

> **Our clone** implements the **DRAM tier** (an in-memory map over S3) and the **S3 tier** (MinIO). The
> NVMe ring-buffer tier is documented but omitted — see [`05-clone-mapping.md`](./05-clone-mapping.md).

## The write path (~100–250ms, dominated by the S3 PUT)

1. Load balancer hashes the namespace → a query node.
2. **Group commit:** concurrent writes to a namespace are buffered (turbopuffer commits **1 WAL entry
   per second per namespace**; concurrent writes in that window batch into one entry). This decouples
   throughput from S3's per-write latency.
3. **WAL commit:** append a new file under `wal/`, update `manifest.json` via CAS, get the S3 ack,
   return `200`. **A successful return means the data is durably persisted on object storage.**
4. **Cache update:** write into local DRAM (and the NVMe ring buffer in real tpuf).
5. **Queue indexing** if the WAL has run far enough ahead of the index.

A WAL entry passes through three states: *written-but-uncommitted → committed-but-unindexed →
indexed-and-committed*. Reported: **p50 ≈ 165 ms for a 500 kB write**, **~10,000+ vectors/sec** per
namespace. This is deliberately ~200× slower than a Postgres write — the trade that buys warehouse-class
throughput and S3-backed durability.

## The indexing path (asynchronous, compute/query separated)

After data is committed it is indexed **asynchronously, on separate nodes** — so heavy indexing never
competes with query latency ("compute-compute separation"). **Until data is indexed it is still
searchable** via an exhaustive scan of the recent (unindexed) WAL.

In real turbopuffer the indexing queue itself lives on object storage: a query node tells a **broker**
it needs reindexing; the broker group-commits all such requests into one CAS write to `queue.json`; an
**indexer** claims a job by marking it in-progress (the loser's CAS just fails and retries). Indexers
write three index types:

- **ANN index** (centroid/SPFresh) for vectors — see [`02`](./02-spfresh-spann-index.md)
- **Inverted BM25 index** for full-text — see [`04`](./04-bm25-fulltext.md)
- **Exact / bitmap indexes** for metadata filtering

A reindex is triggered once the unindexed WAL grows past **a configurable byte threshold** (turbopuffer
does *not* publish a literal default — the often-quoted "128 MiB" is actually the L3-cached upper-tree
size, a different thing).

> **Our clone** runs indexing as an explicit `tpuf index` command (not a live broker/indexer process),
> but uses the **same manifest-CAS epoch swap** to publish a new index atomically.

## The query path (one endpoint, mode chosen by `RankBy`)

A single `Query` endpoint serves both vector and full-text search; the retrieval mode is selected by
`RankBy` (a vector → ANN; a text query → BM25). On every query:

1. Check the manifest for the live index **epoch** and the **unindexed WAL tail**.
2. Run the chosen retrieval over the **indexed** data (centroid scan, or BM25 over the inverted index).
3. **Exhaustively scan the unindexed WAL tail** so brand-new writes are included.
4. Apply **metadata filters**, merge, sort, return top-K (with a `$dist`/score per row).

**Strong consistency by default:** because a query usually routes to the writing node, a write is
immediately visible — over 99.8% of queries return consistent data. On a read the node checks the
manifest (`GET ... If-None-Match`; a `304` means the index is current, a `200` means replay the WAL
delta) — about an 8 ms "consistency tax" that can be disabled for sub-10 ms warm reads (trading
freshness; worst-case staleness up to ~1 hour under eventual consistency).

## What turbopuffer provides (and deliberately doesn't)

- **A**tomicity, **C**onsistency, **D**urability — yes.
- General **I**solation (read-write transactions) — intentionally **not** (it's a search engine, not an
  OLTP DB). Limited transactional semantics exist (atomic conditional writes via S3 CAS, atomic batches).
- **CAP stance:** prioritizes **consistency over availability** when object storage is unreachable;
  adjustable toward availability per query.

---

Next: [`02-spfresh-spann-index.md`](./02-spfresh-spann-index.md) — the centroid vector index in detail.
