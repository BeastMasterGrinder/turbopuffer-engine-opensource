# turbopuffer: Architecture, Goals, and How It Works at Scale

*A technical overview compiled from turbopuffer's documentation, engineering blog, customer case studies, and third-party analysis. Reference links are listed at the end.*

---

## 1. What turbopuffer Is

turbopuffer is a vector and full-text search engine built from first principles on **object storage** (Amazon S3, Google Cloud Storage) rather than on RAM or replicated SSDs. Its tagline is "search every byte," and its pitch is search that is fast, roughly 10x cheaper than incumbents, and extremely scalable.

A single query can combine several retrieval modes:

- **Vector search** — approximate nearest neighbor (ANN) over embeddings
- **Full-text search** — BM25 ranking over an inverted index
- **Hybrid search** — vector + full-text fused and re-ranked
- **Metadata filtering** — exact filters via bitmap indexes
- **Regex / trigram search** — used for code search
- **Aggregations** — counts and groupings

Reported production scale: **4T+ documents (15PB+)**, **10M+ writes/sec**, and **25k+ queries/sec**. A third-party analysis reports turbopuffer serving **2.5+ trillion vectors** across **200M+ namespaces** for customers including Cursor, Notion, and Anthropic.

---

## 2. The Goal: Why It Exists

### The origin story

In late 2022, co-founder and CEO Simon Hørup Eskildsen (formerly a principal engineer at Shopify) was consulting with the team at **Readwise** on adding semantic search across 100M+ documents. On the vector databases of the day, that feature would have cost roughly **$20k/month** versus about **$5k/month** for their existing relational database — so the feature was shelved. Eskildsen watched a good team decide *not* to ship features purely because production search was too expensive. That experience inspired him to start building turbopuffer in 2023.

### The core thesis: cost is the problem

turbopuffer's central argument is that search prices are not a law of physics — incumbent engines simply weren't built to harness modern hardware and cloud services. Three tailwinds changed the equation:

1. **Object storage is now ubiquitous** (and, since Dec 2020, strongly consistent).
2. **NVMe SSDs are fast and cheap**, with massive parallel I/O.
3. **AI and vectors demand far more from search systems** than ever before.

The storage-cost math is the heart of the design:

| Storage tier | Approx. cost | Notes |
|---|---|---|
| In-memory (2022-era vector DBs) | $2+/GB | Most expensive |
| Triply-replicated SSD @ 50% util | ~$0.6/GB | Incumbent search/DB tier |
| SSD cache for hot data | ~$0.1/GB | Warm tier |
| Object storage (S3/GCS) | ~$0.02/GB | Source of truth |

Object storage is up to **~100x cheaper than memory** for cold data and **6–20x cheaper** for warm data. On a per-TB/month basis, incumbent "RAM + 3x SSD" setups run ~$3,600 while turbopuffer's "S3 + SSD cache" runs ~$70 — with raw S3 at ~$20.

A separate factor makes this acute for AI: **embeddings are 20–30x larger than the source text** and don't compress well (a ~5KB article can become ~24KB of vectors). Storing that in RAM at scale is economically painful; storing it on S3 is not.

### Mission

"Make it possible for our customers to **search every byte they have**." The company invokes the Jevons paradox: when search becomes cheap enough, people search far more, indexing data they previously couldn't justify storing in an index.

---

## 3. The Key Design Insight: Search Is Not a Transactional Database

turbopuffer frames search as one of "five common databases" companies extract from their primary store (caching, relational, search, warehouse, streaming). Its insight: **search's write workload looks like a data warehouse** (high throughput, no transactions, relaxed write latency) — but unlike a warehouse, **search reads must finish in under ~100ms**.

| Category | Tech | Read latency | Write latency | Storage |
|---|---|---|---|---|
| Caching | Redis / Memcached | <500µs | <500µs | Memory |
| Relational | MySQL / Postgres | <1ms | <1ms | Memory + replicated SSD |
| Search | Elasticsearch / Vector DBs | <100ms | <1s | Memory + replicated SSD |
| Warehouse | BigQuery / Snowflake | >1s | >1s | Object storage |
| Streaming | Kafka / Warpstream | <100ms | <100ms | Replicated HDD or object storage |

Incumbent search engines borrowed replicated-disk architectures built for low-latency, high-concurrency transactional updates — so users overpay for performance characteristics search doesn't actually need. turbopuffer instead pairs **warehouse-style storage (object storage)** with **search-style read latency (caching + a smart index)**.

---

## 4. Architecture

### Foundation: object storage as the single source of truth

The API routes to a cluster of **Rust binaries** (`./tpuf`) that read and write the customer's data on object storage. This is **not tiering** (cold data lazily flushed to S3) — it is an **object-storage-first storage engine**, structured as an LSM where object storage *is* the source of truth. As the team puts it, the rules are different on object storage: any node can compact data, latency is high but throughput is phenomenal, individual writes are expensive, and storage is cheap.

Each **namespace** is simply a prefix on object storage (e.g. `s3://tpuf/{namespace_id}/`), containing a `/wal` directory and an `/index` directory. Namespaces are fully isolated — separate WAL, separate indexes, separate prefix — which is what makes cheap, near-unlimited multi-tenancy possible.

### Node types and the coordination layer

- **Query nodes** — handle reads and writes; stateless; hold a local DRAM + NVMe cache.
- **Indexer nodes** — build indexes in the background; CPU-heavy; autoscale independently.
- **Broker** — batches coordination/queue operations; stateless; one active at a time.
- **Object storage (S3/GCS)** — the only persistent state: WAL, indexes, metadata, and the coordination queue.

A defining property: **the only critical-path dependency is object storage.** There is no Raft, Paxos, Zookeeper, or Kafka. Coordination is done through a single JSON file on S3, updated atomically via **compare-and-swap (CAS)** — S3 added conditional `If-Match` PUTs (returning `412` on conflict) in Nov 2024, which is equivalent to `UPDATE ... WHERE version = N`. This lets every node stay stateless and delegates all concurrency control to object storage. The result is **99.99% uptime since launch**, because high availability is just "how many query nodes we run."

### Three-tier cache

Data migrates up the tiers as it's queried, so most queries never touch S3:

| Tier | Latency | Holds |
|---|---|---|
| DRAM | ~1ms | Hottest data — centroids, active namespaces |
| NVMe SSD | ~5–10ms | Warm namespaces |
| S3 (cold) | ~50–500ms | Everything (source of truth) |

The NVMe cache is implemented as a **ring buffer** — a single file the size of the disk that writes sequentially and wraps around, skipping frequently-accessed data. It's about 200 lines of code, with no LRU or eviction heuristics. Crucially, turbopuffer does **not** load an entire namespace into cache before serving a query; the engine performs **small ranged reads directly against object storage**, so even a cold namespace answers quickly. The load balancer uses **consistent hashing on the namespace** so the same namespace routes to the same node and keeps caches warm — but any node can serve any namespace.

### Write path (~100–250ms, dominated by the S3 PUT)

1. Load balancer hashes the namespace → query node.
2. **Group commit:** concurrent writes are buffered for ~100–250ms (one WAL commit per second per namespace).
3. **WAL commit:** append a new file to the namespace's `/wal` prefix, update the manifest via CAS, get the S3 acknowledgment, then return `200`. A successful return means the data is **durably persisted on object storage**.
4. **Cache update:** write into local DRAM and the NVMe ring buffer.
5. **Queue indexing** if the WAL has run far enough ahead of the index.

This is deliberately ~200x slower than a Postgres write (p50 ≈ 165ms for a 500kB write) — the tradeoff that buys warehouse-style throughput (10,000+ vectors/sec per namespace) and S3-backed durability. WAL entries pass through three states: written-but-uncommitted, committed-but-unindexed, and indexed-and-committed.

### Indexing path (asynchronous, CAS-coordinated)

After data is committed, it's indexed asynchronously on separate indexer nodes — **compute-compute separation** means heavy indexing never competes with query latency. Until data is indexed it's **still searchable** via an exhaustive scan of the recent (unindexed) WAL.

The indexing queue lives on object storage. A query node tells the broker it needs re-indexing; the broker group-commits all such requests into one CAS write to `queue.json`; an indexer claims a job by marking it in-progress (the loser's CAS simply fails and retries). Indexers write three index types into the `/index` prefix:

- **ANN index** for vectors
- **Inverted BM25 index** for full-text
- **Exact/bitmap indexes** for metadata filtering

Fault tolerance is built in: a dead broker is replaced via CAS in seconds; a dead indexer is detected via stale heartbeat and its job is reassigned (at-least-once delivery).

### The vector index: centroids, not graphs

turbopuffer's vector index is **centroid-based**, derived from **SPFresh**, rather than a graph index like **HNSW** or **DiskANN**. This choice is dictated by object storage.

HNSW — the approach used by Pinecone, Weaviate, Qdrant, and Milvus — requires walking a graph of **10–20 sequential, dependent hops**, each one waiting on the previous. Each hop costs:

- ~300–1,800µs in RAM (fine)
- ~1–10ms on NVMe (workable)
- ~50–100ms on S3 → **1–2 seconds per query (unusable)**

A centroid index reduces this to roughly **two dependent steps instead of twenty**: find the nearest centroids, then fetch the matching clusters. The on-disk layout per namespace holds a `centroids.bin` (a fast index of cluster centers) plus `clusters-N.bin` files.

A **cold vector query** is engineered around the cost of S3 roundtrips (each ~100ms+ from first principles, p90 ~250ms for <1MB), targeting a **maximum of three roundtrips** to stay under a second:

1. Download metadata for the storage engine.
2. Scan ~10,000 centroids (~60MB, kept in DRAM for active namespaces) using SIMD/AVX-512 dot products; pick the **top ~3 closest clusters**; consult filter indexes and the unindexed WAL.
3. Fetch those cluster files (~6MB each) in parallel.

Within a cluster, candidate vectors are scored using **RaBitQ** (binary quantization with provable error bounds): a fast binary scan (XOR + popcount, ~10ns/pair) narrows ~3,000 candidates to ~50 in microseconds, then a full-precision rerank scores only those ~50. Metadata filters use **bitmap indexes** (one bitwise AND filters 512 docs per CPU instruction), and a query planner chooses filter-first vs. search-first based on selectivity.

---

## 5. Consistency, Durability, and Guarantees

turbopuffer provides **Atomicity, Consistency, and Durability**. General **Isolation** (read-write transactions) is intentionally not offered, though limited transactional semantics exist.

- **Durable writes** — committed to object storage before the API returns success.
- **Strong consistency by default** — a write is immediately visible to the next query because queries usually route to the writing node. **Over 99.8% of queries return consistent data.** On a read, the node checks the manifest (`GET manifest.json` with `If-None-Match`; a `304` means the index is current, a `200` means replay the WAL delta) — about an 8ms "consistency tax" that can be disabled.
- **Eventual consistency (optional)** — configurable per query for sub-10ms warm latency, trading freshness. Worst-case staleness is up to ~1 hour; specifically, once a namespace has **>128MiB of outstanding unindexed writes**, further writes aren't visible until indexed and cached.
- **Atomic conditional writes** — evaluated atomically with the write (Serializable behavior; prevents lost updates), built on S3 CAS.
- **Atomic batches** — all writes in an upsert apply together.
- **`patch_by_filter` / `delete_by_filter`** — two-phase, behaving like Read Committed isolation (analogous to Postgres `UPDATE ... WHERE`).
- **Branch isolation** — namespaces can be branched as instant, copy-on-write, point-in-time snapshots that are fully independent thereafter.
- **CAP stance** — prioritizes consistency over availability when object storage is unreachable; adjustable toward availability per query.

---

## 6. Performance at Scale

**Latency benchmarks**

| Workload | Cold (p90/p50) | Warm (p90/p50) |
|---|---|---|
| Vector, 1M × 768-dim (~3GB) | 444ms p90 | 10ms p90 |
| Full-text BM25, 1M docs (~300MB) | 285ms p90 | 18ms p90 |
| 1M documents (architecture docs) | 874ms p50 | 14ms p50 |
| End-to-end vector query (analysis) | ~500ms | ~10ms (15–20ms on NVMe) |

**Reported production limits**

| Dimension | Observed | Stated limit |
|---|---|---|
| Documents (global) | 4T+ @ 15PB+ | Unlimited |
| Documents per namespace | 500M+ @ 2TB | 500M @ 2TB |
| Documents queried simultaneously | 100B+ @ 10TB | — |
| Namespaces | 250M+ | Unlimited |
| Write throughput (global) | 10M+ writes/s @ 32GB/s | — |
| Write throughput (per namespace) | 32k+ writes/s @ 64MB/s | 10k writes/s @ 32MB/s |
| Queries (global / per namespace) | 25k+ /s / 1k+ /s | — |
| Vector recall@10 | 90–100% | — |

To enforce fairness across 200M+ tenants — which the Linux kernel knows nothing about — turbopuffer runs a **user-space scheduler** over every resource (CPU, memory, NVMe, network, S3 operations) per tenant.

**Sweet spot:** Millions of vectors → Postgres + pgvector is fine. Tens of millions → a transition zone. **Hundreds of millions to billions → turbopuffer's home turf.** It's a poor fit for sub-millisecond writes (an S3 PUT is ~100ms minimum), full ACID transactions, or very small datasets.

---

## 7. How It Performs in Production: Customer Evidence

The architecture's payoff shows up consistently as **massive multi-tenancy (a namespace per user/tenant) at low cost**, with warm latency competitive with RAM-based systems.

**Notion** — Largest disclosed scale: **10B+ vectors**, **1M+ namespaces**, **1GB/s** and **100,000+ writes/s** peaks, targeting 100TB+. Migrated October 2024. Reported **80% cost reduction** — enough to **remove per-user AI charges** — with ≥99.99% uptime and zero performance drops. *"turbopuffer makes it too easy to build state-of-the-art AI apps."*

**TELUS** — **25,000+ namespaces** (one personalized RAG copilot each) serving **57,000+ employees** via its Fuel iX engine, at **<100ms p99**. A distinguished engineer had burned through five vector databases before turbopuffer; migration took **under 30 minutes with zero downtime**. Reported ~**1000x cheaper** than alternatives and **$90M+ in benefits**. *"With turbopuffer, there's no scaling wall. It's infinite."*

**Linear** — **2B+ documents** across **4M+ namespaces** (modeled per `org_id`/table) at **13ms p50**, replacing **Elasticsearch + pgvector** for a **70% cost cut**. Uses hybrid search with Cohere embeddings + rerank, and per-namespace customer-managed encryption keys (CMEK).

**Superhuman** — **9B+ emails** across hundreds of thousands of inboxes at **60ms p90**, **97%+ recall@10**. Their previous vector DB throttled morning email ingestion (search lag once exceeded 24 hours) and capped active tenants at 10k. turbopuffer's write/query separation and unlimited namespaces let them index **5 years of email instead of 1** and migrate **3B+ embeddings in a single day without re-embedding** (saving $300k+). *"I will always pick a product with great reliability over a product with great features. turbopuffer has both."*

**Pylon** — Semantic search over support tickets for B2B customers; replaced **OpenSearch** (where cost scaled linearly per tenant) and reached **production in 3 days** — fast enough that they wrote their own Go client before the official one existed. Header figures: 50k+ namespaces, 600M+ documents at 24ms p90.

**Readwise** — The origin customer: **1TB+ of data**, **50k+ namespaces**, **150M+ documents**, implemented in under a month, powering "Chat with Your Library." Its 2022 cost-vs-feature dilemma is literally what inspired turbopuffer.

**Cursor** — The first large customer (Nov 2023): migrated billions of vectors across millions of codebases in days, cut costs ~95%, and improved eval performance up to 23.5%.

A recurring theme across customers is the **"index everything" mindset shift**: once storage and indexing are cheap, teams stop rationing what they index and stop killing features over infrastructure cost — exactly the problem that inspired the company.

---

## 8. Developer Experience

turbopuffer exposes a simple HTTP API with official SDKs (the Go client is generated with Stainless, so other language clients share its shape). Operations hang off a **`Namespace`** handle, and a single **`Query`** endpoint serves both vector and full-text search — the mode is selected by `RankBy`:

```go
tpuf := turbopuffer.NewClient(
    option.WithAPIKey(os.Getenv("TURBOPUFFER_API_KEY")),
    option.WithRegion("gcp-us-central1"),
)
ns := tpuf.Namespace("example")

// Vector search with filters
res, _ := ns.Query(ctx, turbopuffer.NamespaceQueryParams{
    RankBy:  turbopuffer.NewRankByVector("vector", []float32{0.1, 0.2}),
    TopK:    turbopuffer.Int(10),
    Filters: turbopuffer.NewFilterAnd([]turbopuffer.Filter{
        turbopuffer.NewFilterEq("name", "foo"),
        turbopuffer.NewFilterEq("public", 1),
    }),
})

// Full-text BM25 — same endpoint, different RankBy
res, _ = ns.Query(ctx, turbopuffer.NamespaceQueryParams{
    TopK:   turbopuffer.Int(10),
    RankBy: turbopuffer.NewRankByTextBM25("text", "quick walrus"),
})
```

Filters are composable (`NewFilterAnd`, `NewFilterEq`), results carry a `$dist` score, regions are explicit (multi-cloud, including GCP), and the client auto-retries on `409 Conflict` — consistent with the CAS-on-S3 coordination model. The company is remote-first, runs a community Slack, publishes a public status page, and offers a hosted MCP server for AI assistants.

---

## 9. Summary

turbopuffer rethinks search around a single bet: **object storage is cheap, durable, and now strongly consistent — so make it the source of truth, and engineer everything else around hiding its latency.** A centroid-based ANN index (two dependent steps, not HNSW's twenty), a three-tier DRAM/NVMe/S3 cache, CAS-based coordination through a JSON file on S3 (no Raft or Kafka), and separation of indexing from querying together deliver warehouse-class storage economics with search-class read latency. The payoff is order-of-magnitude cost reductions and effectively unlimited multi-tenancy, which is why companies like Notion, Linear, Superhuman, TELUS, Cursor, and Anthropic run billions to trillions of vectors on it.

---

## Reference Links

**Official documentation & site**
- Homepage — https://turbopuffer.com/
- Architecture — https://turbopuffer.com/docs/architecture
- Guarantees (consistency / durability) — https://turbopuffer.com/docs/guarantees
- Founding blog post: "turbopuffer: fast search on object storage" — https://turbopuffer.com/blog/turbopuffer
- Jobs / "index every byte" — https://turbopuffer.com/jobs

**Customer case studies**
- TELUS (25,000+ AI copilots) — https://turbopuffer.com/customers/telus
- Readwise (the origin story) — https://turbopuffer.com/customers/readwise
- Pylon (multi-tenant search in 3 days) — https://turbopuffer.com/customers/pylon
- Superhuman Mail — https://turbopuffer.com/customers/superhuman
- Linear — https://turbopuffer.com/customers/linear
- Notion (10B+ vectors) — https://turbopuffer.com/customers/notion

**Third-party analysis & SDK**
- "How Turbopuffer Serves 2.5 Trillion Vectors on S3" (Ajay Edupuganti, Medium) — https://ajay-edupuganti.medium.com/how-turbopuffer-serves-2-5-trillion-vectors-on-s3-7d7ab7f9a7fa
- Official Go client (turbopuffer-go) — https://github.com/turbopuffer/turbopuffer-go

*Note: Trillion-vector / 200M-namespace figures come from the third-party Medium analysis (citing founder interviews and the engineering blog), not official spec pages. The Pylon case study lists slightly different namespace/document counts in its header vs. body.*
