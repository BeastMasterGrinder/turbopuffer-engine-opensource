# 05 — How our clone maps to turbopuffer (and what we simplify)

This is the honest ledger: for every turbopuffer concept, what **our Go clone actually does**, and what
we deliberately leave out. The goal is *faithful core, readable code* — not production scale.

## Concept → implementation

| turbopuffer concept | Our clone | Faithful? |
|---|---|---|
| Object storage = source of truth | **MinIO** (S3-compatible) via docker-compose; the engine only persists to it | ✅ real S3 API |
| Namespace = a prefix | `{bucket}/{namespace}/...` with isolated `wal/` + `index/` | ✅ |
| WAL, durable before return | `Upsert` writes `wal/{seq}.json` then CAS-updates `manifest.json`; returns only after the S3 ack | ✅ |
| CAS coordination (no Raft/Kafka) | `manifest.json` updated with conditional **`If-Match: <etag>`** PUT; **412** → reload + retry | ✅ MinIO supports If-Match |
| Async indexing, compute/query split | `tpuf index` builds a new index **epoch**, then CAS-swaps the manifest pointer | ⚠️ a command, not a live broker/indexer |
| Unindexed WAL still searchable | both query paths **exhaustively scan the WAL tail** `[IndexedUpTo, WALSeq)` and union results | ✅ |
| Centroid (IVF) vector index | flat **k-means**, `K≈√N` clusters; query probes top **`nProbe`** (default 3) clusters | ⚠️ flat, not the 100×-fan-out hierarchy |
| RaBitQ binary scan → rerank | **RaBitQ-lite**: residual **sign-bit codes** + popcount prefilter → exact rerank | ⚠️ no rotation/unbiased estimator |
| Metadata filtering | `Eq` + `And`/`Or` over attributes, evaluated per candidate | ⚠️ map eval, not bitmap indexes |
| Full-text BM25 | hand-written inverted index + BM25 (k1=1.2, b=0.75) | ✅ in miniature |
| Three-tier DRAM/NVMe/S3 cache | in-memory **DRAM tier** (epoch-keyed map) over MinIO (**S3 tier**) | ⚠️ NVMe ring-buffer tier omitted |
| Single `Query` endpoint, `RankBy` | one `Query` call; `--vector` → ANN, `--bm25` → full-text | ✅ |
| Strong consistency by default | query routes through the manifest; new writes always visible | ✅ (no eventual-consistency mode) |

## Deliberate non-goals (and where each would hook in)

- **Group commit** (1 WAL entry/sec/namespace) — our CLI commits per `upsert`. *Hook:* buffer batches in
  a goroutine before the WAL write in `namespace.Upsert`.
- **Live broker + indexer processes / `queue.json`** — we index inline. *Hook:* a daemon that watches
  `IndexedUpTo` vs `WALSeq` and CAS-claims jobs.
- **SPFresh LIRE incremental updates** (split/merge/reassign) — we rebuild the index per `index` run.
  *Hook:* `internal/engine/indexer.go` would gain split/merge/reassign instead of full rebuild.
- **Hierarchical 100×-fan-out centroid tree** — we use one flat centroid level. *Hook:* recurse the
  clustering in `vector.go` and store levels.
- **Bitmap/attribute indexes + filter-first/search-first planner** — we evaluate filters per candidate.
  *Hook:* precompute `(attr_value, cluster) → roaring bitmap` in the indexer.
- **NVMe ring-buffer cache, consistent-hashing LB, true RaBitQ rotation, hybrid fusion, branches/CMEK,
  multi-region** — documented, not built.

## Why these simplifications are the *right* ones for a clone

The simplifications all preserve the **architecturally interesting** behavior — object-storage-first
persistence, CAS coordination, async-index-with-WAL-scan, two-step centroid retrieval, binary-prefilter
reranking — while removing scale machinery (hierarchy depth, incremental rebalancing, ring buffers) that
only earns its complexity at billions of vectors. You can run the clone, watch real objects appear in
MinIO, and trace every code path end to end.

## Project layout

```
cmd/tpuf/main.go               CLI: create | upsert | index | query | info
internal/storage/
  storage.go    ObjectStore interface + sentinel errors
  s3.go         S3(MinIO) impl with CAS (If-Match) — the only AWS SDK importer
  memory.go     in-memory ObjectStore w/ real CAS — infra-free tests & no-Docker demo
internal/cache/cache.go        DRAM-tier in-memory cache over object storage
internal/engine/
  types.go      Document, Manifest, RankBy, Filter, QueryResult
  manifest.go   load + CAS-save manifest
  wal.go        append / list / read WAL segments
  vector.go     cosine/euclidean, k-means, RaBitQ-lite codes
  bm25.go       tokenizer, inverted index, BM25 scoring
  indexer.go    build centroid + BM25 + docs index from WAL, CAS epoch swap
  query.go      planner: vector/BM25 + filters + WAL-tail scan + merge
  namespace.go  Namespace handle: Create, Upsert, Query, Info
*_test.go                      co-located table-driven unit tests (see docs/07)
docker-compose.yml             MinIO + auto-created `tpuf` bucket
docs/                          this knowledge base + the 3 source papers
```

> Layout follows the official go.dev "single command + supporting packages" shape; the full rationale
> and the **unit-testing strategy** (co-located `_test.go`, table-driven tests, the in-memory store) are
> in [`07-project-layout-and-testing.md`](./07-project-layout-and-testing.md).
