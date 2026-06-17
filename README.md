# tpuf — an educational turbopuffer clone

`tpuf` is a small, **educational** clone of [turbopuffer](https://turbopuffer.com): a vector +
full-text search engine built on one bet — **object storage is the source of truth, and everything
else exists to hide its latency.**

It is a Go core-engine library plus a CLI (`create | upsert | index | query | info`) that does
**centroid/IVF vector search** and **BM25 full-text search** over **MinIO** (S3-compatible) running in
Docker. The goal is a *faithful core in readable code* — not production scale. When a choice is between
clever and clear, this codebase chooses clear.

> This README summarizes the design. The full, sourced knowledge base lives in [`docs/`](./docs) —
> start at [`docs/README.md`](./docs/README.md). Where turbopuffer does not publicly confirm a number,
> the docs flag it explicitly; this README does the same and never asserts an unconfirmed internal as
> fact.

---

## The core idea

Incumbent search engines keep data on RAM plus triply-replicated SSD, inheriting architectures built
for low-latency *transactional* updates. turbopuffer's observation: **search is not a transactional
database** — its write workload looks like a **data warehouse** (high throughput, no transactions,
relaxed write latency), with the one extra requirement that *reads* come back fast (turbopuffer targets
under ~100 ms). So it pairs **warehouse-style storage** (S3/GCS, ~$0.02/GB) with **search-style read
latency** (a smart index plus caching).

Three ideas make that work, and `tpuf` implements all three:

1. **Object storage is the source of truth.** A namespace is just a key prefix
   (`{bucket}/{namespace}/...`) holding a write-ahead log and a built index. Cheap, durable, and (since
   S3 went strongly consistent in Dec 2020 and added conditional writes) coordinatable.
2. **Coordination is CAS on a single JSON file — no Raft, no Kafka, no Zookeeper.** A `manifest.json`
   is updated with a conditional `If-Match: <etag>` PUT. If the ETag still matches, the write wins; if
   not, the store returns **HTTP 412 Precondition Failed** and the writer reloads and retries. That is
   exactly `UPDATE ... WHERE version = N`, delegated to object storage. (MinIO supports both `If-Match`
   and `If-None-Match`, so the clone implements the real CAS model faithfully.)
3. **Unindexed data is still searchable.** Indexing is asynchronous. Until a write is folded into the
   index, queries find it by **exhaustively scanning the unindexed WAL tail** and unioning the results.

> Sources for the above: the turbopuffer founding blog and architecture/concepts pages, distilled in
> [`docs/01-architecture.md`](./docs/01-architecture.md).

---

## Architecture map

### What lives on object storage

A namespace is fully isolated under its own prefix:

```
{bucket}/{namespace}/
├── manifest.json                 # the CAS-coordinated source-of-truth head (version, dim, metric,
│                                 #   WALSeq, IndexedUpTo, IndexEpoch, DocCount)
├── wal/00000000000000000000.json # committed write segments (the LSM log), 20-digit zero-padded seq
├── wal/00000000000000000001.json
└── index/v{epoch}/               # one immutable, write-once index epoch
    ├── centroids.json            # centroid vectors + cluster sizes
    ├── cluster-0.json            # vectors (+ RaBitQ-lite codes) + attrs for cluster 0
    ├── bm25.json                 # inverted index for full-text
    └── docs.json                 # id -> attributes (filtering + return payloads)
```

The `manifest.json` is the head: its ETag is the CAS token, and it points at the live index epoch and
the WAL high-water mark.

### How the code is laid out

The repo follows the official go.dev "single command + supporting packages" shape (see
[`docs/07`](./docs/07-project-layout-and-testing.md)): a CLI in `cmd/`, the engine in `internal/`.

```
cmd/tpuf/main.go               CLI: create | upsert | index | query | info
cmd/tpuf-bench/main.go         latency benchmark: p50..p99.9, single/multi-tenant/cold-vs-hot modes
cmd/tpuf-node/main.go          stateless HTTP query server (for the load-balancer demo)
internal/bench/bench.go        latency Recorder + nearest-rank percentile summary + table renderer
internal/storage/
  storage.go    ObjectStore interface (Get/PutCAS/PutIfAbsent/Put/List) + sentinel errors
  s3.go         S3(MinIO) impl with CAS (If-Match) — the only file importing the AWS SDK
  memory.go     in-memory ObjectStore with real CAS — infra-free tests & a no-Docker demo mode
internal/cache/cache.go        DRAM-tier cache over object storage (immutable index objects only)
internal/engine/
  types.go      Document, WALSegment, Manifest, RankBy, Filter, QueryResult, on-disk index shapes
  manifest.go   load + CAS-save the manifest
  wal.go        append / list / read WAL segments; materialize live docs (last-writer-wins)
  vector.go     cosine/euclidean distances, k-means, RaBitQ-lite codes
  bm25.go       tokenizer, inverted index, BM25 scoring
  indexer.go    build centroid + BM25 + docs index from the WAL, then CAS the epoch swap
  query.go      planner: vector/BM25 + filters + WAL-tail scan + merge
  namespace.go  Namespace handle: Create, Upsert, Index, Query, Info
deploy/                        nginx consistent-hash load-balancer demo (see deploy/README.md)
benchmarks/                    run.sh (canned bench configs) + RESULTS.txt (saved runs)
```

### The three paths

- **Write (`upsert`)** — durable before return. Append a new `wal/{seq}.json` segment (write-once, via
  `If-None-Match`), then CAS the manifest to bump `WALSeq`. The call returns only after the store
  acknowledges; a successful return means the data is durably on object storage.
- **Index (`index`)** — build a fresh epoch. Snapshot `WALSeq` at the *start*, materialize the live
  docs, run k-means (K ≈ √N clusters) and BM25, write every `index/v{epoch}/*` object under the new
  prefix, then publish with a **single manifest CAS** that flips `IndexEpoch` and sets `IndexedUpTo` to
  the start snapshot. Until that one CAS lands, queries keep serving the old epoch.
- **Query (`query`)** — one endpoint, mode chosen by `RankBy`: a vector triggers ANN (probe the top
  `nProbe` clusters, RaBitQ-lite prefilter, exact rerank); text triggers BM25 over the inverted index.
  Either way the query then **exhaustively scans the unindexed WAL tail** `[IndexedUpTo, WALSeq)`,
  applies last-writer-wins (newer overwrites the indexed copy), subtracts tombstoned deletes, applies
  metadata filters, sorts, and returns top-K. With no index yet (`IndexEpoch == 0`) it serves purely
  from the tail — proof that unindexed data is searchable.

---

## How to run (end-to-end)

You need Docker (for MinIO) and Go 1.26.

```bash
# 1. Boot MinIO (S3 API :9000, console :9001) and auto-create the `tpuf` bucket.
docker compose up -d

# 2. Load the S3 credentials/endpoint into your shell.
set -a; source .env.example; set +a

# 3. Build and run the engine's unit tests (fast; no infra needed thanks to the in-memory store).
go mod tidy && go build ./... && go test ./internal/engine/...

# 4. Create a namespace: 4-dim cosine vectors, full-text on the `body` attribute.
go run ./cmd/tpuf create demo --dim 4 --metric cosine --text-field body

# 5. Upsert the sample docs (4-dim vectors + a `body` text field + a `lang` attribute).
go run ./cmd/tpuf upsert demo --file examples/sample.json

# 6. Query BEFORE indexing — answered entirely from the WAL tail (the headline demo).
go run ./cmd/tpuf query demo --vector "0.1,0.2,0.3,0.4" --top-k 3
go run ./cmd/tpuf query demo --bm25 "quick walrus" --top-k 3

# 7. Build and publish an index epoch via a single manifest CAS.
go run ./cmd/tpuf index demo

# 8. Query AFTER indexing — the indexed path plus the (now empty) WAL tail.
go run ./cmd/tpuf query demo --vector "0.1,0.2,0.3,0.4" --n-probe 3
go run ./cmd/tpuf query demo --bm25 "walrus" --filter '{"op":"eq","field":"lang","value":"en"}'

# 9. Inspect the manifest.
go run ./cmd/tpuf info demo

# 10. Open the MinIO console to see every object as it is written.
#     http://localhost:9001  (login with MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
#     You'll see demo/manifest.json, demo/wal/..., and demo/index/v1/...
```

The two proofs to watch for: **step 6 returning results with no index present** shows unindexed WAL
data is searchable; the **MinIO console showing every object** shows object storage is the source of
truth.

### Other useful commands

```bash
gofmt -w . && go vet ./...                              # format + vet
go test ./...                                            # full unit suite — fast, no infra
go test ./... -race                                      # exercises the CAS / concurrent-upsert paths
go test ./internal/storage -tags=integration             # real-MinIO 412 contract test (needs Docker)
```

### Benchmarking latency (p50..p99.9)

`tpuf-bench` builds a fresh namespace, upserts synthetic docs in batches, then times vector and BM25
queries **before** indexing (each query scans the unindexed WAL tail) and **after** (served from the
indexed epoch with the DRAM cache warm). The gap between those rows is the whole thesis made visible.

```bash
go run ./cmd/tpuf-bench --backend memory --docs 2000 --batch 100 --queries 300   # pure engine cost, no infra
set -a; source .env.example; set +a                                              # then, against real MinIO:
go run ./cmd/tpuf-bench --backend s3 --docs 500 --batch 50 --queries 50          # real object-storage latency
```

A representative MinIO run: the vector **tail scan** lands around p50 16ms / p99 19ms (a manifest read
plus one object read per WAL segment, every query — the WAL is never cached), while the **indexed** path
is ~p50 2ms / p99 4ms (only the manifest GET hits MinIO; index objects come from the DRAM cache). Latency
is dominated by object-storage round-trips, which is exactly what the index and cache exist to hide.

**Multi-tenant mode** (`--namespaces N --concurrency C`) is the realistic regime: N tenants are each
created + indexed, then C worker goroutines issue queries spread across them, sharing one DRAM cache.
Pair it with `--cache-objects M` — a bounded LRU cache — set below the resident working set to watch
cold-start misses recur under tenant churn (the pressure a finite DRAM tier, and the real product's
omitted NVMe tier, must absorb):

```bash
go run ./cmd/tpuf-bench --backend s3 --namespaces 12 --concurrency 12 \
    --dim 256 --docs 1000 --queries 6000 --cache-objects 80     # bounded: forces eviction
go run ./cmd/tpuf-bench --backend s3 --namespaces 12 --concurrency 12 \
    --dim 256 --docs 1000 --queries 6000 --cache-objects 0      # unbounded: every tenant stays resident
```

On a terminal, multi-tenant runs show a **live [Bubble Tea](https://github.com/charmbracelet/bubbletea)
dashboard** (per-phase progress bars, throughput, cache hit-rate, and ETA); piped or non-TTY runs fall
back to plain periodic progress lines. It reports aggregate p50…p99.9 under load, achieved wall-clock
throughput, and the shared cache's hit/miss/eviction counts. Capping the cache below the working set
drops the vector hit rate sharply
(e.g. ~99% → ~53%) because each tenant's vector index is many objects (centroids + cluster files),
while BM25 — two objects per tenant — stays hot.

There is also a canned runner and saved reference numbers:

```bash
benchmarks/run.sh                          # memory backend, single-tenant (no infra)
benchmarks/run.sh --backend s3 --mode all  # single + multi-tenant + cold-vs-hot against MinIO
```

### Benchmark results (representative, MinIO on one dev machine — read the *shape*)

Full output and methodology in [`benchmarks/RESULTS.txt`](./benchmarks/RESULTS.txt). The headline findings:

| What | Measurement | Takeaway |
|---|---|---|
| **Index vs. WAL-tail scan** (single tenant) | vector query p50 **18ms → 2.9ms**, p99 **28ms → 4.9ms** | indexing + DRAM cache cut tail latency ~6× |
| **Cold vs. hot, same query** | vector p50 **13.5ms (cold) → 7.9ms (hot)**, 1.7×; cold pass = 1200 cache misses, hot pass = **0** | the cache's win is the avoided object-storage round-trip |
| **Cache pressure** (12 tenants, dim 256) | bounded 80-obj cache **53.8%** hot, p50 38.8ms, 297 qps → unbounded **99.7%** hot, p50 21.9ms, **520 qps** | a finite DRAM tier degrades gracefully; the manifest read is the latency floor |
| **Scaling (consistent hashing)** | add a 4th node → **40/50 namespaces stay put**, the 10 that move all go to the new node | scaling doesn't cold-flush existing caches (plain `hash % N` would move ~37/50) |

> Caveat the benchmark surfaces honestly: every query does one **uncached manifest GET** (correctness
> rule 2), so even a 100%-cache-hit query has a ~object-storage-round-trip floor. Real turbopuffer
> optimizes that away; the clone leaves it visible.

### Load-balancer demo (namespace routing)

An optional demo of turbopuffer's routing tier — `nginx` consistent-hashing the namespace to one of
several identical Go query nodes (so a tenant's cache stays warm on one node, while *any* node can serve
*any* namespace because state is on S3). Full walkthrough in [`deploy/README.md`](./deploy/README.md):

```bash
docker compose --profile lb up -d --build    # 3 nodes + nginx, reusing MinIO (LB on :8088)
deploy/demo.sh                                 # show hash(namespace) -> node + per-node cache warmth
deploy/remap-demo.sh                           # add a node, watch only ~1/N namespaces remap
```

---

## Honest notes: Go vs. Rust, and non-goals

### Go vs. Rust

The real turbopuffer is written in **Rust**, chosen for predictable low-latency performance, tight
memory control, and no GC pauses on the query hot path — the properties you want when you are squeezing
ranged reads out of object storage at scale.

This clone is written in **readable Go** on purpose. The trade is deliberate: Go's standard library,
simple concurrency, and low ceremony make the *architecturally interesting* mechanisms — the CAS retry
loop, the WAL-tail scan, the two-step centroid retrieval, the binary-prefilter rerank — easy to read end
to end. We are **not** chasing turbopuffer's latency or throughput numbers; we are reproducing its
*shape* so you can trace every code path. Concretely, the clone leans on Go idioms that aid clarity over
raw speed: the engine talks only to a small `ObjectStore` interface (so the whole thing is testable
against an in-memory store with no Docker), errors are wrapped and matched with `errors.Is`/`errors.As`,
and the 412/404 contract is the single load-bearing concurrency primitive rather than a custom runtime.

### What this clone faithfully reproduces

Object-storage-first persistence; namespaces as isolated prefixes; durable-before-return WAL writes;
CAS-on-a-JSON-file coordination with real `If-Match`/412 retries; async indexing published via an atomic
epoch swap; exhaustive WAL-tail scan so fresh writes are searchable; a centroid (IVF) vector index with
two-step retrieval; a RaBitQ-style binary prefilter then exact rerank; hand-written BM25 (k1=1.2,
b=0.75); a single `Query` endpoint whose mode is chosen by `RankBy`; and strong consistency by default
(the query routes through the manifest, so new writes are always visible).

### Deliberate non-goals (and where each would hook in)

Drawn from [`docs/05-clone-mapping.md`](./docs/05-clone-mapping.md):

- **Group commit** (turbopuffer batches concurrent writes — reported as ~1 WAL entry/sec/namespace) —
  the clone commits per `upsert`. *Hook:* buffer batches in a goroutine before the WAL write.
- **Live broker + indexer processes / `queue.json`** — the clone indexes inline via the `index`
  command. *Hook:* a daemon watching `IndexedUpTo` vs `WALSeq` that CAS-claims jobs.
- **SPFresh LIRE incremental updates** (split/merge/reassign) — the clone rebuilds the index per `index`
  run. *Hook:* `internal/engine/indexer.go` would gain incremental rebalancing.
- **Hierarchical 100×-fan-out centroid tree** — the clone uses one flat centroid level (K ≈ √N). *Hook:*
  recurse the clustering and store levels.
- **Bitmap / attribute indexes and a filter-first vs search-first planner** — the clone evaluates
  filters per candidate. *Hook:* precompute `(attr_value, cluster) → bitmap` in the indexer.
- **NVMe ring-buffer cache tier** — the clone implements the **DRAM tier** (an in-memory map over MinIO,
  caching only immutable index objects) and the **S3 tier** (MinIO); the NVMe tier is documented but
  omitted.
- **True RaBitQ rotation/unbiased estimator, hybrid fusion, branches/CMEK, multi-region** —
  documented, not built. (A consistent-hashing **load-balancer demo** *is* included under
  [`deploy/`](./deploy) — nginx routing namespaces to Go query nodes — but it sits outside the core
  engine; see [`deploy/README.md`](./deploy/README.md).)

These simplifications all preserve the behavior that makes the architecture interesting while dropping
scale machinery (hierarchy depth, incremental rebalancing, ring buffers) that only earns its complexity
at billions of vectors.

---

## Status

The engine, CLI, benchmark tool, and load-balancer demo are **implemented and tested**: `go test ./...`
passes (unit tests, no infrastructure), `go test ./... -race` is clean, and the real-MinIO contract test
passes under `-tags=integration`. The full end-to-end recipe in
[`docs/06`](./docs/06-implementation-blueprint.md) runs against MinIO. The research and design remain
sourced in [`docs/`](./docs).

The **engine's** single external dependency is `aws-sdk-go-v2/service/s3`. Everything conceptually
interesting — vector math, k-means, the RaBitQ-lite codes, BM25, and the CAS loop — is hand-written
standard-library Go, because reaching for a library to implement a core concept would defeat the point of
the clone. The one exception is the optional benchmark CLI (`cmd/tpuf-bench`), which uses
`charmbracelet/bubbletea` + `bubbles` + `lipgloss` for its live progress TUI — dev tooling that sits
outside the engine, so the engine's single-dependency invariant holds.
