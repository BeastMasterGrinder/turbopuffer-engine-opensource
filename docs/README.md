# How turbopuffer works — and how this clone mirrors it

This folder is the **knowledge base** for the clone. It explains, in detail, the ideas the real
turbopuffer is built on, with the **primary research papers stored locally** in [`papers/`](./papers).
Everything here is sourced; where turbopuffer does *not* publicly confirm a number, it's flagged
explicitly so we never mirror a guessed figure.

## Read in this order

| # | Doc | What it covers |
|---|-----|----------------|
| 1 | [`01-architecture.md`](./01-architecture.md) | The core bet (object storage = source of truth), namespaces-as-prefixes, the WAL, the `manifest.json` + **CAS** coordination model (no Raft/Kafka), the DRAM/NVMe/S3 cache tiers, and the **write / index / query** paths. |
| 2 | [`02-spfresh-spann-index.md`](./02-spfresh-spann-index.md) | The **centroid (IVF) vector index** — SPANN's posting-list design, SPFresh's incremental **LIRE** update protocol (split / merge / reassign, the NPA rule), why this beats HNSW on object storage. |
| 3 | [`03-rabitq-quantization.md`](./03-rabitq-quantization.md) | **RaBitQ** binary quantization: D-dim vector → D-bit code, the XOR/AND+popcount estimator, the two-stage *binary-scan → full-precision rerank*, and the simplified version we implement. |
| 4 | [`04-bm25-fulltext.md`](./04-bm25-fulltext.md) | **Full-text search**: the inverted index and the BM25 ranking formula (k1, b), and how unindexed writes stay searchable. |
| 5 | [`05-clone-mapping.md`](./05-clone-mapping.md) | The honest mapping: **what our Go clone does for each concept**, the deliberate simplifications, and the 3 corrections to common turbopuffer misconceptions. |
| 6 | [`06-implementation-blueprint.md`](./06-implementation-blueprint.md) | The **code-ready plan**: every Go type, signature, algorithm, edge case, build order, and the end-to-end verification recipe. |
| 7 | [`07-project-layout-and-testing.md`](./07-project-layout-and-testing.md) | The **directory layout** (official go.dev guidance) and the **unit-testing strategy**: co-located `_test.go`, table-driven tests, and the in-memory `ObjectStore` that makes the engine testable without MinIO. |

## The papers (stored locally)

See [`papers/SOURCES.md`](./papers/SOURCES.md). Short version:

- **SPFresh** (SOSP '23) → `papers/spfresh-sosp23.pdf` — turbopuffer's vector index is "derived from SPFresh."
- **SPANN** (NeurIPS '21) → `papers/spann-neurips21.pdf` — the centroid/posting-list index SPFresh builds on.
- **RaBitQ** (SIGMOD '24) → `papers/rabitq-sigmod24.pdf` — the binary quantization used to score inside a cluster.

## The one-sentence summary

> Object storage is cheap, durable, and (since 2020) strongly consistent — so make it the **source
> of truth**, and engineer everything else (a centroid index with 2 dependent steps not HNSW's 20,
> a DRAM/NVMe cache, and CAS-on-a-JSON-file coordination) to **hide its latency**.
