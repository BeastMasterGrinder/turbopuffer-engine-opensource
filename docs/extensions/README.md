# Extending the clone — non-goal designs

[`../05-clone-mapping.md`](../05-clone-mapping.md) lists the features tpuf originally deliberately
*didn't* build, each with a one-line **"Hook:"** noting where it would attach to the code. These
documents expand those non-goals into real, buildable design entries: how the feature works in
turbopuffer (sourced), where the clone stands today, and the concrete changes a faithful implementation
needs.

> **Status (2026-06-18): all eight extensions below are now implemented**, following
> [`IMPLEMENTATION-HANDOFF.md`](./IMPLEMENTATION-HANDOFF.md) — each with co-located table-driven tests
> (`go test ./... -race` green), a `cmd/tpuf-bench` demo or CLI/`deploy/` proof, and the 5 CAS
> correctness rules preserved. The matching KB doc remains the *design rationale* — read it before
> changing the code; each notes anything left deliberately deferred (e.g. weighted score fusion, branch
> GC). The hard non-goal — SPFresh's **LIRE** incremental rebalancing — has **Phase 1** implemented
> (Option A: incremental rebuild behind the unchanged single-CAS epoch); its deeper phases stay
> documented-not-built in [`../spfresh-lire/`](../spfresh-lire/).

These are the **low/medium-effort** extensions: each is a self-contained addition that preserves the
existing architecture.

| Doc | Expands the non-goal | One-line summary |
|---|---|---|
| [`group-commit.md`](./group-commit.md) | Group commit (1 WAL entry/sec/namespace) | Batch many concurrent upserts into one durable object-storage PUT to decouple write throughput from per-PUT S3 latency — the same trick relational engines use to amortize `fsync`. |
| [`broker-indexer-queue.md`](./broker-indexer-queue.md) | Live broker + indexer processes / `queue.json` | Move indexing off the query/write path: a stateless broker group-commits job requests into a CAS-coordinated `queue.json` and an indexer fleet claims them — "object storage is the source of truth" applied to *work scheduling*. |
| [`nvme-ring-buffer-cache.md`](./nvme-ring-buffer-cache.md) | NVMe ring-buffer cache tier | The middle cache tier: a single fixed-size local file written sequentially and wrapped FIFO when full — no LRU, no eviction — so most warm queries never pay the S3 round-trip. |
| [`bitmap-attribute-indexes.md`](./bitmap-attribute-indexes.md) | Bitmap/attribute indexes + filter planner | Precompute `(attr_value) → roaring bitmap` at build time so filters become whole-corpus bitmap intersections that can skip entire clusters — turbopuffer's "native filtering," avoiding the recall loss of post-filtering and the latency of naive pre-filtering. |
| [`hybrid-fusion.md`](./hybrid-fusion.md) | Hybrid search: fusing vector + BM25 | Merge a vector ranking and a BM25 ranking into one list when their scores are incomparable; covers Reciprocal Rank Fusion vs weighted score fusion, and why turbopuffer keeps fusion a thin client-side step. |
| [`hierarchical-centroid-tree.md`](./hierarchical-centroid-tree.md) | Hierarchical 100×-fan-out centroid tree | Make the centroid set *itself* a centroid index, recursively, so per-query centroid scanning stays cheap at billions of vectors — the shape of turbopuffer's "ANN v3" wide, shallow tree. |
| [`true-rabitq.md`](./true-rabitq.md) | True RaBitQ rotation | Upgrade RaBitQ-lite to the full method (Gao & Long, SIGMOD '24): a random orthogonal rotation plus an unbiased inner-product estimator with an `O(1/√D)` error bound, turning a biased sign-bit heuristic into a quantizer with a provable guarantee. |
| [`branches-copy-on-write.md`](./branches-copy-on-write.md) | Branches (copy-on-write namespaces) | A constant-time fork of a namespace: a new manifest points at the parent's immutable WAL/index objects and diverges only on write — turbopuffer's `branch_from`, enabled by the prefix-plus-manifest design. |
