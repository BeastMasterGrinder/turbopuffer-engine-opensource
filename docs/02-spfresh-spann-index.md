# 02 — The centroid vector index (SPANN + SPFresh)

turbopuffer's vector index is **centroid-based** ("derived from SPFresh"), *not* a graph index like
**HNSW** or **DiskANN**. This single choice is dictated by object storage.

## Why not HNSW? (the object-storage tax on graph indexes)

HNSW (Pinecone, Weaviate, Qdrant, Milvus) walks a graph of **10–20 sequential, *dependent* hops** —
each hop's next read depends on the previous result. Cost per hop:

| Storage | Per hop | 10–20 hops |
|---|---|---|
| RAM | ~300–1,800µs | fine |
| NVMe | ~1–10ms | workable |
| **S3** | **~50–100ms** | **1–2 seconds per query → unusable** |

A centroid index turns this into **~2 dependent steps instead of ~20**: (1) find the nearest centroids,
(2) fetch the matching clusters. Two round-trips, both parallelizable. That is the whole game on object
storage.

---

## SPANN — the base design (NeurIPS '21, `papers/spann-neurips21.pdf`)

SPANN is an **inverted-index / IVF** design with a clean memory/disk split:

- **In memory:** the **centroids** `c₁…c_N` of all posting lists, held in a fast ANN index (Microsoft's
  **SPTAG**). This gives sub-millisecond "which clusters are nearest?" lookups.
- **On disk:** the **posting lists** themselves — each posting list is a cluster of nearby vectors.

### Build (balanced clustering)
- SPANN uses a **large** number of posting lists — about **16% of the dataset size** (1M vectors →
  ~160K lists), tuned down to ~10–12% at billion scale so the centroid index fits the memory budget.
- Lists are kept **balanced** (similar sizes → bounded query latency) via **hierarchical balanced
  clustering (HBC)**: recursively k-means-split any oversized cluster until every list is under a length
  cap. **Length cap ≈ 12 KB for byte vectors, 48 KB for float vectors** (~96 vectors/list at 128-dim).
- **Boundary replication ("posting-list expansion"):** a vector near several cluster boundaries is
  **replicated into up to 8 nearby lists** (closure rule: assign to any centroid within `(1+ε₁)×`
  closest, ε₁≈10), with an **RNG rule** to avoid storing near-duplicate copies in the same direction.
  This recovers the recall that partial search would otherwise lose at boundaries.

### Query (partial search + dynamic pruning)
1. Find the `K` closest centroids in the in-memory index.
2. **Query-aware dynamic pruning:** only actually fetch a posting list if its centroid is within a
   relative factor of the closest — `Dist(q, c) ≤ (1+ε₂)·Dist(q, c_closest)` (ε₂≈0.6 for recall@1,
   ≈7 for recall@10). Easy queries fetch few lists; hard queries expand. (Motivation: on SIFT1M, 80% of
   queries need ~6 lists but 99% need ~114 — a fixed probe count is either wasteful or low-recall.)
3. Load those lists from disk, compute **exact** distances, rank, return top-K.

**Result:** ~90% recall@1 and recall@10 at billion scale in ~1ms with only 32GB RAM; **~2× faster than
DiskANN** at equal recall/memory.

---

## SPFresh — incremental updates on top of SPANN (SOSP '23, `papers/spfresh-sosp23.pdf`)

A static SPANN index is great until your data changes. Rebuilding a billion-scale index costs
64GB–1100GB RAM and **2–5 days**. SPFresh's contribution is the **LIRE** protocol: keep the index
correct under inserts/deletes with only **tiny, local** rewrites.

### The governing invariant: NPA (Nearest Partition Assignment)
> **Every vector must live in the posting list whose centroid is nearest to it.**

Given the centroid set, membership is uniquely determined. All of LIRE exists to preserve NPA cheaply as
data shifts. Five operations:

| Op | Trigger | What it does |
|---|---|---|
| **Insert** | user write | Append vector to the **tail of its nearest posting** (read-modify-write of only the last disk block). If the posting exceeds the split limit → enqueue **Split**. |
| **Delete** | user write | **Tombstone only** — flip a delete bit / bump version in an in-memory **1-byte-per-vector** version map. No disk write. GC'd lazily. |
| **Split** | posting > max length | GC deletes first; if still too big, **2-means split** into two new postings with two new centroids; remove old centroid, add the two new ones; then **Reassign**. |
| **Merge** | posting < min length | Append its vectors into the **nearest** posting, delete the small posting + its centroid, then **Reassign** only the moved vectors. |
| **Reassign** | after split/merge | Fix NPA for the small boundary set that may now be mis-assigned (see below). |

### Reassign — the clever, cheap part
After a split (old centroid `A₀` → new `A₁,A₂`), only a **tiny boundary set** can violate NPA. LIRE
checks just the **64 nearest postings** to `A₀` (the "reassign range") using two formally-derived rules:

- **Vectors that were in the split posting:** candidate to move iff `D(v,A₀) ≤ D(v,Aᵢ)` for **both** new
  centroids (the old centroid was still closer than both new ones → `v` may belong to a *neighbor*).
- **Vectors in nearby postings `B`:** candidate to move iff `D(v,Aᵢ) ≤ D(v,A₀)` for **some** new
  centroid (a new centroid got closer → `v` may belong in `A₁`/`A₂` now).

For each surviving candidate: find its true nearest posting, re-check NPA (drop false positives), then
move it (append-with-new-version to the correct posting, tombstone the old copy).

**Why it's basically free (measured, 1% daily updates):** only **0.4% of inserts trigger any
rebalancing**; a split-reassign evaluates ~5,094 vectors but actually moves only **~79**; mean cascade
length 3. The index stays self-consistent without ever doing a global rebuild.

### Concrete numbers
- Query probes the **nearest 64 postings** by default.
- P99.9 latency ~**4 ms, stable** (2.41× lower than DiskANN, which spikes >20 ms during rebuilds).
- Insert ~**1.5 ms**; steady-state RAM **<20 GB**; recall10@10 **≥0.86 (uniform) / ≥0.81 (skewed)** at
  billion scale.

---

## How turbopuffer describes *their* version (ANN v3)

turbopuffer's published specifics (use these as our mirror-able defaults):

- A **hierarchical tree of centroids** with a **100× branching factor per level** (chosen to match the
  DRAM↔SSD size ratio). Upper levels (≤128 MiB: `100³ × 128 bytes`) stay resident in **L3 cache/DRAM**.
- **Leaf clusters ≈ 100 vectors each.** A 100B-vector query scans **~500 leaf clusters**.
- Per-level data fetched ≈ `500 clusters × 100 vectors × 1024 dims × 2 bytes (f16) = 100 MB`.
- The hierarchy **bounds S3 round-trips to the tree height** (~3–4 for a fully cold query, ~100 ms each).
- Headline: **92% recall@10, p99 200 ms, over 100 billion vectors** (1024-dim, unfiltered, v3 beta).

> ⚠️ turbopuffer does **not** publish literal filenames `centroids.bin`/`clusters-N.bin` — that's a
> third-party guess. They describe a centroid index + per-cluster blobs fetched by offset. Our clone
> uses its own JSON filenames.

---

## What our clone implements (a faithful "SPANN-lite")

We implement the **base SPANN/IVF idea** (this is what makes the object-storage story work) and **note**
where SPFresh's LIRE would extend it:

1. **Build:** flat k-means (Lloyd's), `K ≈ round(√N)` clusters — a standard small-scale IVF heuristic.
   (turbopuffer's 100×-fan-out *hierarchy* matters at billions of vectors; at demo scale, one flat level
   of centroids reproduces the same two-step query.)
2. **Assign:** each doc → nearest centroid (we keep it simple; boundary replication is noted, not built).
3. **Query:** score query vs all centroids → probe top **`nProbe` (default 3)** clusters → load them →
   **RaBitQ-lite binary prefilter → full-precision rerank** (see [`03`](./03-rabitq-quantization.md)) →
   union with the exhaustive scan of the unindexed WAL tail → filter → top-K.
4. **Updates:** we **rebuild the index** on `tpuf index` (epoch swap via CAS). LIRE's incremental
   split/merge/reassign is documented here as the production approach but not implemented — rebuild is
   correct and clearer at demo scale.

See the honest trade-off table in [`05-clone-mapping.md`](./05-clone-mapping.md).
