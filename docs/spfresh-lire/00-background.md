# SPFresh & SPANN — background: why an incremental, balanced posting-list index

> First entry in the `spfresh-lire/` series. It sets up the **problem** the LIRE protocol solves; the
> later entries describe LIRE's mechanism (split / merge / reassign, the NPA rule) and how it would hook
> into this clone. The summary view lives in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)
> and the deliberate non-goal it expands is in [`../05-clone-mapping.md`](../05-clone-mapping.md):
> *"SPFresh LIRE incremental updates (split/merge/reassign) — we rebuild the index per `index` run.
> Hook: `internal/engine/indexer.go` would gain split/merge/reassign instead of full rebuild."*

**What this is and why turbopuffer cares.** turbopuffer's vector index is centroid-based, and turbopuffer
publicly says it is **"based on SPFresh"** (ANN v3 blog; [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)
phrases the same fact as *"derived from SPFresh"* — keep this as **their** stated lineage, not an internal
fact we independently verified). SPFresh (SOSP '23) is itself built **on top of SPANN** (NeurIPS '21):
SPANN is the static, disk-friendly posting-list (IVF) index, and SPFresh adds the **LIRE** protocol that
keeps that index correct and balanced as vectors are inserted and deleted — *without ever rebuilding it
globally*. This entry explains (1) what SPANN's design is and why it is the right shape for an
object-storage system like turbopuffer, and (2) precisely why naive insertion into a balanced IVF index
degrades it over time — which is the gap LIRE exists to close. Everything below is sourced from the two
local papers; turbopuffer's own claims are flagged as such.

## SPANN: a balanced posting-list (IVF) index built for disk

SPANN follows the **inverted-index / IVF** methodology (SPANN §1, §3). Its defining choice is a clean
memory/disk split (SPANN §3, "Index structure"):

- **In memory:** the **centroids** `c₁ … c_N` of all posting lists, held in a fast in-memory ANN index
  (Microsoft's **SPTAG**) so "which posting lists are nearest to `q`?" is a sub-millisecond lookup
  (SPANN §3.2.1).
- **On disk:** the **posting lists** themselves. A posting list is a cluster of nearby vectors; the data
  vectors `X` are partitioned into `N` posting lists `X₁ … X_N` with `X₁ ∪ … ∪ X_N = X` (SPANN §3,
  "Index structure").

**Query is partial search** (SPANN §3, "Partial search"): for a query `q`, find the `K ≪ N` closest
centroids in the in-memory index, load **only** those `K` posting lists from disk, compute exact
distances over their vectors, and return the top results. This is the architectural payoff the whole
design is built around — see "Why this shape fits object storage" below.

SPANN identifies three challenges a usable disk-based IVF index must solve (SPANN §3.1) and gives a
technique for each (SPANN §3.2):

| Challenge (SPANN §3.1) | Why it hurts | SPANN's technique (SPANN §3.2) |
|---|---|---|
| **Posting length limitation** | Postings live on disk; an oversized posting costs many disk reads, and *uneven* posting sizes give high-variance (long-tail) query latency. | **Balanced clustering** — partition into many posting lists while *minimizing the variance* of posting length `Σ(|Xᵢ| − |X|/N)²` (SPANN §3.2.1, Eq. 1), via **hierarchical balanced clustering** that recursively k-means-splits oversized clusters until each is under a length cap. |
| **Boundary issue** | A query's true neighbors may sit just across a cluster boundary in a posting list whose centroid isn't among the nearest `K`, so partial search misses them (SPANN §3.1, Figure 1). | **Posting-list expansion** — replicate a boundary vector into multiple nearby postings under a *closure* rule (within `(1+ε₁)×` the closest centroid), de-duplicated by an *RNG* rule (SPANN §3.2.2, Eq. 2, Figures 4–5; up to 8 replicas, §4.2). |
| **Diverse search difficulty** | On SIFT1M, 80% of queries recall top-1 from ~6 postings, but 99% need ~114 — a fixed probe count is either wasteful or low-recall (SPANN §3.1, Figure 2). | **Query-aware dynamic pruning** — fetch posting `i` only if `Dist(q, cᵢ) ≤ (1+ε₂)·Dist(q, c₁)` (SPANN §3.2.3, Eq. 3; ε₂≈0.6 for recall@1, ≈7 for recall@10, §4.2). |

**Why this shape fits object storage.** A graph index (HNSW/DiskANN) answers a query by walking
~10–20 *sequential, dependent* hops, where each hop's next read depends on the previous result. On S3,
where a cold read is tens to hundreds of milliseconds, that serializes into seconds per query
([`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md), "Why not HNSW?";
[`../01-architecture.md`](../01-architecture.md) cache-tier table). A centroid index turns this into
**~2 dependent steps** — (1) score the in-memory centroids, (2) fetch the nearest posting lists — both of
which parallelize. That is the entire reason an IVF/posting-list design, not a graph, is what an
object-storage-first engine wants. SPANN's own results: ~90% recall@1/recall@10 at billion scale in
~1 ms with 32 GB RAM, ~2× faster than DiskANN at equal recall/memory (SPANN Abstract, §4.2.1).

## Why naive insertion degrades a balanced IVF index over time

Everything above describes a **static, freshly built** index. The moment data changes, the question is
how to absorb the change. A cluster-based index is *cheaper* to update than a graph: inserting a vector
touches only one posting list (append it to the nearest one), whereas a graph insert must wire the new
vector into hundreds of neighbor edges (SPFresh §1, §2.3). That cheapness is exactly why turbopuffer
chose a cluster-based index. But cheap-per-insert is not the same as *correct over time*.

The governing correctness property is **NPA — Nearest Partition Assignment** (SPFresh §3.2): *every
vector should live in the posting list whose centroid is nearest to it*, so its centroid faithfully
represents it during partial search. Given the centroid set, NPA uniquely determines membership.

Now watch what naive insert-and-never-rebalance does to it:

1. **Centroids are fixed; the data distribution drifts.** Inserts append to whatever posting is currently
   nearest, but the centroids were computed from the *old* data and are never moved. As writes accumulate,
   the live distribution shifts away from those frozen centroids, so more and more vectors end up *not* in
   their true-nearest posting — NPA quietly rots. SPFresh §2.3 ("Early attempts to in-place update")
   observes this directly: applying a Vearch-style naive in-place insert to SPANN, "the recall will
   decline as static centroids cannot capture the gradual distribution shift in the partition," and
   updating one-third of the vectors "degrades the query recall by more than one point and increases tail
   latency by 4X" compared to a static build.

2. **Postings become unbalanced, so latency long-tails.** Appends are not uniform — hot regions of the
   space grow their postings without bound. Because query latency is dominated by how many vectors a
   probed posting contains, *unbalanced* postings reintroduce exactly the "posting length limitation"
   SPANN worked to eliminate (SPANN §3.1): SPFresh §2.3 notes query latency "will increase due to the
   expansion of the posting length." The carefully variance-minimized partition (SPANN §3.2.1, Eq. 1)
   decays back into a skewed one.

3. **The industry workaround — periodic global rebuild — is brutal at scale.** Because in-place updates
   degrade quality, existing ANN systems instead accumulate updates in a secondary index and periodically
   **rebuild the whole index** out-of-place (SPFresh §1, §2.3). The cost is the headline motivation for
   SPFresh: rebuilding a 1-billion-vector DiskANN index needs **1100 GB DRAM for ~2 days, or ~5 days under
   a 64 GB / 16-vCPU budget** (SPFresh §2.3, Table 1). Rebuilding can cost *more than the index serving
   itself*, and it causes "catastrophic drop[s] in query performance because of severe computational
   resource starvation" (SPFresh §2.3).

```
# naive insert into a static IVF index — correct per-op, wrong over time
on insert(v):
    p = nearest_posting(v)        # by FROZEN centroids
    append v to p                 # one cheap disk append
    # centroids never move  -> NPA decays as the distribution drifts (problem 1)
    # p grows without bound  -> postings unbalance, tail latency rises (problem 2)
# the usual fix: every so often, throw it all away and rebuild (problem 3 — days of compute)
```

**This is the precise problem LIRE solves.** SPFresh's insight (SPFresh §1, §3.1): a single vector update
to an *already* well-balanced index can only perturb a **small, local** region, so the fix should also be
small and local. LIRE keeps postings balanced by **splitting** an oversized posting and **merging** an
undersized one *incrementally*, and restores NPA by **reassigning** only the tiny boundary set of vectors
a split/merge could have mis-assigned — identified by two formally-derived necessary conditions (SPFresh
§3.2, §3.3). Empirically the local region is tiny: in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)'s
reading of the paper, only ~0.4% of inserts trigger any rebalancing and a split evaluates ~5,094 vectors
but moves only ~79. The result is a billion-scale index that stays correct and balanced under continuous
updates with **no global rebuild** — SPFresh reports <20 GB steady-state RAM and 2.41× lower tail latency
than DiskANN (SPFresh Abstract). The LIRE mechanism itself is the subject of the next entry in this
folder (and is summarized in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) §SPFresh).

## What our clone does today, and the gap

Our clone implements the **base SPANN/IVF idea** — flat (single-level) centroids built by k-means — and
**rebuilds** the index on every `tpuf index` run instead of updating it incrementally. Concretely
([`../05-clone-mapping.md`](../05-clone-mapping.md), [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)):

- **Build.** [`internal/engine/indexer.go`](../../internal/engine/indexer.go) `buildVectorIndex` calls
  `ChooseK(n)` (returns `K ≈ round(√n)`, [`internal/engine/vector.go`](../../internal/engine/vector.go))
  and runs `KMeans(points, k, metric, iters)` **once** — a single flat partition, Lloyd's algorithm, no
  balance constraint and no recursion. It buckets each document into its assigned cluster (computing a
  RaBitQ-lite `ResidualCode`) and writes one `CentroidsFile` (`centroids.json`) plus one `ClusterFile`
  (`cluster-{i}.json`) per cluster ([`internal/engine/types.go`](../../internal/engine/types.go)).
- **Update.** There is **no** insert/delete-into-index path. A new upsert lands in the WAL
  ([`internal/engine/wal.go`](../../internal/engine/wal.go) `AppendWAL`, via `Namespace.Upsert` in
  [`internal/engine/namespace.go`](../../internal/engine/namespace.go)); it becomes searchable immediately
  via the WAL-tail scan, and is only folded into the index the next time `BuildIndex` runs — which
  **re-clusters from scratch** over the whole live set under a fresh epoch.

**The gap (and why it's the right call here):** because we rebuild from scratch each epoch, NPA is
trivially re-established and postings are re-balanced every time — we never *experience* the degradation
described above, so we never need LIRE. At demo scale (`K ≈ √N`, e.g. ~32 centroids for ~1,000 docs) a
full rebuild is milliseconds; LIRE's split/merge/reassign would add substantial code to optimize a cost
that is already negligible. SPFresh's machinery earns its complexity only at billions of vectors where a
rebuild costs days (SPFresh §2.3, Table 1). This is exactly how
[`../05-clone-mapping.md`](../05-clone-mapping.md) frames the non-goal.

## How it would hook into tpuf (sketch — full design in the next entry)

The change would be contained to the vector-index build/update path; the WAL, manifest-CAS, and epoch
machinery stay intact. The key insight is that LIRE maps cleanly onto our **epoch + CAS** model:

- **Incremental rebalance instead of full rebuild.** Per the non-goal's stated hook,
  [`internal/engine/indexer.go`](../../internal/engine/indexer.go) would gain `Split` / `Merge` /
  `Reassign` operating on the existing `CentroidsFile` / `ClusterFile` objects rather than re-running
  `KMeans` over everything. Deletes would become **tombstones** rather than forcing a rebuild (SPFresh's
  Insert/Delete are append + a version/tombstone, SPFresh §3.2).
- **Epoch swap is still the publish primitive.** Today every index object is written **write-once under a
  fresh `index/v{epoch}/` prefix** with the unconditional `putJSON`, and the new index goes live with a
  **single atomic manifest CAS** flipping `IndexEpoch` (`BuildIndex` in `indexer.go`; rules in
  [`../01-architecture.md`](../01-architecture.md) and CLAUDE.md). An incremental rebalance would still
  publish a new epoch — only the *minority* of postings touched by a split/merge need new objects, with
  unchanged `cluster-{i}.json` objects carried forward — and still go live via the one
  `SaveManifestCAS` flip. **CAS implication to get right:** because immutable index objects are content
  under an epoch prefix, even an "incremental" update is published as a new immutable epoch, not by
  mutating a live object in place; the WAL-tail overlay `[IndexedUpTo, WALSeq)` semantics
  ([`internal/engine/query.go`](../../internal/engine/query.go) `RunQuery`) are untouched.
- **NPA-on-rebuild is the property we'd be preserving cheaply.** A full rebuild re-establishes NPA for
  free; LIRE's whole point is to preserve that same NPA invariant *without* the rebuild. Any incremental
  hook must keep NPA as its correctness contract (SPFresh §3.2), or query recall silently rots — the very
  failure mode this entry documents.

## What's genuinely hard / what to get right

- **The honest framing: at our scale this buys nothing.** A full rebuild keeps the index perfectly
  balanced and NPA-correct every epoch, in milliseconds. LIRE optimizes a cost (rebuild time) that is
  only painful at billion scale (SPFresh §2.3, Table 1). Treat this as a *documented design*, exactly as
  [`../05-clone-mapping.md`](../05-clone-mapping.md) frames it — not an implementation to chase.
- **Balance and NPA are coupled, and both are easy to break.** Skipping the balance constraint
  reintroduces the long-tail-latency problem SPANN solved (SPANN §3.1, §3.2.1, Eq. 1); letting centroids
  go stale reintroduces the recall-decay problem SPFresh solved (SPFresh §2.3). An incremental design must
  hold **both** — that is precisely why LIRE's reassign step is derived from two formal necessary
  conditions rather than a heuristic (SPFresh §3.3).
- **Boundary recall.** Partial search already misses neighbors that fall just across a cluster boundary
  (SPANN §3.1, Figure 1); SPANN's mitigation is posting-list expansion / boundary replication (SPANN
  §3.2.2). Our clone notes boundary replication as *"noted, not built"*
  ([`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)); incremental updates that move vectors
  across boundaries make getting this right *more* necessary, not less.
- **Concurrency under split/reassign is the production-hard part.** SPFresh runs reassignment as a
  background, multi-threaded pipeline off the foreground update path and uses CAS on per-vector version
  numbers to make concurrent reassignment safe (SPFresh §4.1, §4.2.2). In our single-process clone the
  analogue is the manifest CAS + epoch-immutability model; reconciling LIRE's *in-place* posting mutation
  with our *write-once-per-epoch* object model is the main design tension to resolve in the next entry.

## Sources

- **SPANN: Highly-efficient Billion-scale Approximate Nearest Neighbor Search**, Chen et al., NeurIPS
  2021 — read locally at [`../papers/spann-neurips21.pdf`](../papers/spann-neurips21.pdf).
  - §1, §3 "Index structure" / "Partial search": in-memory centroids in SPTAG + on-disk posting lists;
    partial search loads the `K ≪ N` nearest postings. Abstract + §4.2.1: ~90% recall@1/@10 at billion
    scale, ~1 ms, 32 GB RAM, ~2× faster than DiskANN.
  - §3.1 + Figures 1–2: the three challenges — posting length limitation, boundary issue, diverse search
    difficulty (SIFT1M: 80% of queries need ~6 postings, 99% need ~114).
  - §3.2.1 + Eq. 1: balanced clustering minimizing posting-length variance; hierarchical balanced
    clustering. §3.2.2 + Eq. 2 + Figures 4–5: posting-list expansion / closure + RNG de-dup (≤8 replicas,
    §4.2). §3.2.3 + Eq. 3: query-aware dynamic pruning (ε₂≈0.6 recall@1, ≈7 recall@10, §4.2).
  - arXiv: <https://arxiv.org/abs/2111.08566> · MSR PDF:
    <https://www.microsoft.com/en-us/research/wp-content/uploads/2021/11/SPANN_finalversion1.pdf>
- **SPFresh: Incremental In-Place Update for Billion-Scale Vector Search**, Xu et al., SOSP 2023 —
  read locally at [`../papers/spfresh-sosp23.pdf`](../papers/spfresh-sosp23.pdf).
  - §1: cluster-based indices are cheaper to update than graph indices (constant local modification),
    but quality "deteriorates because the data distribution skews over time"; LIRE's intuition that a
    single update perturbs only a local region.
  - §2.3 "Freshness Demands and Challenges" + Table 1: naive/out-of-place updates degrade recall and tail
    latency (Vearch-on-SPANN: updating one-third of vectors degrades recall ">one point" and raises tail
    latency "4X"; "static centroids cannot capture the gradual distribution shift"); global rebuild costs
    (DiskANN 1B: 1100 GB DRAM / ~2 days, or ~5 days at 64 GB / 16 vCPUs). Figure 2: static vs in-place
    recall/latency.
  - §3.2 LIRE / NPA: "each vector should be put on the nearest posting"; Insert/Delete are append +
    version/tombstone. §3.3: the two formal necessary conditions for reassignment. §4.1, §4.2.2:
    background multi-threaded Local Rebuilder, per-vector version numbers, CAS for concurrent reassign.
    Abstract: <20 GB steady-state RAM, 2.41× lower tail latency than DiskANN, no global rebuild.
  - arXiv: <https://arxiv.org/abs/2410.14452> · ACM DOI: 10.1145/3600006.3613166
- **turbopuffer, "ANN v3: 200ms p99 query latency over 100 billion vectors"** —
  <https://turbopuffer.com/blog/ann-v3> (fetched and verified). Direct quote: *"Vector indexes in
  turbopuffer are based on SPFresh, a centroid-based approximate nearest neighbor index that supports
  incremental updates."* Also: *"We extended the SPTAG graph-based index described in the original SPFresh
  paper, nesting clusters hierarchically in a multi-dimensional tree structure"* and a *"100x branching
  factor between levels."*
  - ⚠️ **Flagged as turbopuffer's own claim, not an independently verified internal:** turbopuffer states
    its index is "based on SPFresh"; [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) carries
    this as *"derived from SPFresh."* turbopuffer does **not** publish the literal SPFresh
    split/merge/reassign code it runs, its on-disk posting format, or its exact rebalance thresholds — the
    *mechanism* this entry describes is the SPANN/SPFresh papers' design their index is "derived from,"
    not a claim about turbopuffer's exact implementation. See also
    [`../papers/SOURCES.md`](../papers/SOURCES.md) for turbopuffer figures we deliberately do **not**
    mirror as fact.
- **This clone's code & docs:** [`../../internal/engine/vector.go`](../../internal/engine/vector.go)
  (`ChooseK`, `KMeans`, `ResidualCode`), [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go)
  (`BuildIndex`, `buildVectorIndex`, `putJSON`),
  [`../../internal/engine/types.go`](../../internal/engine/types.go) (`CentroidsFile`, `ClusterFile`,
  `Manifest`), [`../../internal/engine/wal.go`](../../internal/engine/wal.go) (`AppendWAL`),
  [`../../internal/engine/namespace.go`](../../internal/engine/namespace.go) (`Namespace.Upsert`),
  [`../../internal/engine/query.go`](../../internal/engine/query.go) (`RunQuery`, WAL-tail overlay),
  [`../01-architecture.md`](../01-architecture.md), [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md),
  [`../05-clone-mapping.md`](../05-clone-mapping.md), [`../papers/SOURCES.md`](../papers/SOURCES.md).
</content>
</invoke>
