# Extension — Hierarchical (multi-level) centroid index

> **Implemented (2026-06-18).** Recursive k-means levels are stored in `centroids.json` and traversed
> top-down at query time (`internal/engine/{vector,indexer,query}.go`), with a query returning the same
> top-K as the flat index. Honest finding: at this clone's N the measured fan-out shows the tree is
> equal-or-slower than the flat scan — it is built to *show the shape*, not for a speedup. The text below
> is the design rationale.

> One of the deliberate non-goals in [`../05-clone-mapping.md`](../05-clone-mapping.md): *"Hierarchical
> 100×-fan-out centroid tree — we use one flat centroid level. **Hook:** recurse the clustering in
> `vector.go` and store levels."* This doc expands that hook into a real design entry.

A flat IVF index (one level of centroids) answers a vector query in two steps: score the query against
**every** centroid, then fetch the few nearest clusters. That is exactly what our clone does today, and
it is the right call at demo scale. But "score against every centroid" is `O(K)` per query, and the
whole reason a centroid index beats a graph index on object storage is that it keeps the per-query work
small and the number of **dependent** S3 round-trips tiny (see [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md):
~2 dependent steps vs ~20 for HNSW). At billions of vectors a single flat level breaks that promise:
the centroid set itself becomes too large to scan cheaply or to keep resident. The fix is to make the
centroid set *itself* a centroid index — recursively — producing a **tree of centroids** whose leaves
are the posting lists (clusters of real vectors). turbopuffer's current vector index ("ANN v3") is
exactly this: a wide, shallow centroid tree. This is a large-`N` optimization and would be purely
pedagogical at our scale.

## How it works

### The scaling problem with one flat level

Pick `K` clusters over `N` vectors. Two costs pull in opposite directions:

| If `K` is… | Centroid scan (in memory) | Cluster fetch (from S3) |
|---|---|---|
| small (few big clusters) | cheap — few centroids to score | expensive — each cluster is huge, so you read a lot of irrelevant vectors |
| large (many small clusters) | expensive — scoring `K` centroids dominates | cheap — each cluster is small and on-point |

SPANN's own ablation lands on `K ≈ 16%` of the dataset to balance these (1M vectors → ~160K posting
lists), tuned down to ~10–12% at billion scale so the centroid index still fits the memory budget
(SPANN §4.2.3 Ablation studies, "Hierarchical balanced clustering": "we can choose 16% of points as the
centroids"; the ~10–12% billion-scale figure is from §4.2.1). At `N = 10⁹` that is ~100–160M centroids
— far too many to scan linearly per query, and the in-memory centroid index that SPANN keeps in SPTAG
(SPANN §3.2.1) stops being cheap. The flat design has run out of road.

### Recurse: the centroid set becomes its own index

If scanning `K` centroids is too expensive, cluster the centroids too, and keep going until the top is
small enough to scan exhaustively. You get a tree:

```
            root (1 node, scanned exhaustively)
           /    |    \                                  level 0  — fan-out F
        c       c       c        ...                    level 1  — ~F   centroids
      / | \   / | \   / | \                             level 2  — ~F²  centroids
     …   …   …   …   …   …   …                           level 3  — ~F³  centroids
    [leaf posting lists: the actual data vectors]       leaves   — ~N/L  clusters, L vectors each
```

- **Internal levels** hold *centroids of centroids*. They are small and stay resident in fast memory.
- **Leaf level** holds the posting lists — clusters of the real document vectors — on object storage,
  fetched only when a query routes into them.

**Search — beam descent, not a flat scan.** At each level keep the `b` nearest nodes (a beam), descend
into their children, repeat. Cost per level is `O(b · F)` distance computations; total query work is
`O(b · F · height)` instead of `O(K)`. Crucially the number of *dependent* S3 round-trips is bounded by
the **tree height**, not by how many leaves you eventually read — the upper levels are in memory, so
descending them is free of S3 latency, and the leaf fetches are the only object-storage reads.

```
# query(q, topK):
beam = [root]
for level in 0 .. height-1:
    cand = children(beam)                       # in-memory until the leaf level
    beam = topB(cand by Dist(q, node.centroid))  # keep b nearest
leaves = beam                                    # the posting lists to fetch
vecs   = fetch(leaves)                           # the only S3 round-trip(s)
return rerank(q, vecs)[:topK]                    # exact distance, top-K
```

**Build — recursive balanced clustering.** This is SPANN's *hierarchical balanced clustering* (HBC),
SPANN §3.2.1 and Figure 3: "iteratively balanced partition the vectors in a large cluster … into a
small number of small clusters … until each cluster only contains [a] limit number of vectors." SPANN
recurses with a small branching constant `k` per split, which "reduce[s] the clustering time complexity
from `O(|X|·m·N)` to `O(|X|·m·k·logₖ(N))`" (SPANN §3.2.1). A leaf is sealed once it is under a **posting
length cap** — SPANN uses ~12 KB for byte vectors and ~48 KB for float vectors (SPANN §4.2), i.e. each
posting stays loadable in a few disk reads.

```
# build_tree(vectors, cap):
if size(vectors) <= cap:
    return Leaf(vectors)                          # a posting list
centroids, members = kmeans(vectors, k=F)         # one balanced split
children = [build_tree(members[i], cap) for i in 0..F)]
return Node(centroids, children)                  # store this level's centroids
```

### turbopuffer's reported design (ANN v3)

turbopuffer publicly describes their production vector index as a centroid **tree**, derived from
SPFresh/SPANN (see [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) §"How turbopuffer
describes their version"). Their stated parameters, from the ANN v3 blog:

- **Fan-out ≈ 100× per level** — "Each node in the tree has approximately 100 children, creating a wide,
  shallow tree structure." The 100× is chosen to "roughly match the size ratio between DRAM and SSD,"
  so if all data vectors fit on SSD, all centroid vectors fit in DRAM.
- **Upper levels resident in L3 cache** — "all three upper levels of the tree in L3 cache, the largest
  requiring `100³ × 128 = 128 MiB`." (This is the source of the often-mis-quoted "128 MiB" number; it
  is the cached upper-tree size, **not** a WAL-reindex threshold — see
  [`../papers/SOURCES.md`](../papers/SOURCES.md) and [`../01-architecture.md`](../01-architecture.md).)
- **Leaf clusters ≈ 100 vectors each**; a 100B-vector query "needed to search about 500 data vector
  clusters (each 100 vectors large) on each machine," fetching `500 clusters × 100 vectors/cluster ×
  1024 dimensions/vector × 2 bytes/dimension = 100MB per level` — "equating to a bandwidth requirement of
  `500 x 100 x 1024 x 2 / 16 = 6MB` per level of the tree" once the vectors are binary-quantized (16×
  smaller).
- **Height bounds the cold S3 round-trips:** the blog states "the hierarchy bounds the number of
  round-trips to object storage to the height of the SPFresh tree" (the tree height, not the leaf
  count). The blog does *not* state a literal round-trip count or per-round-trip latency; a height of
  ~3–4 is inferred from the three cached upper levels above, and ~50–500 ms is object storage's cold
  read latency per [`../01-architecture.md`](../01-architecture.md) — neither number is asserted by the
  ANN v3 blog.
- **Headline:** "200ms p99 query latency over 100 billion vectors." The blog itself does **not** state a
  recall percentage; the recall figures are sourced separately (see the flag below).

> ⚠️ **On recall:** the ANN v3 *blog* states **no recall percentage** (its only quantitative recall
> remark is "less than 1% of data vectors in the narrowed search space need to be reranked to avoid an
> impact on recall"). The **"92% recall@10"** figure carried in
> [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) comes from turbopuffer's ANN v3 launch
> post on X — "v3 in beta, unfiltered search, 1024D, k=10, 92% recall." Separately, turbopuffer's
> *continuous-recall* page states a **target** of "90-95% recall@10 for all queries" and reports
> average recall@10 "strictly above 90%" across their largest customers. So treat 92% as the v3-beta
> launch number (unfiltered, 1024-dim) and 90–95% as turbopuffer's stated target range — not a single
> blog-asserted fact. The fan-out, L3 sizing, leaf size, per-level bandwidth, and 200ms p99 figures
> above are quoted directly from turbopuffer's ANN v3 blog. turbopuffer does not publish the exact
> internal split/rebalance algorithm or on-disk node format; the *mechanism* here (beam descent,
> recursive HBC) is the SPANN/SPFresh design their index is "derived from," not a claim about
> turbopuffer's exact code.

## What our clone does today (and the gap)

We build **one flat level**. In [`../../internal/engine/vector.go`](../../internal/engine/vector.go):

- `ChooseK(n int)` returns `K ≈ round(√n)` — a standard small-scale IVF heuristic, *not* SPANN's 16%
  rule and not a hierarchy.
- `KMeans(points, k, metric, iters)` runs a single pass of Lloyd's algorithm and returns
  `(centroids [][]float32, assign []int)` — one flat partition, no recursion.

In [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go), `buildVectorIndex` calls
`ChooseK` then `KMeans` once, buckets each document into its assigned cluster (computing a RaBitQ-lite
`ResidualCode`), and writes one `centroids.json` plus one `cluster-{i}.json` per cluster. The on-disk
shapes are `CentroidsFile` and `ClusterFile`/`ClusterEntry` in
[`../../internal/engine/types.go`](../../internal/engine/types.go) — note `CentroidsFile` holds a flat
`Centroids [][]float32`, with no notion of parent/child levels.

The query side (`internal/engine/query.go`, planner described in
[`../01-architecture.md`](../01-architecture.md)) scores the query against **all** centroids and probes
the top `nProbe` (default 3, per [`../05-clone-mapping.md`](../05-clone-mapping.md)). That `O(K)`
centroid scan is the exact step a tree would replace.

**The gap:** there is no tree — no internal centroid levels, no beam descent. At our scale (`K ≈ √N`,
e.g. ~32 centroids for 1000 docs) a flat scan is trivially fast and a tree would add code and slowness
for zero benefit. The hierarchy earns its complexity only when `K` itself grows into the millions.

## How it would hook into tpuf

The change is contained to the build and query of the vector index; the WAL, manifest-CAS, and epoch
machinery are untouched.

1. **`vector.go` — recurse the clustering.** Add a tree builder beside `KMeans`, e.g.
   `BuildCentroidTree(points [][]float32, fanout, capacity int, metric string) *CentroidNode`, that
   splits with `kmeans(..., k=fanout)` and recurses on each child bucket until a bucket is under
   `capacity` (the leaf becomes a posting list). The leaf-distance, residual-code, and `Hamming`/
   `Agreement` helpers are reused unchanged. `ChooseK` would be replaced (at large `N`) by a
   fanout/capacity pair rather than `√N`.

2. **`types.go` — store levels, not a flat list.** Extend `CentroidsFile` (or add a
   `CentroidTreeFile`) describing internal nodes as `{centroid, childCluster|childNodeIDs}`, while the
   leaf payload stays the existing `ClusterFile` / `ClusterEntry`. Keep it JSON for readability,
   matching the current index objects.

3. **`indexer.go` — write the tree under the epoch prefix.** `buildVectorIndex` would persist the
   internal levels (small — e.g. an `index/v{epoch}/centroid-tree.json`) plus the same per-leaf
   `cluster-{i}.json` files it writes today. **This is the important CAS/epoch property:** every index
   object is still **write-once under a fresh `index/v{epoch}/` prefix**, written with the unconditional
   `putJSON` (see `indexer.go`'s comment on `putJSON` and `BuildIndex`'s rule-3/rule-4 notes). The tree
   is just more immutable objects under that prefix; the index still goes live with the **single atomic
   manifest CAS** that flips `IndexEpoch`. No new coordination, no change to `IndexedUpTo` / WAL-tail
   semantics — a query still overlays the unindexed WAL tail `[IndexedUpTo, WALSeq)` exactly as before
   ([`../05-clone-mapping.md`](../05-clone-mapping.md), [`../01-architecture.md`](../01-architecture.md)).

4. **`query.go` — beam descent instead of flat scan.** Replace "score query vs all centroids → top
   `nProbe`" with: load the small internal tree (cacheable in the DRAM tier like any write-once epoch
   object), beam-descend to the leaf set, then fetch only those `cluster-{i}.json` leaves and run the
   existing RaBitQ-lite prefilter → exact rerank → WAL-tail union → filter → top-K. `nProbe` generalizes
   to a beam width `b` and a leaf budget.

## What's genuinely hard / what to get right

- **It buys nothing at our scale — and that is the honest framing.** With `K ≈ √N` the flat scan is a
  handful of dot products. A tree adds build recursion, a multi-level on-disk format, and beam-descent
  query code to optimize a cost that is already negligible. Treat this as a *documented design*, not an
  implementation to chase, exactly as [`../05-clone-mapping.md`](../05-clone-mapping.md) frames it.
- **Balance is the whole point.** A naïve recursive k-means produces wildly uneven leaf sizes, which is
  precisely the "posting length limitation" SPANN calls out (SPANN §3.1, §3.2.1): unbalanced postings
  give high-variance query latency. SPANN's answer is multi-constraint **balanced** clustering plus a
  hard posting-length cap (SPANN §3.2.1, Eq. 1). Skipping balance reintroduces the problem the tree was
  meant to solve.
- **Boundary recall degrades with depth.** A vector near a split boundary can be routed away from its
  true nearest leaf, and the error compounds at every level — SPANN's Figure 1 / §3.1 "boundary issue."
  SPANN's mitigation is **posting-list expansion**: replicate a boundary vector into up to 8 nearby
  leaves under a closure rule, de-duplicated by an RNG rule (SPANN §3.2.2, Eqs. 2). Our clone already
  notes boundary replication as "noted, not built" ([`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md));
  a tree makes it more necessary, not less.
- **Beam width vs. recall vs. cost.** A too-narrow beam prunes the correct subtree early (recall loss);
  too wide and you forfeit the speedup. SPANN's *query-aware dynamic pruning* (SPANN §3.2.3, Eq. 3 —
  fetch a posting only if `Dist(q, cᵢ) ≤ (1+ε₂)·Dist(q, c₁)`) is the principled per-query analogue and
  the right thing to port if recall matters, rather than a fixed beam.
- **Updates get harder.** We rebuild the index per `tpuf index` run, which sidesteps this. A real tree
  drifts under inserts/deletes and wants SPFresh's LIRE split/merge/reassign to stay balanced without a
  global rebuild ([`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) §SPFresh) — a separate
  non-goal in [`../05-clone-mapping.md`](../05-clone-mapping.md). Combining a tree *and* incremental
  rebalancing is where production complexity actually lives.

## Sources

- **SPANN: Highly-efficient Billion-scale Approximate Nearest Neighbor Search**, Chen et al., NeurIPS
  2021 — read locally at [`../papers/spann-neurips21.pdf`](../papers/spann-neurips21.pdf).
  - §3.2.1 "Posting length limitation" + Figure 3: hierarchical balanced clustering recurses k-means
    until each cluster is under a length limit; complexity `O(|X|·m·k·logₖ(N))`; in-memory SPTAG
    centroid index.
  - §3.1 + Figure 1: posting-length-limitation and boundary issues. §3.2.2 + Eq. 2 + Figures 4–5:
    posting-list expansion / closure assignment / RNG de-dup (up to 8 replicas, §4.2). §3.2.3 + Eq. 3:
    query-aware dynamic pruning. §4.2: posting-length caps ~12 KB (byte) / ~48 KB (float), up to 8
    closure replicas, ε₁=10, ε₂=0.6 (recall@1) / 7.0 (recall@10). §4.2.1: ~10–12% centroid ratio at
    billion scale (≈32 GB for SIFT1B). §4.2.3 Ablation studies, "Hierarchical balanced clustering":
    ~16% centroid ratio, 1M → 160K clusters in ~50 s.
  - arXiv: <https://arxiv.org/abs/2111.08566> · MSR PDF:
    <https://www.microsoft.com/en-us/research/wp-content/uploads/2021/11/SPANN_finalversion1.pdf>
- **SPFresh: Incremental In-Place Update for Billion-Scale Vector Search**, Xu et al., SOSP 2023 —
  [`../papers/spfresh-sosp23.pdf`](../papers/spfresh-sosp23.pdf). Cited for why a static tree needs LIRE
  (split/merge/reassign) under updates; details in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md).
  arXiv: <https://arxiv.org/abs/2410.14452>.
- **turbopuffer, "ANN v3: 200ms p99 query latency over 100 billion vectors"** —
  <https://turbopuffer.com/blog/ann-v3> (fetched and re-verified). Direct quotes used: "Each node in the
  tree has approximately 100 children, creating a wide, shallow tree structure"; "This branching factor
  roughly matches the size ratio between DRAM and SSD (10x - 50x)"; "we can fit all three upper levels
  of the tree in L3 cache, the largest requiring `100^3 * 128 = 128 MiB`"; "about 500 data vector
  clusters (each 100 vectors large)"; "500 clusters x 100 vectors/cluster x 1024 dimensions/vector x 2
  bytes/dimension = 100MB per level" and "`500 x 100 x 1024 x 2 / 16 = 6MB` per level of the tree"; "the
  hierarchy bounds the number of round-trips to object storage to the height of the SPFresh tree";
  "200ms p99 query latency over 100 billion vectors."
  - ⚠️ **Flagged — recall is NOT in this blog.** The ANN v3 blog states no recall percentage (only
    "less than 1% of data vectors in the narrowed search space need to be reranked"). The **"92%
    recall@10"** in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) is from turbopuffer's
    ANN v3 launch post on X (<https://x.com/turbopuffer/status/1978173877571441135> — "v3 in beta,
    unfiltered search, 1024D, k=10, 92% recall"). The **"90-95% recall@10"** range is turbopuffer's
    stated *target* on their continuous-recall page (<https://turbopuffer.com/blog/continuous-recall>),
    not a v3-specific measurement. turbopuffer does **not** publish its exact internal node format or
    rebalance algorithm; the build/search *mechanism* here is the SPANN/SPFresh design their index is
    "derived from," not turbopuffer's literal implementation.
- **This clone's code & docs:** [`../../internal/engine/vector.go`](../../internal/engine/vector.go)
  (`ChooseK`, `KMeans`, `ResidualCode`, `Hamming`, `Agreement`),
  [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go) (`BuildIndex`,
  `buildVectorIndex`, `putJSON`),
  [`../../internal/engine/types.go`](../../internal/engine/types.go) (`CentroidsFile`, `ClusterFile`,
  `ClusterEntry`, `Manifest`), [`../01-architecture.md`](../01-architecture.md),
  [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md),
  [`../05-clone-mapping.md`](../05-clone-mapping.md), [`../papers/SOURCES.md`](../papers/SOURCES.md).
