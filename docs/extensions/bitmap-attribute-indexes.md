# Extension — Bitmap attribute indexes and the filter planner

> **Implemented (2026-06-18).** The indexer writes `index/v{epoch}/bitmaps.json` and the query path has a
> filter-first vs search-first planner in `internal/engine/{bitmap,indexer,query}.go`; the result set is
> proven identical to the per-candidate path across selectivities, and `cmd/tpuf-bench --filter-plan`
> shows the cold-I/O win when selective. The text below is the design rationale; only equality
> (eq/and/or) is bitmap-indexed (no ranges/NOT), and the BM25 tail stays on per-candidate `Filter.Match`.

> One of the deliberate non-goals in [`../05-clone-mapping.md`](../05-clone-mapping.md): *"Bitmap/attribute
> indexes + filter-first/search-first planner — we evaluate filters per candidate. Hook: precompute
> `(attr_value, cluster) → roaring bitmap` in the indexer."* This doc expands that one line into a real
> design.

**What this is.** Today tpuf finds vector/BM25 candidates first and then tests each one against the filter
predicate, one document at a time (`Filter.Match` in [`../../internal/engine/types.go`](../../internal/engine/types.go),
called per-candidate in [`../../internal/engine/query.go`](../../internal/engine/query.go)). A *bitmap
attribute index* turns that around: at index-build time we precompute, for each attribute value, a
**compressed bitmap of the document ids that have it**. A query can then evaluate `category = "docs" AND
lang = "en"` as a single bitmap **intersection** over the whole corpus before it touches a vector — and,
critically, decide *whether a given cluster can contain any match at all* and skip it. turbopuffer calls
this **native filtering**: "the vector and the filtering indexes cooperate with each other," and the
attribute indexes "understand the clustering hierarchy" so the engine can "skip clusters" that hold no
matching document even when they are geometrically nearest to the query
([turbopuffer, *Native filtering*](https://turbopuffer.com/blog/native-filtering)).

This matters because the two naive strategies are both bad. **Post-filtering** (retrieve top-K by vector,
then drop the rows that fail the filter) silently loses recall: if the filter is selective, most of your
K nearest neighbours get discarded and you return far fewer than K real matches — turbopuffer reports this
as down to *0% recall* for selective filters. **Pre-filtering** done naively (scan everything that matches
the filter, then rank) is exact but can be *10+ seconds* on object storage. The bitmap-aware planner is
how you get both: turbopuffer reports ~90% recall at ~25 ms for filtered queries with native filtering
(numbers from [turbopuffer, *Native filtering*](https://turbopuffer.com/blog/native-filtering); these are
turbopuffer's own published figures for their production system, not measured on this clone).

## How it works

### Roaring bitmaps: the compressed set primitive

A bitmap (bitset) represents a set of integers — here, document ids mapped to a dense `[0, N)` id space —
as one bit per possible id. Set intersection / union / difference become hardware `AND` / `OR` / `ANDNOT`
over machine words, which is why bitmap indexes are the standard substrate for analytic filtering. The
problem is space: a plain bitmap costs `N/8` bytes regardless of how few ids are set, so a value held by 3
documents out of 10 million still costs ~1.2 MB.

**Roaring bitmaps** (Lemire et al.) solve that. They split the 32-bit id space into **chunks of 65536
integers**, and store each chunk in whichever of three *container* types is cheapest: "an uncompressed
bitmap, a simple list of integers, or a list of runs"
([roaringbitmap.org](https://roaringbitmap.org/about/)). The standard rule:

| Container | Used when a chunk holds… | Cost | Notes |
|---|---|---|---|
| **array** (sorted 16-bit list) | few ids (≤ 4096) | 2 bytes/id | sparse values stay tiny |
| **bitmap** (2¹⁶-bit dense) | many ids (> 4096) | fixed 8 KB | dense values |
| **run** (RLE of `[start,length]`) | long contiguous spans | ~4 bytes/run | added in the optimized lib |

The 4096 array↔bitmap threshold is the point where 2 bytes/id stops beating a fixed 8 KB bitmap; the
selection and the vectorized AND/OR/ANDNOT across containers are the contribution of the implementation
paper ([Lemire et al. 2018, *SPE* 48(4)](https://onlinelibrary.wiley.com/doi/10.1002/spe.2560)). Set
operations stay correct because two roaring bitmaps are intersected *container-by-container* keyed on the
high 16 bits, picking the right algorithm per container pair. Roaring bitmaps "tend to outperform
conventional compressed bitmaps such as WAH, EWAH or Concise"
([roaringbitmap.org](https://roaringbitmap.org/about/)) — the 2016 design paper reports them both
compressing better and running "up to 900 times faster for intersections" than those alternatives
([Chambi, Lemire, Kaser, Godin 2016, *SPE* 46(5)](https://lemire.me/en/publication/arxiv14026407/)) — and
Roaring is the bitmap index in Lucene, Spark, Druid, ClickHouse and others
([roaringbitmap.org](https://roaringbitmap.org/about/)).

> Our clone keeps exactly one external dependency (`aws-sdk-go-v2/service/s3`; see `CLAUDE.md`). A faithful
> implementation here would **hand-write a small roaring-style bitmap** (array + bitmap containers; runs
> optional) rather than import `RoaringBitmap/roaring` — same discipline as the hand-written k-means,
> RaBitQ-lite and BM25.

### Two granularities of index (this is the key idea)

turbopuffer maintains attribute indexes at **two levels** — *row-level* and *cluster-level* — and
addresses every document as **`{cluster_id}:{local_id}`** so the attribute index can survive the cluster
rebalancing that SPFresh performs ([turbopuffer, *Native filtering*](https://turbopuffer.com/blog/native-filtering)).
The cluster-level index is what unlocks pruning:

- **Cluster-level (coarse):** for each `(field, value)`, a bitmap *over cluster ids* — "does cluster `c`
  contain at least one document with `field = value`?" Tiny. Lets the planner answer "can this cluster
  possibly match the filter?" with **no document fetch**.
- **Row-level (fine):** for each `(field, value)`, a bitmap over the `local_id`s *within a cluster*. Used
  to compute the exact surviving set once a cluster is actually fetched.

turbopuffer describes the query as a "two-step process": "bitmap unions and intersections on the cluster
level before fetching exact bitmaps"
([turbopuffer, *Native filtering*](https://turbopuffer.com/blog/native-filtering)).

### The filter-first / search-first planner

With those bitmaps the engine can *choose a plan* per query based on how selective the filter is — the
classic filter-and-refine tradeoff:

```
filter_set := evaluate(filter)        // a roaring bitmap of matching cluster-ids (coarse)
selectivity := popcount(filter_set) / total

if selectivity is very low (filter matches almost nothing):
    # FILTER-FIRST (pre-filter): the filter is the cheap part.
    candidate_clusters := clusters touched by filter_set
    rank vectors only inside those clusters
else:
    # SEARCH-FIRST (ANN-first), filter-aware:
    probe_order := clusters by centroid distance to query   # as today
    for each probed cluster, in order:
        if cluster_bitmap AND filter_set == empty:  skip it    # the prune
        else: fetch it, AND row-level filter bitmaps, rank survivors
    keep probing until K filtered results found (probe expansion)
```

The non-obvious half is the `else` branch: a pure search-first scan with post-filtering returns too few
rows when the filter is selective, so a filter-aware search-first plan must be willing to **probe more
clusters than the default `nProbe`** until it has collected K *surviving* candidates. The cluster-level
bitmap makes that cheap — you skip the clusters that can't contribute without paying to read them. SPANN's
own *query-aware dynamic pruning* is the same shape applied to geometry: fetch a posting list only if its
centroid is within a relative factor of the closest, because "80% of queries need ~6 lists but 99% need
~114" ([`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md); SPANN, NeurIPS '21,
`../papers/spann-neurips21.pdf` §query). Bitmap pruning extends that "stop early / expand as needed" logic
from distance to attribute predicates.

## What our clone does today, and the gap

tpuf evaluates filters **per candidate, after retrieval**. In
[`../../internal/engine/query.go`](../../internal/engine/query.go):

- `runVectorQuery` builds a `dists`/`attrs` map from the probed clusters and WAL tail, then loops
  `for id, dist := range dists { if !p.Filter.Match(attrs[id]) { continue } ... }`.
- `runBM25Query` does the identical `if !p.Filter.Match(attrs[id])` skip over its scored ids.

`Filter` itself ([`../../internal/engine/types.go`](../../internal/engine/types.go)) is a recursive tagged
union (`Op` ∈ `""`/`eq`/`and`/`or`) and `Filter.Match(attrs map[string]any) bool` walks it against one
document's attributes. There is **no index** behind it — every candidate is tested, and crucially the
filter only ever runs on documents we *already fetched*. That is exactly the simplification recorded in the
mapping table: *"Metadata filtering … Eq + And/Or over attributes, evaluated per candidate … map eval, not
bitmap indexes"* ([`../05-clone-mapping.md`](../05-clone-mapping.md)).

Two concrete consequences this extension would fix:

1. **No cluster skipping.** `vectorCandidatesFromIndex` fetches every probed `cluster-{i}.json` and reranks
   its members regardless of the filter; a cluster where *zero* documents match `lang = "en"` is still read
   from object storage and scanned. The filter can only shrink the result *after* the I/O is paid.
2. **Effective recall collapses under selective filters.** Because we keep the top `topK *
   shortlistMultiplier` by RaBitQ-lite agreement *before* the filter runs (see the shortlist logic in
   `vectorCandidatesFromIndex`), a selective filter can wipe out the whole shortlist and leave us returning
   far fewer than `topK` true matches — the post-filtering recall trap, in miniature.

## How it would hook into tpuf

The index is already an immutable, per-epoch snapshot published by one manifest CAS — the perfect place to
materialize derived bitmaps. Nothing about the CAS/epoch model needs to change; we add new write-once
objects under the epoch prefix and one new read path.

**1. Build the bitmaps in the indexer.**
[`../../internal/engine/indexer.go`](../../internal/engine/indexer.go) already materializes the live
document set and writes `centroids.json`, `cluster-{i}.json`, `bm25.json`, `docs.json` under
`index/v{epoch}/` (`indexPrefix`). Add a `buildAttributeIndex(...)` step alongside `buildVectorIndex` that
walks the same `live map[string]Document`, and a new file:

- `index/v{epoch}/attrs.json` (a new `attrsKey(ns, epoch)` next to the existing `centroidsKey` /
  `bm25Key` / `docsKey` helpers) holding, per `(field, value)`:
  - a **cluster-level** roaring bitmap over the IVF cluster ids (built from the `assign[]` array that
    `buildVectorIndex` already computes when bucketing members), and
  - **row-level** bitmaps over each cluster's local member index.

Because indexer output is write-once under a fresh epoch prefix, `attrs.json` is written with the same
unconditional `putJSON` the other index files use — no extra CAS, and it goes live atomically with the rest
of the epoch at the existing single `SaveManifestCAS` that flips `IndexEpoch` (correctness rule 4). A
roaring bitmap serializes to a compact byte blob; storing it as base64 inside the JSON file (or as a
sibling `.bin` object) keeps the one-dependency rule intact.

> **Build-time only — interaction with `--text-field`.** The attribute index is built from
> `Document.Attributes`, the same source `buildBM25Index`/`writeDocs` already read; the filterable fields
> are simply the non-text attributes. No new `Document`/`Manifest` field is strictly required, though a
> `Manifest` note of which fields are indexed would let the planner fail fast on an unindexed field.

**2. Use them in the query planner.** In `runVectorQuery`
([`../../internal/engine/query.go`](../../internal/engine/query.go)), before the
`vectorCandidatesFromIndex` call:

- Compile `p.Filter` into a roaring bitmap by recursing the existing `Op` union: `eq` → look up the
  `(field, value)` bitmap; `and` → intersect children; `or` → union children; `""` → the all-ones "match
  everything" set (so an empty filter is a no-op and today's behaviour is preserved exactly).
- Pass the resulting **cluster-level** bitmap into `vectorCandidatesFromIndex` and, inside its
  `for _, c := range probes` loop, `continue` past any cluster whose bit is unset in the filter set — the
  prune.
- If the filter is highly selective, switch to the **filter-first** plan: derive the candidate clusters
  directly from the filter bitmap instead of from `nearestClusters`, and/or keep expanding `nProbe` until
  `topK` *post-filter* survivors are found.

The same compiled bitmap drops into `runBM25Query` to prune by intersecting against the posting-list hits.

**3. The WAL tail still needs `Filter.Match`.** This is the important honesty point. Queries union the
indexed epoch with the unindexed WAL tail `[IndexedUpTo, WALSeq)` via `MaterializeLiveAndDeleted`
([`../../internal/engine/wal.go`](../../internal/engine/wal.go)) so fresh writes are searchable before
they're indexed ([`../01-architecture.md`](../01-architecture.md)). Those tail documents have **no bitmap**
yet — they aren't in any epoch — so the per-document `Filter.Match` path stays exactly as it is for the
tail. `Filter.Match` is not replaced; it becomes the *small-N tail* evaluator while the bitmap index is the
*large-N indexed* evaluator. (turbopuffer reconciles this at scale with the `{cluster_id}:{local_id}`
addressing so bitmaps survive rebalancing; in our rebuild-per-epoch clone, the tail is simply the
not-yet-indexed remainder, and every `tpuf index` run rebuilds the bitmaps from scratch — consistent with
the "we rebuild rather than incrementally update" choice in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md).)

## What's genuinely hard / what to get right

- **Stable, dense id space.** Roaring indexes integers; document ids in tpuf are strings. The indexer must
  assign each live document a dense `[0, N)` ordinal (and, for the row-level index, a `local_id` within its
  cluster) and persist the mapping so query results can translate bitmap bits back to string ids and the
  `docs.json` / `ClusterEntry.Attrs` payloads. turbopuffer's `{cluster_id}:{local_id}` scheme exists
  precisely to make this addressing stable across cluster moves
  ([turbopuffer, *Native filtering*](https://turbopuffer.com/blog/native-filtering)); our per-epoch rebuild
  sidesteps the *moving* problem but still has to pick and record the ordinals.
- **Choosing the plan.** Filter-first vs search-first hinges on a *selectivity estimate*, and `popcount` of
  the bitmap gives it almost for free — but pick wrong (search-first on an ultra-selective filter) and you
  either under-probe (low recall) or scan everything (slow). A safe v1: always prune in search-first, and
  only flip to filter-first below a hard selectivity threshold.
- **Correlation between the filter and the vector space.** Cluster pruning shines when matching documents
  are *concentrated* in a few clusters and hurts (forces wide probe expansion) when a selective filter is
  *spread thin* across many clusters — the worst case for any partition-based ANN index under filtering.
  This is inherent, not a bug; it's why the planner must be allowed to expand probes.
- **Only equality is supported today.** `Filter` is `eq`/`and`/`or` only — no ranges, no `not`. Range
  predicates (`price < 10`) need either many per-value bitmaps unioned or a different structure
  (sorted/bit-sliced index); document the limitation rather than pretend `eq` bitmaps cover it.
- **High-cardinality fields.** A unique-per-document field (e.g. a UUID) produces N singleton bitmaps —
  pure overhead, no pruning value. A real implementation would skip indexing fields above a cardinality
  threshold; for the clone, indexing only low-cardinality categorical fields is the honest scope.
- **The dependency rule.** Importing `RoaringBitmap/roaring` would be the easy path and the wrong one for
  this project. The interesting, in-scope work is a minimal hand-written container set (array + bitmap;
  runs optional), matching how every other "conceptually interesting" piece in this clone is hand-written
  (`CLAUDE.md`).

## Sources

- **Roaring bitmaps — overview & users.** roaringbitmap.org/about — confirms the 65536-integer chunking and
  the three container types ("an uncompressed bitmap, a simple list of integers, or a list of runs"), that
  intersections/unions/differences are bitwise AND/OR/ANDNOT, and the adopter list (Lucene, Spark, Druid,
  ClickHouse, Elastic, Pinot, Netflix Atlas): <https://roaringbitmap.org/about/>
- **Chambi, Lemire, Kaser, Godin (2016).** "Better bitmap performance with Roaring bitmaps." *Software:
  Practice and Experience* 46(5), 709–719. The original Roaring design (packed arrays vs bitmaps;
  outperforms WAH/EWAH/Concise): <https://lemire.me/en/publication/arxiv14026407/> (arXiv:1402.6407).
- **Lemire, Kaser, Kurz, Deri, O'Hara, Saint-Jacques, Ssi-Yan-Kai (2018).** "Roaring Bitmaps:
  Implementation of an Optimized Software Library." *Software: Practice and Experience* 48(4). The
  optimized library, run containers, and vectorized set operations. DOI
  [10.1002/spe.2560](https://onlinelibrary.wiley.com/doi/10.1002/spe.2560) · arXiv:
  <https://arxiv.org/abs/1709.07821>. *(The 4096 array↔bitmap threshold is a documented implementation
  detail of this library; roaringbitmap.org states the container types but not the literal threshold —
  flagged here as implementation lore, not a verbatim quote.)*
- **turbopuffer — *Native filtering*.** The source for the *filter-first vs search-first* framing,
  cluster-level vs row-level attribute bitmaps, the `{cluster_id}:{local_id}` addressing, "skip clusters,"
  the "bitmap unions and intersections on the cluster level before fetching exact bitmaps" two-step, and the
  ~90% recall / ~25 ms vs 0% / 10+ s figures: <https://turbopuffer.com/blog/native-filtering>. *(These
  recall/latency numbers are turbopuffer's own published figures for their production engine; turbopuffer
  does not publish the bitmap library, container thresholds, or file layout it uses internally — those are
  not asserted here.)*
- **SPANN — query-aware dynamic pruning** (the geometric analogue of bitmap probe expansion): Chen et al.,
  *SPANN*, NeurIPS '21 — `../papers/spann-neurips21.pdf` (query section); summarized with the
  "80% of queries need ~6 lists, 99% need ~114" motivation in
  [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md).
- **This clone's current behaviour:** per-candidate filtering in
  [`../../internal/engine/query.go`](../../internal/engine/query.go) (`Filter.Match` calls in
  `runVectorQuery`/`runBM25Query`); the `Filter` tagged union and `Match` in
  [`../../internal/engine/types.go`](../../internal/engine/types.go); index build/epoch-swap in
  [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go); WAL-tail overlay in
  [`../../internal/engine/wal.go`](../../internal/engine/wal.go); and the non-goal + hook recorded in
  [`../05-clone-mapping.md`](../05-clone-mapping.md).
</content>
</invoke>
