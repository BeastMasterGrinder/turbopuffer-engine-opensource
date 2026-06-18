# 01 — The LIRE incremental-rebalancing protocol (split / merge / reassign)

LIRE is the core contribution of **SPFresh** (SOSP '23). It is the mechanism that lets a centroid /
cluster-based ("IVF") vector index — the same family turbopuffer's index is *derived from* (see
[`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)) — absorb a continuous stream of inserts
and deletes **in place**, with only tiny *local* rewrites, instead of periodically rebuilding the whole
index. The paper's name expands to **L**ightweight **I**ncremental **RE**-balancing (the letters are
underlined exactly that way in the paper title of §3.2, "LIRE: <u>L</u>ightweight <u>I</u>ncremental
<u>RE</u>-balancing"). turbopuffer cares about this because object storage is the source of truth and a
global rebuild of a billion-vector index is brutally expensive — DiskANN's rebuild needs **1100 GB of
DRAM for 2 days, or 64 GB for 5 days** (SPFresh Table 1), and the data distribution drifts continuously
as you write. LIRE keeps a *well-partitioned* index well-partitioned without ever rebuilding it.

> Scope note: this clone deliberately does **not** implement LIRE — it rebuilds the index per `tpuf
> index` run (see [`../05-clone-mapping.md`](../05-clone-mapping.md), "SPFresh LIRE incremental updates …
> we rebuild the index per `index` run"). This doc explains the protocol and exactly where it *would*
> hook into the real code so the gap is precise rather than hand-wavy.

## What problem LIRE solves

A cluster-based index assigns each vector to its nearest posting (partition) centroid; a query finds the
nearest centroids and scans only those postings. This is cheap to update locally — an insert or delete
touches one posting — but the paper's §2.3 ("Freshness Demands and Challenges") observes the catch:
**as updates accumulate, the data distribution skews, postings become unbalanced, and both query latency
and accuracy degrade.** SPFresh §2.3 reproduces this with Vearch-style in-place updates on SPANN
(Figure 2): updating one-third of the vectors degrades recall and **increases tail latency by ~4×**,
because the fixed centroids no longer match the shifted data. Existing systems therefore fall back to
out-of-place updates with **periodic global rebuilds** (§2.3) — which is exactly the expense LIRE avoids.

The key rationale (§3, restating SPANN's balance property, §3.1): a single vector update to a
*well-balanced* index "may only incur changes in a local region. Because the updates are small, the
corresponding changes are most likely to be limited in a local region. This makes the entire rebalancing
process lightweight and affordable" (§2, intro to LIRE, and §3.1).

## NPA — Nearest Partition Assignment (the invariant LIRE preserves)

The governing property is **NPA, "nearest partition assignment"** (§3.2): *"each vector should be put on
the nearest posting so that it can be well represented by the posting centroid."* SPFresh §3.4 makes this
precise with the index state:

- `C` : the set of posting centroids (Eq. 3).
- `M` : vector → centroid membership (Eq. 4).

…and the crucial fact (§3.4): *"given `C`, each vector in `M` is assigned to its nearest centroid in `C`,
i.e., `M` is uniquely determined by `C`."* So membership is a pure function of the centroids — LIRE never
has to track membership separately; it only has to keep every vector sitting in the posting whose
centroid is genuinely nearest. A *split* or *merge* changes `C`, which can silently make some nearby
vectors violate NPA; the job of **Reassign** is to repair exactly those.

## The five operations

LIRE's protocol is built from five basic operations (§3.2). Two are user-facing; three are internal and
"oblivious to users" (§3.2):

| Op | Trigger | What it does | Source |
|---|---|---|---|
| **Insert** | user write | Append the vector to its nearest posting (the original SPANN assignment). | §3.2 "Insert & Delete" |
| **Delete** | user write | Mark the vector deleted via a tombstone **version number** so it won't appear in results and is GC'd later. | §3.2 "Insert & Delete"; §4.1 |
| **Split** | posting exceeds a preset max length | GC deleted vectors first; if still oversized, run **balanced clustering** to split it into two postings with two new centroids; delete the old centroid, add the two new ones; then trigger **Reassign**. | §3.2 "Split"; §4.2.1 |
| **Merge** | posting smaller than a min-length threshold | Delete the small posting, append its vectors directly into its nearest posting; then **Reassign** only the moved vectors. Merges never cause splits (vectors only move *out*). | §3.2 "Merge" |
| **Reassign** | after a split or merge | Re-establish NPA for the small boundary set the split/merge may have disturbed, using the two necessary conditions below. | §3.2; §3.3 |

A subtle point from §3.2: a naïve split *violates* NPA. Figure 4 illustrates it — splitting posting `A`
into `A1, A2` can make a vector in neighbor `B` now closer to `A2` than to `B` (and vice versa). That is
why every split (and merge) must be followed by a reassign pass, and why a reassign can cascade (a
reassigned vector may itself overflow a target posting and trigger another split — §3.2, end).

## Reassign — the two necessary conditions (the clever, cheap part)

Reassigning vectors is the expensive part (it rewrites on-disk postings), so LIRE's contribution is to
**check as few vectors as possible**. §3.3 derives two *necessary conditions* for reassignment after a
split of old centroid `A_o` into new centroids `A_1, A_2` (Euclidean space assumed):

1. **A vector `v` that was in the split posting** is a reassignment candidate only if
   `D(v, A_o) ≤ D(v, A_i)` for **both** `i ∈ {1,2}` (Eq. 1). Intuition (§3.3): if the old (now deleted)
   centroid was still closer than *both* new centroids, then `v` may actually belong to some *neighboring*
   posting rather than to `A_1`/`A_2`. If a new centroid is already closer, the new assignment is fine and
   no neighbor can beat it, so no check is needed.

2. **A vector `v` in a nearby posting `B`** is a candidate only if `D(v, A_i) ≤ D(v, A_o)` for **some**
   `i` (Eq. 2). Intuition (§3.3): a newly created centroid has gotten closer to `v` than the old centroid
   was, so `v` might now belong in `A_1`/`A_2`. If both new centroids are farther than the old one was,
   they are also farther than `v`'s current centroid `B`, so `v` stays put.

These are *necessary*, not sufficient, conditions — they cheaply prune the vast majority of vectors, then
the survivors get an exact NPA re-check. §3.3: *"for vector candidate `v`, LIRE first searches `v`'s new
closest posting, then performs NPA check to get rid of false-positives: if a vector actually does not need
reassignment, the reassign operation is aborted. Otherwise, LIRE appends `v` in the newly identified
posting that is NPA-compliant and then deletes `v` in the original posting."*

To bound the cost further, LIRE doesn't scan the whole dataset for condition checks — it *"only examines
nearby postings … by selecting several `A_o`'s nearest postings"* (§3.3). §5.5's parameter study finds the
**nearest 64 postings is enough** ("Reassign top64"; Figure 11) to recover near-static accuracy.

Pseudocode for the post-split reassign pass (paraphrasing §3.2–§3.3 — *pseudocode, not Go*):

```
on split(posting P, old centroid A_o) -> (A_1, A_2):
    candidates = []
    for v in P.vectors:                       # vectors from the split posting
        if D(v, A_o) <= D(v, A_1) and D(v, A_o) <= D(v, A_2):   # Eq. 1
            candidates.append(v)
    for B in nearest_postings(A_o, 64):        # only the ~64 nearest neighbor postings
        for v in B.vectors:
            if D(v, A_1) <= D(v, A_o) or D(v, A_2) <= D(v, A_o): # Eq. 2
                candidates.append(v)
    for v in candidates:
        P_new = nearest_posting(v)             # true NPA target
        if P_new != current_posting(v):        # drop false positives
            append(P_new, v_with_bumped_version)
            tombstone(v_in_old_posting)        # GC'd lazily
```

## Convergence and measured cost

**Convergence (§3.4, "Split-Reassign Convergence").** A reassign can trigger more splits, which trigger
more reassigns — does it terminate? The paper proves yes. Sketch: a sequence of inserts changes `C` as
`C_i, C_{i+1}, …`; each split obeys `|C_{i+1}| = |C_i| + 1` (one centroid deleted, two added) and
`|C| ≤ |V|` (cannot exceed the number of vectors). Since `|V|` is finite, the number of split actions `N`
is finite, so the cascade terminates in finite steps (§3.4). Merges trivially terminate.

**Why it's effectively free at 1% daily update rate (§5.2.2, "Low and Stable Search Tail Latency").**
Measured on the 100M-scale SPACEV workload: *"only 0.4% insertion will cause rebalancing. Among them, the
average split number is 2, and the maximum split number is 160, with a cascading length of 3"* (§5.2.2).
Merge frequency is **0.1%** of updates. On average each rebalance evaluates **5094 vectors** but only
**79 are actually reassigned** (§5.2.2, also §2 contributions: "a minimal set of neighborhood vectors").
That is the whole bet: in a well-partitioned index, the boundary set that violates NPA after a local
change is tiny.

**Reported benefits (§ Abstract, §5).**

- vs DiskANN global rebuild: **2.41× lower P99.9 search tail latency on average**, with **only 1% of the
  DRAM and < 10% of the cores at peak**, on a billion-scale index at 1% daily update rate (Abstract; §5.2).
- **Stable** tail latency (§5.2.2): SPFresh holds P99.9 around **~4 ms**, while DiskANN spikes **> 20 ms**
  during global rebuilds and SPANN+ (append-only, no split/reassign) drifts past 10 ms as postings skew.
- Search accuracy stays high and *grows* over time because new vectors keep landing in correct postings;
  SPANN+'s accuracy degrades as distributions skew (§5.2.2, "High Search Accuracy").
- Billion-scale stress test (§5.3): stable recall **≥ 0.862 (uniform) / ≥ 0.807 (skewed)** while
  saturating NVMe IOPS, at ~74 GB memory.

## How LIRE updates only the affected partitions (vs a global rebuild)

The architectural reason LIRE is cheap (§4.1, Figure 5): updates are split across a **foreground In-place
Updater** and a **background Local Rebuilder**, connected as a feed-forward pipeline so split/merge/reassign
never sit on the foreground critical path. Inserts append to a posting's tail; deletes flip a one-byte
**version number** in an in-memory version map (seven bits for reassign-version, one bit for the delete
label — §4.2.1); split/merge/reassign are batched jobs run by background threads (§4.2). The on-disk
storage engine (**Block Controller**, §4.3) is *append-only per posting*, so an insert is a read-modify-write
of only the **last block** of one posting — not the whole posting and certainly not the whole index. Only
the postings that overflow/underflow and their ~64 nearest neighbors are ever rewritten. A global rebuild,
by contrast, re-clusters and rewrites *every* vector (§2.3, Table 1).

## What our clone does today, and the gap

The clone implements the **base SPANN/IVF idea** but not LIRE. Concretely:

- `BuildIndex` in [`internal/engine/indexer.go`](../../internal/engine/indexer.go) folds the entire
  durable WAL `[0, walUpTo)` into a **fresh epoch** via `buildVectorIndex` → `ChooseK` + `KMeans`
  ([`internal/engine/vector.go`](../../internal/engine/vector.go)), writes immutable
  `index/v{epoch}/centroids.json` + `cluster-{i}.json`, and publishes the epoch with a single
  `SaveManifestCAS` ([`internal/engine/manifest.go`](../../internal/engine/manifest.go)). This is a **full
  rebuild every run** — there is no split, merge, or reassign.
- There is **no NPA enforcement between rebuilds.** Between `tpuf index` runs, freshly upserted documents
  live only in the WAL tail and are found by the exhaustive tail scan in
  [`internal/engine/query.go`](../../internal/engine/query.go) (`runVectorQuery` →
  `MaterializeLiveAndDeleted(ctx, store, ns, m.IndexedUpTo, m.WALSeq)`), *not* by being placed into a
  posting. So the "data-distribution-skew" problem LIRE solves cannot even arise in the clone — because the
  clone never does incremental in-place posting updates in the first place. The clone's correctness comes
  from *rebuilding*, which is the very thing LIRE exists to avoid at scale.
- **Deletes** in the clone are WAL tombstones (`Document.Deleted`, [`internal/engine/types.go`](../../internal/engine/types.go))
  resolved at materialize time, not the in-memory version-map tombstones SPFresh uses (§4.2.2). Same
  *effect* (deleted vectors don't appear), different mechanism.

So the gap is total and deliberate: the clone is a faithful *static SPANN-lite*; LIRE is the *dynamic*
layer it omits.

## How LIRE would hook into tpuf (CAS / manifest / epoch implications)

This is the interesting part, and it is **not** a drop-in port — SPFresh's design assumes mutable
append-only posting *blocks* on local NVMe (Block Controller, §4.3), whereas tpuf's `index/v{epoch}/*`
objects are **write-once and immutable** (the comment in `indexer.go`: *"Every object written under it is
write-once and immutable"*). The honest hooks:

1. **Make postings mutable per-cluster objects.** Today a cluster is one immutable `cluster-{i}.json`
   (`ClusterFile` / `ClusterEntry` in `types.go`). LIRE's "append to a posting's tail" maps to a
   read-modify-write **CAS PUT** of `cluster-{i}.json` with `If-Match` — the same 412-retry loop the
   manifest already uses. SPFresh's fine-grained *posting-level write lock* (§4.2.2) becomes per-object CAS:
   a concurrent appender that loses the race gets a 412 and retries. (turbopuffer does not publicly document
   doing per-cluster CAS this way; this is inferred from the clone's existing CAS model, not stated by
   either source.)

2. **A split/merge/reassign job instead of `BuildIndex`.** Per [`../05-clone-mapping.md`](../05-clone-mapping.md)
   ("*Hook:* `internal/engine/indexer.go` would gain split/merge/reassign instead of full rebuild"), the
   indexer would: detect a posting over the length cap, run a **2-means split** (reuse `KMeans` from
   `vector.go` with `k=2`), write the two new cluster objects, update `centroids.json`, then run the §3.3
   reassign pass over the **nearest ~64 centroids** (computed exactly like `nearestClusters` in `query.go`,
   but ranked against `A_o`). The new `RaBitQ-lite` residual codes (`ResidualCode` in `vector.go`) must be
   **recomputed** for every reassigned vector, since the code is the sign-bit residual against *its
   centroid* and the centroid just changed.

3. **The atomic publish stays a manifest CAS, but the unit shrinks.** Today the whole epoch flips at one
   `SaveManifestCAS` that bumps `IndexEpoch` (rule 4). LIRE-style updates are incremental, so either (a) the
   manifest grows a per-cluster version/etag map so a single split publishes atomically without a new epoch,
   or (b) each rebalance still cuts a tiny new epoch. Option (a) is closer to SPFresh and to turbopuffer's
   "single manifest is the source of truth" model; option (b) is simpler but defeats the "no rebuild" point
   if epochs copy unchanged clusters. Either way the **WAL-tail-still-searchable** guarantee
   (`IndexedUpTo`..`WALSeq` scan in `query.go`) is unaffected — that is orthogonal to how the index itself is
   maintained.

4. **Convergence must be bounded under CAS.** SPFresh's convergence proof (§3.4) assumes a single-node
   updater. Under object-storage CAS, a cascading split-reassign that loses a 412 mid-cascade must be
   idempotent on retry — the version-number scheme (§4.2.2, "abort the reassignment and re-execute … if an
   atomic CAS operation fails on the vector version map") is precisely the mechanism that makes a lost race
   safe, and would map onto bumping a per-vector version inside the cluster object before the CAS PUT.

## What's genuinely hard / what to get right

- **Immutability vs in-place.** tpuf's whole story is *write-once objects on S3*; LIRE's whole story is
  *in-place posting mutation*. Reconciling them means turning postings into CAS-mutated objects, which
  trades the clone's pleasant "every index object is immutable and freely cacheable" property for
  per-object versioning. This is the deepest design tension and the reason the clone correctly chose rebuild.
- **Reassign false-positive aborts under concurrency.** §4.2.2 reports a vector can append to a posting that
  was concurrently deleted by a split (< 0.001% of inserts), forcing an abort and re-execute. Over CAS, this
  is a 412 on a now-gone target object; the retry must re-resolve the NPA target, not blindly re-PUT.
- **Recomputing residual codes.** A reassigned or re-centroided vector's `ResidualCode` (`vector.go`) is
  stale the moment its centroid changes. Forgetting to recompute silently corrupts the RaBitQ-lite prefilter
  (it would prefilter against the wrong centroid), degrading recall in a way unit tests on a static index
  would not catch.
- **The 64-posting reassign range is empirical, not exact.** §5.5/Figure 11 shows top-64 is "enough" for the
  paper's datasets; it is a recall/cost tuning knob, not a correctness bound. A clone reusing it should treat
  it as configurable and document that smaller ranges trade recall for speed.
- **Length-cap and merge-threshold choice.** Balanced postings depend on a sane split max / merge min. The
  paper relies on SPANN's balanced clustering (§3.2, "balanced clustering process … in [SPANN]"); a flat
  k-means clone (`ChooseK` ≈ √N) has no such balance guarantee, so naïvely bolting LIRE onto it could thrash.
  This is why LIRE assumes *starting from* a well-balanced index (§3, key rationale).

## Sources

- **SPFresh: Incremental In-Place Update for Billion-Scale Vector Search**, Xu et al., SOSP '23 — local PDF
  `docs/papers/spfresh-sosp23.pdf`. Cited by section: §2.3 (freshness challenges, Figure 2, Table 1),
  §3.1 (SPANN balance), §3.2 (NPA definition, the five operations, Figure 4, naïve-split NPA violation),
  §3.3 (reassign necessary conditions Eq. 1 / Eq. 2, nearby-posting scan), §3.4 (index state Eq. 3/4,
  split-reassign convergence proof), §4.1 (architecture, Figure 5, Updater/Local Rebuilder/Block Controller),
  §4.2–§4.2.2 (rebuild operators, version numbers, concurrency control, CAS on version map), §4.3
  (Block Controller append-only blocks), §5.2.2 (low/stable tail latency, 0.4% rebalance / avg split 2 /
  max 160 / cascade 3, 5094 evaluated → 79 reassigned), §5.3 (billion-scale recall ≥0.862/≥0.807),
  §5.5 + Figure 11 (reassign top-64 parameter study). The LIRE acronym (Lightweight Incremental RE-balancing)
  is the underlined title of §3.2.
- **SPFresh publication page** — Microsoft Research:
  https://www.microsoft.com/en-us/research/publication/spfresh-incremental-in-place-update-for-billion-scale-vector-search/
  (confirms LIRE = "Lightweight Incremental Rebalancing", the split/reassign description, the "1% of DRAM and
  less than 10% cores" figure, venue SOSP '23, and the author list).
- **ACM Digital Library** — https://dl.acm.org/doi/10.1145/3600006.3613166 (DOI 10.1145/3600006.3613166;
  corroborates LIRE as the protocol that splits partitions and reassigns boundary vectors to adapt to
  distribution shift).
- **SPANN: Highly-efficient Billion-scale ANN Search** (NeurIPS '21) — local PDF
  `docs/papers/spann-neurips21.pdf` — the base index LIRE rebalances; summarized in
  [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md).
- **This repo (real code referenced above):** `internal/engine/indexer.go` (`BuildIndex`,
  `buildVectorIndex`, write-once epoch objects), `internal/engine/vector.go` (`ChooseK`, `KMeans`,
  `ResidualCode`, `Distance`), `internal/engine/query.go` (`runVectorQuery`, `nearestClusters`,
  `MaterializeLiveAndDeleted` WAL-tail scan, `defaultNProbe = 3`), `internal/engine/manifest.go`
  (`SaveManifestCAS`, `LoadManifest`), `internal/engine/types.go` (`Manifest`, `ClusterFile`,
  `ClusterEntry`, `Document.Deleted`), and the clone-mapping ledger
  [`../05-clone-mapping.md`](../05-clone-mapping.md).

> **Inference flags.** The per-cluster-object CAS scheme, the choice between epoch-per-rebalance vs a
> per-cluster version map in the manifest, and the residual-code-recompute requirement are **design
> inferences for this clone**, derived from the clone's existing CAS/epoch model — they are *not* stated by
> SPFresh (which uses local-NVMe append-only blocks, not S3 objects) and are *not* publicly confirmed by
> turbopuffer (which does not document its internal posting-update mechanics at this level).
