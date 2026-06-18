# Implementing LIRE incremental updates in tpuf

> Companion to [`00-background.md`](./00-background.md) (why an incremental, balanced index is needed)
> and to the SPFresh/LIRE section of
> [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md) (the LIRE mechanism in detail). This doc is the **implementation
> design**: how to replace tpuf's full per-epoch rebuild with incremental LIRE-style split / merge /
> reassign, and — the genuinely hard part — what that does to our CAS-coordinated manifest. It is one of
> the deliberate non-goals in [`../05-clone-mapping.md`](../05-clone-mapping.md): *"SPFresh LIRE
> incremental updates (split/merge/reassign) — we rebuild the index per `index` run. **Hook:**
> `internal/engine/indexer.go` would gain split/merge/reassign instead of full rebuild."*

A static SPANN/IVF index is correct only for the data it was built from. As inserts and deletes
accumulate, the centroid set drifts away from the data distribution: postings grow uneven, the
nearest-centroid assignment goes stale, and recall degrades until you rebuild. SPFresh's **LIRE**
(Lightweight Incremental REbalancing) protocol fixes this *in place* with tiny, local rewrites instead
of a global rebuild (SPFresh §3.2). turbopuffer's production vector index is built on exactly this:
their ANN v3 blog states plainly that *"Vector indexes in turbopuffer are based on SPFresh, a
centroid-based approximate nearest neighbor index that supports incremental updates."* (turbopuffer does
**not** publicly describe its split/merge/reassign mechanics — those are the SPFresh paper's; see
Sources.) Our clone instead does the simple, correct thing: every `tpuf index` run rebuilds a brand-new
epoch from the whole WAL. This doc is the honest design for closing that gap — and an honest account of
why it is the hardest non-goal in the ledger.

## What our clone does today (and why rebuild is the easy path)

`BuildIndex` in [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go) folds the durable
WAL `[0, walUpTo)` into a **fresh, immutable epoch** and publishes it with a single manifest CAS:

```
m := LoadManifest()              # fresh GET
walUpTo := m.WALSeq              # rule 3: snapshot at index START
epoch   := m.IndexEpoch + 1
live    := MaterializeLive(0, walUpTo)        # last-writer-wins, deletes applied
buildVectorIndex(...)            # ChooseK → KMeans → per-doc ResidualCode → centroids.json + cluster-{i}.json
buildBM25Index(...)              # bm25.json   (if TextField != "")
writeDocs(...)                   # docs.json
SaveManifestCAS: IndexEpoch=epoch; IndexedUpTo=walUpTo; DocCount=len(live)   # rule 4: atomic swap
```

Every object under `index/v{epoch}/` is written **write-once** with the unconditional `putJSON`
(`store.Put`), because the epoch prefix is fresh — no concurrent writer can collide (see the comments on
`putJSON` and `BuildIndex`). The index goes live *only* at the final `SaveManifestCAS` that flips
`IndexEpoch` in [`../../internal/engine/manifest.go`](../../internal/engine/manifest.go). `KMeans` and
`ChooseK` in [`../../internal/engine/vector.go`](../../internal/engine/vector.go) re-cluster **all**
vectors from scratch on every run.

This is the deliberate simplification ([`../05-clone-mapping.md`](../05-clone-mapping.md)): a full
rebuild is `O(N)` work but trivially correct, has no in-place mutation hazards, and reuses our existing
write-once + single-CAS machinery untouched. The gap LIRE closes only *matters* when `N` is large enough
that re-clustering everything is the dominant cost — which, at demo scale, it never is. So treat the
rest of this doc as a *documented design*, not an implementation to chase.

## How LIRE would change the on-disk shapes

LIRE keeps the [SPANN](../02-spfresh-spann-index.md) layout — a centroid index plus one posting list per
cluster — but mutates them incrementally. The five operations (Insert, Delete, Split, Merge, Reassign;
SPFresh §3.2) map onto our two index files like this:

| LIRE op | tpuf object touched today | Incremental change |
|---|---|---|
| **Insert** | (none — goes to WAL) | append vector to the *tail* of its nearest `cluster-{i}.json` (read-modify-write of one cluster); if it exceeds a split cap → enqueue Split |
| **Delete** | (none — WAL tombstone) | bump a per-vector **version** in a version map; the old copy is dropped at query time and GC'd lazily — no cluster rewrite |
| **Split** | one `ClusterFile` + `CentroidsFile` | 2-means split an oversized cluster into two; remove the old centroid, add two new ones in `centroids.json`; then Reassign |
| **Merge** | two `ClusterFile`s + `CentroidsFile` | append a too-small cluster's members into its nearest cluster; drop its centroid; then Reassign |
| **Reassign** | a handful of nearby `ClusterFile`s + version map | move only the small boundary set that now violates NPA (Nearest Partition Assignment), append-with-new-version into the correct cluster, tombstone the old copy |

Two new pieces of state appear, neither of which exists in tpuf today:

- **A per-vector version map.** SPFresh keeps a one-byte-per-vector version number (seven bits for the
  reassign version, one bit for a delete label) so a stale replica can be detected and dropped during
  search and GC'd later (SPFresh §4.1, §4.2.1). In our JSON world this would be a `version int` field on
  `ClusterEntry` (in [`../../internal/engine/types.go`](../../internal/engine/types.go)) plus a small
  authoritative version table — because a reassigned vector is *appended* to its new cluster before the
  old copy is removed, so two copies coexist until GC.
- **A split/merge/reassign job notion.** SPFresh decouples this as a background *Local Rebuilder*
  pipeline driven by an *Updater* (SPFresh §4.1, §4.2), so rebalancing never sits on the foreground
  write path. tpuf has no daemon; the closest fit is to fold these jobs into a future indexer process
  (related non-goal: [`../extensions/broker-indexer-queue.md`](../extensions/broker-indexer-queue.md)).

The `CentroidsFile.Centroids [][]float32` / `Sizes []int` arrays would no longer be rewritten wholesale;
split and merge would edit individual rows. That single change — *mutating* `centroids.json` rather than
writing a fresh one — is the root of every coordination problem below.

## The hard part: per-cluster versioning vs. the single-manifest CAS

This is where the design stops being a paraphrase of the paper and starts being genuinely about *our*
clone, because SPFresh and tpuf coordinate completely differently.

**SPFresh coordinates with fine-grained, in-process primitives.** Concurrent append/split/merge on the
same posting take a per-posting write lock; concurrent reassign of the same vector races on a
compare-and-swap of that vector's entry in the in-memory version map (SPFresh §4.2.2). It is a
multi-threaded single-machine engine with shared memory. tpuf has none of that: every coordination
decision is a conditional `PUT` against object storage, and the manifest is the *only* mutable,
CAS-guarded object (correctness rule 2 — never cached; rule 4 — atomic publish is one CAS;
[`../06-implementation-blueprint.md`](../06-implementation-blueprint.md)).

The tension: LIRE wants to mutate **many** objects (split rewrites a cluster file and two centroids and
several neighbor clusters during reassign), each independently, concurrently, and durably — but tpuf's
whole correctness story is that **one** CAS publishes a consistent snapshot atomically. You cannot CAS
five cluster files "together"; object storage gives you per-object conditional writes, not multi-object
transactions. There are three plausible ways to bridge this, in increasing fidelity and difficulty:

### Option A — Keep epochs; make rebuild incremental internally (recommended first step)

Keep the *exact* publish model we have — write-once objects under `index/v{epoch}/`, one final manifest
CAS — but stop re-clustering from scratch. The new epoch *copies forward* the previous epoch's centroids
and clusters and applies only the LIRE deltas implied by the WAL tail `[IndexedUpTo, WALSeq)`: append
new vectors to their nearest cluster, split/merge where caps are crossed, reassign the boundary set.
Most clusters are copied byte-for-byte; only the touched ones are recomputed.

- **Coordination changes: none.** Still write-once under a fresh epoch, still one atomic CAS, rules 1–5
  untouched. Concurrency is exactly today's — `index` is a single actor producing an immutable snapshot.
- **What it buys:** the *quality* win of LIRE (centroids track the distribution; postings stay balanced)
  and far less compute per index run than full k-means, **without** touching the CAS model.
- **What it does not buy:** no in-place storage savings — each epoch is still a full physical copy. This
  is the SPFresh *algorithm* without the SPFresh *storage engine*. It is the right first step precisely
  because it isolates the hard idea (incremental rebalancing math) from the hard system change
  (mutable shared state).

> **Implementation status (Phase 1 — Option A — IMPLEMENTED).** Option A is now built in the engine. The
> LIRE rebalancing math lives in [`../../internal/engine/lire.go`](../../internal/engine/lire.go) (the
> mutable `lireIndex` working set plus `splitOversized` / `mergeUnderfull` / `reassign`); the copy-forward
> driver — `clusterLive` → `incrementalCluster` → `loadPrevEpoch` / `snapshot` — lives in
> [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go) and is wired into `BuildIndex`
> behind the unchanged single-CAS publish. `BuildIndex` now copies the previous epoch's centroids/clusters
> forward and applies only the WAL-tail deltas: insert into nearest posting, split oversized postings with
> a local 2-means, merge under-full ones, reassign the NPA boundary set. Touched postings are recomputed;
> the rest are carried forward. The per-vector version map is real: `ClusterEntry.Version`
> ([`../../internal/engine/types.go`](../../internal/engine/types.go)) is a JSON-round-tripping field that
> `reassign` bumps when it appends a higher-version copy to the NPA-correct posting before dropping the old
> one, so the version shadows the stale replica (and accumulates monotonically across epochs).
>
> **Coordination is unchanged — Option A's whole point.** The incremental path still writes every
> `index/v{epoch}/*` object write-once under a fresh epoch prefix and still goes live at exactly **one**
> `SaveManifestCAS` (rule 4). `IndexedUpTo`/`walUpTo` are snapshotted at index start (rule 3); the WAL uses
> `PutIfAbsent` (rule 1); the manifest and WAL are read fresh, never cached, while only immutable
> `index/v{epoch}/*` objects are `GetCached` (rule 2); the query still overlays the WAL tail
> `[IndexedUpTo, WALSeq)` with last-writer-wins and tombstones (rule 5). Nothing about the CAS model moved,
> which is exactly why Option A is the correct Phase 1.
>
> **Correctness-over-optimization choices made here.** (1) A vector whose *own* position changed (a fresh
> insert or a re-upsert that moved it) is force-rechecked unconditionally — the §3.3 Eq. 1/Eq. 2 necessary
> conditions are derived for vectors whose *centroid* changed, not for vectors that themselves moved, so the
> conditions alone would leave a moved vector NPA-violating. (2) The working set is reconciled against the
> authoritative live set (the same materialization the full rebuild uses), so the incremental epoch provably
> covers exactly the live set with no double-count or drop, without a per-vector journal. (3) Any case too
> entangled to reconcile incrementally — no prior epoch, a branch's initial flatten, a dimension change, an
> unloadable/empty prior epoch — falls back to a **correct full rebuild**, which is always safe.
>
> **DEFERRED to later phases (not built):**
> - **Full Option B** — per-cluster versioned objects (`index/clusters/{id}/v{n}.json`, write-once) with the
>   manifest demoted to a catalog (`cluster_id → live version` + centroids), so a split/merge publishes by
>   writing new immutable bodies and flipping the catalog in one CAS instead of cutting a whole new epoch.
>   This is where two copies of a reassigned vector legitimately coexist *across objects* until GC, and where
>   `ClusterEntry.Version` earns its keep as the cross-object shadow. It also reintroduces the manifest CAS
>   hotspot and the `O(#clusters)` catalog growth this doc flags above.
> - **The background Local Rebuilder** (SPFresh §4.1–§4.2) — split/merge/reassign as batched background jobs
>   off the foreground write path, with per-posting locks and a per-vector version-map CAS. Phase 1 runs the
>   whole rebalance synchronously inside one single-threaded `index` build against an in-memory snapshot, so
>   none of that concurrency machinery is needed yet.
> - **Lazy GC of stale versions / orphaned epochs** (SPFresh §4.3 Free Block Pool) — Phase 1 drops the stale
>   reassigned copy *within* the same epoch before the CAS, so no garbage accumulates inside an epoch; old
>   epochs are GC-able exactly as before (not built here).
>
> **Reassign is the subtle part — ported, not guessed.** The two necessary conditions (Eq. 1 for split-posting
> members, Eq. 2 for nearby-posting members), the append-then-tombstone order, the false-positive abort on the
> exact NPA re-check, and the bounded nearby-posting scan are taken from SPFresh §3.2–§3.3 as cited below, not
> invented. The `reassignTopN` neighbor bound is a recall/cost knob (§5.5), **not** a correctness bound; at this
> clone's tiny K it is set generously so the scan is effectively exhaustive and the produced epoch is fully
> NPA-correct.

### Option B — Per-cluster versioned objects, manifest as the directory

Make clusters individually mutable and immutable-per-version: keys like
`index/clusters/{id}/v{n}.json`, written write-once with `PutIfAbsent`. The manifest stops being an
epoch pointer and becomes a **catalog**: `cluster_id → live version`, plus the centroid set (or a pointer
to a versioned `centroids.json`). A split is then: write the two new cluster-version objects (write-once,
no contention), then **one manifest CAS** that atomically removes the old cluster id and adds the two new
ids *and* updates the affected centroid rows. The CAS retry loop in `SaveManifestCAS` already does
exactly the read-modify-write-or-412-retry this needs.

- **Coordination changes: substantial but bounded.** Cluster *bodies* are still write-once (so they stay
  cacheable in the DRAM tier, like any epoch object today). All the *mutable* state — which versions are
  live, the centroids — collapses back into the single manifest, preserving "one CAS = one atomic
  publish." A split/merge/reassign becomes "write the new immutable bodies, then one manifest CAS to flip
  the catalog."
- **The cost: the manifest becomes a write hotspot.** Today the manifest is touched once per `upsert` and
  once per `index`. Under Option B every split, merge, and reassign batch is a manifest CAS, so a busy
  namespace serializes its rebalancing through one object's ETag. SPFresh sidesteps this with per-posting
  locks; we would lean on the CAS retry budget (`maxCASAttempts = 10` in `manifest.go`) and on batching
  many cluster changes into one CAS (one rebuild pass → one catalog flip). This is the honest scaling
  ceiling of "coordinate everything through one JSON file."
- **The manifest also grows.** A catalog of every cluster's live version is `O(#clusters)` in the
  manifest body, read and rewritten on *every* CAS. At large `N` this re-re-reads megabytes per write —
  the point where turbopuffer's real index needs the hierarchical centroid tree
  ([`../extensions/hierarchical-centroid-tree.md`](../extensions/hierarchical-centroid-tree.md)) and a
  smarter metadata layout, not a flat catalog.

### Option C — Truly independent per-cluster CAS (closest to SPFresh, hardest)

Give each cluster its own CAS-guarded mutable object and let split/merge/reassign mutate them directly,
coordinating *between* objects with a small intent/journal record. This is the faithful analogue of
SPFresh's per-posting locks. It is also where the correctness rules go to die: there is no single moment
that atomically publishes a multi-cluster change, so a crash between "wrote new cluster A1, A2" and
"removed old cluster A" leaves the index in a state a reader must reconcile. SPFresh handles partial
failure with a periodic snapshot + WAL replay for crash recovery (SPFresh §4.4); replicating that on
object storage means building a mini transaction log — exactly the kind of consensus/coordination
machinery the whole "CAS on one JSON file" bet exists to *avoid* ([`../01-architecture.md`](../01-architecture.md)).
**Recommendation: do not.** It contradicts the clone's thesis; if you want it, you want a different
system.

## How it interacts with the WAL tail and epochs

The WAL-tail scan is the feature that makes this *tractable*, and it should not change. Queries already
union the live index with an exhaustive scan of the unindexed tail `[IndexedUpTo, WALSeq)`
(`runVectorQuery` / `runBM25Query` in [`../../internal/engine/query.go`](../../internal/engine/query.go),
via `MaterializeLiveAndDeleted`), applying last-writer-wins and subtracting tombstones (rule 5). This
means **the index never has to be perfectly current** — fresh writes are searchable through the tail
regardless of whether LIRE has folded them in yet. So LIRE's append/split/merge can run lazily and
in batches; the tail covers the lag, exactly as it covers the rebuild lag today.

That has a clean consequence: **keep `IndexedUpTo`.** Whatever LIRE variant we pick, the contract "the
index covers `[0, IndexedUpTo)`, the query overlays `[IndexedUpTo, WALSeq)`" stays the durable boundary.
Under Option A, `IndexedUpTo` advances exactly as today (snapshot at start of an incremental pass). Under
Option B it advances when a rebalancing batch's catalog CAS lands. Rule 3 still governs: snapshot the WAL
position *before* doing the work, or writes that arrive mid-rebalance get dropped from both the index and
the tail.

**Should we keep epochs at all?** For Option A, yes — epochs *are* the publish mechanism. For Option B,
epochs soften into a per-cluster version counter plus a manifest generation number; the global `epoch`
becomes mostly a GC anchor (which old cluster-version objects are safe to delete). Option C abandons
monotonic epochs for per-object versions and needs explicit GC of orphaned versions — SPFresh's *Free
Block Pool* / lazy garbage collection (SPFresh §4.1, §4.3) is the reference, and the absence of it is a
classic incremental-index leak.

## What's genuinely hard / what to get right

- **Mutating `centroids.json` breaks the cache invariant.** Today every `index/v{epoch}/*` object is
  immutable, which is *why* `query.go` reads them with `GetCached`. The moment centroids become mutable
  (Option C) the DRAM cache can serve a stale centroid set and silently route queries to the wrong
  clusters. Options A and B preserve immutability of *bodies* (only the manifest/catalog mutates, and the
  manifest is never cached — rule 2), which is the main reason to prefer them.
- **Reassign correctness is subtle and must be exact.** LIRE moves a vector by appending it (new version)
  to the correct cluster *before* tombstoning the old copy, so for a window two copies exist; the version
  map is what makes the stale one invisible to search and reclaimable later (SPFresh §3.3, §4.2.1).
  Implemented carelessly this either double-counts a vector in results or drops it during the window.
  tpuf's query-side dedup is keyed on document `id` with last-writer-wins; a per-`id` version would have
  to win deterministically over the indexed copy, mirroring how the WAL tail already shadows indexed docs
  in `query.go`.
- **NPA is the invariant, and splits cascade.** A split can push neighbors out of NPA compliance,
  triggering further reassigns; SPFresh proves this converges (SPFresh §3.4) but measured it as cheap
  only *empirically* — only ~0.4% of inserts trigger any rebalancing, a split-reassign evaluates
  ~5,094 vectors but moves only ~79, mean cascade length 3 (SPFresh §5.2; see
  [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)). Those numbers are SPFresh's on
  billion-scale workloads, **not** anything our clone or turbopuffer publishes; do not assume them at
  small `N`, where a split touches a meaningful fraction of the data and the "local region" assumption is
  weak.
- **The CAS hotspot is the real architectural cost.** Folding all mutable index state into one manifest
  (Option B) is faithful to the clone's thesis but serializes rebalancing through one ETag. The
  mitigation — batch many cluster edits into one CAS — is also what keeps the design honest: a `tpuf
  index` run becomes "compute all deltas, then one atomic catalog flip," which is just Option A wearing
  Option B's clothes. That convergence is the tell that **Option A is the right place to start.**
- **Crash recovery.** A full rebuild is crash-safe for free: a half-written epoch is simply never
  published (the manifest CAS never happens), and its orphaned objects are GC'd. Any in-place scheme
  (Option C) loses that property and must reconstruct consistency after a mid-rebalance crash — SPFresh's
  snapshot+WAL recovery (SPFresh §4.4). Keeping the single-CAS publish (A/B) keeps crash recovery trivial,
  which is a large part of why those options are recommended.
- **This is the hardest non-goal, and that is the point.** Combining LIRE with the manifest-CAS model is
  where production complexity actually lives. The other extensions in [`../extensions/`](../extensions)
  bolt features onto an immutable-snapshot world; LIRE attacks the immutability itself. The intellectually
  honest deliverable is Option A — incremental rebalancing math behind an unchanged atomic-publish
  boundary — and a written acknowledgment that Options B/C trade away the clone's defining simplicity for
  fidelity it does not need at demo scale.

## Sources

- **SPFresh: Incremental In-Place Update for Billion-Scale Vector Search**, Xu et al., SOSP '23 — read
  locally at [`../papers/spfresh-sosp23.pdf`](../papers/spfresh-sosp23.pdf). arXiv:
  <https://arxiv.org/abs/2410.14452> · ACM DOI: 10.1145/3600006.3613166.
  - §3.2 "LIRE: Lightweight Incremental REbalancing" + Figure 4: the five operations (Insert, Delete,
    Split, Merge, Reassign) and the NPA-violation-after-split picture.
  - §3.3 "Reassigning Vectors" + Eqs. 1–2: the two necessary reassignment conditions; reassign appends to
    the new posting then deletes (tombstones) the old copy.
  - §3.4 "Split-Reassign Convergence": proof that split→reassign cascades terminate in finite steps.
  - §4.1 "Overall Architecture" + Figure 5: the *Updater* (foreground, maintains a version map with a
    tombstone bit) and *Local Rebuilder* (background split/merge/reassign jobs) pipeline; one-byte
    per-vector version number (7 bits reassign version + 1 bit delete).
  - §4.2 "Local Rebuilder Design" + §4.2.2 "Concurrent Rebuild": per-posting write lock for
    append/split/merge; per-vector version-map CAS for concurrent reassign; measured <1% posting write
    contention and <0.001% stale-append aborts.
  - §4.3 "Block Controller Design": append-only postings, in-memory block mapping, Free Block Pool for
    lazy GC of stale/garbage blocks.
  - §4.4 "Crash Recovery": periodic snapshot + WAL replay for in-place state.
  - §5.2 "Real-World Update Simulation": empirical rebalancing cost on billion-scale workloads (the
    "0.4% of inserts trigger rebalancing", "~79 vectors moved", "mean cascade 3" figures originate here /
    in the analysis carried in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)). These are
    SPFresh's measurements, **not** turbopuffer's or this clone's.
- **turbopuffer, "ANN v3"** — <https://turbopuffer.com/blog/ann-v3> (fetched and verified). Direct quote
  used: *"Vector indexes in turbopuffer are based on SPFresh, a centroid-based approximate nearest
  neighbor index that supports incremental updates."*
  - ⚠️ **Flagged:** the ANN v3 blog confirms turbopuffer's index is *based on SPFresh* and *supports
    incremental updates*, but it does **not** describe split / merge / reassign, the version map, or any
    on-disk update mechanics. Every such mechanism in this doc is from the SPFresh **paper**, presented as
    the design turbopuffer's index is "based on," not as a claim about turbopuffer's internal
    implementation. (turbopuffer does not publish literal index filenames or its update algorithm — see
    [`../papers/SOURCES.md`](../papers/SOURCES.md).)
- **This clone's code & docs:**
  [`../../internal/engine/indexer.go`](../../internal/engine/indexer.go) (`BuildIndex`,
  `buildVectorIndex`, `putJSON` — full rebuild, write-once-under-epoch, single CAS),
  [`../../internal/engine/manifest.go`](../../internal/engine/manifest.go) (`SaveManifestCAS`,
  `maxCASAttempts`, the never-cache rule),
  [`../../internal/engine/query.go`](../../internal/engine/query.go) (`runVectorQuery`,
  `runBM25Query`, the WAL-tail overlay via `MaterializeLiveAndDeleted`),
  [`../../internal/engine/vector.go`](../../internal/engine/vector.go) (`ChooseK`, `KMeans`,
  `ResidualCode`), [`../../internal/engine/types.go`](../../internal/engine/types.go) (`Manifest`,
  `CentroidsFile`, `ClusterFile`, `ClusterEntry`),
  [`../../internal/engine/namespace.go`](../../internal/engine/namespace.go) (`Upsert`, `Index`),
  [`../../internal/storage/storage.go`](../../internal/storage/storage.go) (`PutCAS`, `PutIfAbsent`,
  `Put`, `ErrPreconditionFailed`).
  - [`../01-architecture.md`](../01-architecture.md), [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md)
    (§SPFresh — the LIRE mechanism in detail), [`../05-clone-mapping.md`](../05-clone-mapping.md) (the
    non-goal + hook), [`../06-implementation-blueprint.md`](../06-implementation-blueprint.md) (the five
    CAS correctness rules), [`../extensions/broker-indexer-queue.md`](../extensions/broker-indexer-queue.md)
    and [`../extensions/hierarchical-centroid-tree.md`](../extensions/hierarchical-centroid-tree.md)
    (related non-goals this design leans on).
