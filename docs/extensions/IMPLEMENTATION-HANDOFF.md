# Implementation handoff — building the non-goal extensions

> **Status: planning doc, intentionally uncommitted.** This is the bridge from the *how it works* KB
> (`docs/extensions/*.md`, `docs/spfresh-lire/*.md`) to a *how to build it in tpuf* plan — the same role
> [`docs/06-implementation-blueprint.md`](../06-implementation-blueprint.md) played for the core engine.
> Read the matching KB doc first, then the per-feature plan here.

Each extension below names the **real files** to touch (all confirmed against the current tree), the
approach, the correctness hazards, the tests, and how to *prove the win* (usually via `cmd/tpuf-bench`
or the CLI). Effort tags match `docs/05-clone-mapping.md`.

---

## Ground rules every extension must respect

These are non-negotiable; they are why the engine is correct today.

1. **The 5 CAS correctness rules still hold** (from `docs/06`): WAL writes use `PutIfAbsent`; the
   manifest/WAL are **never** cached (fresh `LoadManifest` each CAS loop); `IndexedUpTo` is the `WALSeq`
   snapshot at index *start*; an index epoch goes live via **one** `SaveManifestCAS`; query is
   last-writer-wins (WAL tail after the indexed path, tombstones subtracted). Any feature that touches
   the index/WAL/manifest must state how it preserves these.
2. **The engine keeps one dependency** (`aws-sdk-go-v2/s3`). Every extension below is implementable in
   pure stdlib — do not reach for a library to implement a core concept. (The bench's TUI deps are the
   only sanctioned exception, and they're tooling, not engine.)
3. **Test against the in-memory store.** New logic gets co-located table-driven `*_test.go` over
   `storage.New()` (the in-memory `ObjectStore`); the real-MinIO `//go:build integration` test is only
   for new S3-contract surface. Run `go test ./... -race` — anything touching the write path or a new
   cache tier must be race-clean.
4. **Prove it with the bench.** A feature that claims a latency/throughput effect must be demonstrable
   in `cmd/tpuf-bench` (add a flag or a phase) — the way the DRAM cache's value is shown today.

---

## Recommended order

| # | Feature | KB doc | Effort | Depends on | Why this slot |
|---|---|---|---|---|---|
| 1 | Hybrid fusion (RRF) | `hybrid-fusion.md` | Low | — | smallest, read-only, proves the pattern |
| 2 | NVMe ring-buffer tier | `nvme-ring-buffer-cache.md` | Low–Med | — | completes the cache story the bench measures |
| 3 | Group commit | `group-commit.md` | Low–Med | — | self-contained write-path change |
| 4 | Bitmap attr indexes + planner | `bitmap-attribute-indexes.md` | Med | indexer epoch | new immutable index file + planner |
| 5 | True RaBitQ | `true-rabitq.md` | Med | indexer epoch | math upgrade to the prefilter |
| 6 | Branches (CoW) | `branches-copy-on-write.md` | Med | manifest model | structural; needs GC-awareness |
| 7 | Broker + indexer + queue.json | `broker-indexer-queue.md` | Med–High | — | multi-process; mirrors manifest CAS |
| 8 | Hierarchical centroid tree | `hierarchical-centroid-tree.md` | Med | indexer | pedagogical at our N; do for completeness |
| 9 | **SPFresh LIRE** | `../spfresh-lire/` | **High** | reworks the index lifecycle | last — it invalidates "rebuild per epoch" |

Do 1–3 as independent PRs first (each is an afternoon and each is end-to-end demonstrable). 4–8 are
independent of each other. **9 comes last** because it changes the index from immutable-epoch-rebuild to
mutable-in-place, which the others assume.

---

## Per-feature plans

### 1. Hybrid fusion (RRF)
- **Touch:** `internal/engine/query.go`, `internal/engine/types.go`, `cmd/tpuf/main.go`.
- **Approach:** add a hybrid mode to `QueryParams` (e.g. `RankBy{Vector, Text}` both set ⇒ hybrid).
  In `RunQuery`, run the existing `runVectorQuery` and `runBM25Query`, then fuse by **Reciprocal Rank
  Fusion**: `score(id) = Σ 1/(k + rank_in_list)`, `k=60` (cite `hybrid-fusion.md`). Sort desc, take
  `TopK`. Keep both raw `$dist`/`$score` on the result for transparency.
- **CAS/correctness:** read-only — no WAL/manifest/index impact. The WAL-tail scan already runs in both
  sub-queries, so freshness is preserved for free.
- **CLI:** allow `tpuf query <ns> --vector ... --bm25 ...` together ⇒ hybrid (today they're mutually
  exclusive in `cmd/tpuf/main.go`).
- **Tests:** RRF math on synthetic ranked lists; a doc that ranks high in both beats one strong in only
  one; `k` sensitivity.
- **Prove it:** add a `--hybrid` path to the bench; show fused recall vs either mode alone on a labeled set.

### 2. NVMe ring-buffer cache tier
- **Touch:** `internal/cache/` (add `nvme.go`), `internal/cache/cache.go`, `cmd/tpuf-bench/main.go`,
  `cmd/tpuf*/` backend wiring.
- **Approach:** insert a disk tier *under* `Store.GetCached`. On a DRAM miss, check the NVMe tier; on an
  NVMe miss, `backend.Get`, then populate **both**. Model NVMe as a **fixed-size FIFO ring** (per
  `nvme-ring-buffer-cache.md`): a bounded set of slots where a new object overwrites the oldest — no LRU
  bookkeeping. Simplest faithful impl: a capped directory of `{hash(key)}.obj` files + an in-memory
  ring index (`[]string` of keys + a write cursor) persisted alongside; evict = delete the file at the
  cursor before writing the new one.
- **CAS/correctness:** **rule 2 still binds** — the NVMe tier caches *only* the immutable
  `index/v{epoch}/*` objects (the same keys `GetCached` already gates), never the manifest or WAL.
  Because epoch keys are write-once, a cached body can never go stale.
- **Stats:** extend `cache.CacheStats` to count `DRAMHits / NVMeHits / Misses` so the bench's cache
  panel becomes the real **3-tier** breakdown.
- **Tests:** FIFO eviction (oldest slot overwritten); tier promotion (DRAM miss → NVMe hit); a counting
  backend proving an NVMe hit does not reach S3.
- **Prove it:** bench run with DRAM capped small but NVMe large — show DRAM-cold queries served from
  NVMe at a latency between DRAM and S3.

### 3. Group commit
- **Touch:** `internal/engine/namespace.go` (`Upsert`), maybe a new `internal/engine/commit.go`.
- **Approach:** a per-namespace buffering goroutine. `Upsert` enqueues docs + a result channel; a
  flusher coalesces everything that arrives within a window (the KB doc cites turbopuffer's reported
  ~1 WAL entry/sec/namespace — flag it as their figure), writes **one** WAL segment via the existing
  `PutIfAbsent` path, does **one** manifest `SaveManifestCAS`, then signals every waiting caller. Add a
  `Namespace.Close()`/flush to drain on shutdown.
- **CAS/correctness:** **rule 1 preserved** — still `PutIfAbsent` per segment; the batch is just bigger.
  Durable-before-return must still hold: a caller's `Upsert` returns only after *its* batch's WAL PUT +
  manifest CAS are acked.
- **Tests:** N concurrent `Upsert`s land in ⌈N/window⌉ WAL segments (assert via `List`); every caller
  sees success; `-race`. Compare WAL segment count vs today's one-per-upsert.
- **Prove it:** bench the write path (`upsert (batch)` row) with/without group commit at high concurrency.

### 4. Bitmap attribute indexes + filter planner
- **Touch:** `internal/engine/indexer.go`, `internal/engine/query.go`, `internal/engine/types.go`.
- **Approach:** in `BuildIndex`, build `(attrName, value) → bitmap-of-doc-ids` into a new immutable
  `index/v{epoch}/bitmaps.json`. In `RunQuery`, add a planner: if a filter is highly selective
  (small bitmap), **filter-first** (intersect bitmaps → candidate set → score only those); else
  **search-first** (current per-candidate `Filter.Match`). Hand-write a compact bitmap (sorted-uint or a
  roaring-lite) per `bitmap-attribute-indexes.md`.
- **CAS/correctness:** `bitmaps.json` is part of the epoch (immutable, `GetCached`-able). The WAL-tail
  docs still get per-candidate `Filter.Match` (no bitmap for unindexed data) — last-writer-wins intact.
- **Tests:** bitmap build correctness; planner picks filter-first on a selective predicate; result set
  identical to today's per-candidate path (this is the key invariant — same answers, faster).
- **Prove it:** bench a filtered query at low vs high selectivity; show filter-first wins when selective.

### 5. True RaBitQ (rotation + unbiased estimator)
- **Touch:** `internal/engine/vector.go`, `internal/engine/indexer.go`, `internal/engine/query.go`,
  `internal/engine/types.go`.
- **Approach:** replace the sign-bits-only "RaBitQ-lite" with the paper's method (READ
  `docs/papers/rabitq-sigmod24.pdf` and `true-rabitq.md`): a seeded random orthogonal rotation applied
  before quantization, plus the **unbiased distance estimator** for the binary-scan phase. Store the
  rotation seed/matrix in `CentroidsFile` (immutable, per epoch). Use the estimator in the prefilter,
  then exact rerank as today.
- **CAS/correctness:** rotation lives in the epoch — deterministic and immutable. No coordination change.
- **Tests:** estimator within the paper's error bound vs brute-force distances; recall@k vs the current
  lite codes on a synthetic set. Do **not** invent the estimator — port it from the cited equations.
- **Prove it:** bench recall + the prefilter→rerank shortlist size at fixed recall (true RaBitQ should
  need a smaller shortlist for the same recall).

### 6. Branches (copy-on-write namespaces)
- **Touch:** `internal/engine/manifest.go`, `internal/engine/namespace.go`, `cmd/tpuf/main.go`.
- **Approach:** a branch is a **new manifest** under a branch prefix that points at the parent's existing
  (immutable) `wal/` + `index/v{epoch}/` objects; new writes to the branch go to the branch's own WAL.
  CoW because object storage shares the parent's bytes for free (`branches-copy-on-write.md`).
- **CAS/correctness:** the branch manifest is its own CAS head. **Hazard:** GC must not delete objects a
  branch still references — and GC isn't built, so document that a branch pins parent objects.
- **Tests:** branch reads parent data at fork point; writes to branch don't appear in parent (and vice
  versa); fork is O(1) (one manifest PUT).
- **Prove it:** CLI demo — `tpuf branch demo exp1`, upsert into `exp1`, show `demo` unchanged.

### 7. Broker + indexer + queue.json
- **Touch:** new `cmd/tpuf-broker/`, `cmd/tpuf-indexer/`; `internal/engine/namespace.go` (enqueue).
- **Approach:** move indexing out of the inline `index` command into daemons (`broker-indexer-queue.md`).
  A query/write path notes "needs reindex" once the unindexed WAL passes a threshold; the **broker**
  group-commits requests into `queue.json` on object storage via CAS; **indexer** processes claim a job
  by CAS-marking it in-progress (the loser's CAS fails and retries) and then run `BuildIndex`.
- **CAS/correctness:** `queue.json` uses the **exact same** `If-Match`/412 CAS pattern as the manifest —
  reuse the `SaveManifestCAS` shape. Index publish is still one manifest CAS (rule 4). Flag any part of
  the real `queue.json` format that isn't publicly documented.
- **Tests:** two indexers race for one job → exactly one wins (over the memory store, `-race`); a backlog
  drains; an indexer crash mid-job is reclaimable.
- **Prove it:** run a broker + 2 indexers in the `deploy/` compose; show jobs claimed once and the epoch
  advancing without an inline `index` call.

### 8. Hierarchical centroid tree
- **Touch:** `internal/engine/vector.go`, `internal/engine/indexer.go`, `internal/engine/query.go`,
  `internal/engine/types.go`.
- **Approach:** recurse the k-means clustering into levels (`hierarchical-centroid-tree.md`), store the
  tree in `CentroidsFile`, and traverse top-down at query time (probe the best parent, then its children).
- **Reality check:** this only reduces comparisons at large N; at our hundreds–thousands of vectors it is
  pedagogical (likely equal or slower). Build it to *show the shape*, and say so in the doc/tests.
- **Tests:** tree build; query returns the same top-K as the flat index (correctness), measured fan-out.

---

## The hard one: SPFresh LIRE (do last)

Full plan in [`../spfresh-lire/02-implementation-in-tpuf.md`](../spfresh-lire/02-implementation-in-tpuf.md);
protocol in [`01-lire-protocol.md`](../spfresh-lire/01-lire-protocol.md). The crux: tpuf today rebuilds
the whole index per `tpuf index` (immutable epoch + one manifest CAS). LIRE means **mutable, per-cluster,
incremental** split/merge/reassign — which forces the real architectural change: **per-cluster
coordination instead of a single manifest CAS**.

Phased:
1. **Per-cluster versioning.** Give each `cluster-{i}.json` its own version/ETag so clusters can change
   independently without rewriting the whole epoch. Decide: keep epochs as checkpoints, or go fully
   mutable.
2. **Split.** When a posting list exceeds a size bound, split it (local k-means into 2), write the new
   clusters, update the centroid set — all via CAS, with the query path tolerant of a split in progress.
3. **Reassign (the NPA rule).** On centroid drift, reassign affected vectors to their nearest partition;
   port the exact balance/quality invariant from the paper (read `spfresh-sosp23.pdf` §3.2 — do not guess).
4. **Merge.** Collapse under-full posting lists.

**Hardest parts (call them out in the PR):** coordinating concurrent split/merge against live queries
without a global lock (the manifest-CAS-only model doesn't obviously extend); keeping last-writer-wins +
tombstones correct while clusters mutate; and proving the index stays *balanced* over a long write stream.
Treat this as a multi-PR research effort, not a feature.

---

## Suggested PR / milestone sequence

1. `feat(query): hybrid search via reciprocal rank fusion` (+ CLI `--vector --bm25` together).
2. `feat(cache): NVMe ring-buffer tier (3-tier DRAM/NVMe/S3)` (+ bench 3-tier stats).
3. `feat(engine): group commit for batched WAL writes`.
4. `feat(index): bitmap attribute indexes + filter/search planner`.
5. `feat(vector): full RaBitQ rotation + unbiased estimator`.
6. `feat(engine): copy-on-write branches`.
7. `feat: broker + indexer daemons with queue.json CAS` (+ compose services).
8. `feat(index): hierarchical centroid tree` (pedagogical).
9. `feat(index): SPFresh LIRE incremental updates` — staged across several PRs (see above).

## Definition of done (every feature)

- Co-located table-driven tests pass under `go test ./... -race`; `gofmt`/`go vet` clean.
- The 5 CAS correctness rules demonstrably preserved (state how in the PR).
- Engine still has exactly one external dependency.
- The claimed effect is shown in `cmd/tpuf-bench` or a CLI/`deploy/` demo, with before/after numbers.
- The matching KB doc updated if the implementation taught you something the design missed.
