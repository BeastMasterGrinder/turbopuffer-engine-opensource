# Extension — The NVMe ring-buffer cache tier

> **Implemented (2026-06-18).** A FIFO ring-buffer disk tier sits under the DRAM map in
> `internal/cache/nvme.go` (caching only immutable `index/v{epoch}/*` objects — never the manifest/WAL),
> with DRAM/NVMe/S3 counters; `cmd/tpuf-bench --nvme-dir` shows DRAM-evicted reads served from the ring
> instead of S3. The text below is the design rationale.

> Status: **design note**, not built. This expands the "NVMe ring-buffer cache" non-goal from
> [`../05-clone-mapping.md`](../05-clone-mapping.md) (and the NVMe row of the three-tier cache table in
> [`../01-architecture.md`](../01-architecture.md)) into a real knowledge-base entry.

turbopuffer's bet is that *object storage is the source of truth and everything else exists to hide its
latency* ([`../01-architecture.md`](../01-architecture.md)). The two things hiding S3's latency are
caches: a small **DRAM** tier for the hottest data and a much larger **local NVMe SSD** tier for warm
data. This doc is about the middle tier. It is a single fixed-size file on local disk, written
**sequentially and wrapped around when full — a FIFO ring buffer**, with **no LRU and no eviction
heuristics**. turbopuffer reports it at roughly **200 lines of code** (see Sources — this specific
figure comes from a third-party deep-dive, not turbopuffer's own docs). The point of writing it down for
*tpuf* is that our clone deliberately implements only the DRAM and S3 tiers; this is the tier that, in
the real product, turns a small bounded DRAM cache plus slow S3 into a system where most warm queries
never pay the S3 round-trip.

## Why a middle tier exists at all

The three tiers exist because their cost/latency curves are an order of magnitude apart. From
[`../01-architecture.md`](../01-architecture.md) and turbopuffer's own docs:

| Tier | Holds | Read latency (turbopuffer's figures) |
|---|---|---|
| DRAM | centroids, active namespaces (hottest) | ~1 ms |
| **NVMe SSD** | **warm namespaces (the ring-buffer file)** | **tens of ms** |
| S3 (cold) | everything; the source of truth | ~50–500 ms (p50 ≈ 874 ms for a 1M-doc *cold* namespace) |

DRAM is fast but small and expensive; embeddings are 20–30× larger than their source text and compress
poorly ([`../01-architecture.md`](../01-architecture.md)), so you cannot hold many namespaces resident.
S3 is effectively unbounded and ~100× cheaper than RAM, but a cold ranged read is hundreds of
milliseconds. NVMe sits in between: large enough to hold the *working set* of many namespaces, ~10–100×
faster than S3, far cheaper per GB than DRAM. The official architecture page states the warm/cold split
directly: a namespace's documents are **cached on NVMe SSD after the first query**, subsequent queries
are **routed to the same node for cache locality**, and the cold→warm latency drop is dramatic (1M
documents: cold p50 ≈ 874 ms → warm p50 ≈ 14 ms). The cold figure is dominated by the S3 round-trips the
NVMe tier exists to eliminate on the next query.

## How the ring buffer works

The mechanism, as described by a third-party deep-dive of turbopuffer (attribution flagged in Sources):

- The cache is **one file the size of the entire disk** allocated to it.
- A single **write pointer** advances through the file. New cached objects are appended at the pointer;
  when it reaches the end of the file it **wraps back to the start** — hence "ring."
- There is **no per-object metadata about recency or frequency to maintain on the hot path** — no LRU
  list to splice, no access timestamps to bump on every read. That is what keeps it ~200 lines and what
  makes it fast.
- The one concession to access patterns: when the write pointer is **about to overwrite
  frequently-accessed data, it skips ahead** rather than evicting it. So it is FIFO-with-a-skip, not a
  pure round-robin overwrite.

In pseudocode (illustrative — turbopuffer does not publish the source):

```
# Insert a freshly fetched object into the ring.
def cache_put(obj):
    while region_at(write_ptr).is_hot():     # the "skip ahead" rule
        write_ptr = advance(write_ptr, region_at(write_ptr).len)
    region = reserve(write_ptr, len(obj))    # may wrap past end-of-file
    write(region, obj)
    index[obj.key] = region                  # key -> on-disk location
    write_ptr = advance(write_ptr, len(obj))

def cache_get(key):
    region = index.get(key)
    if region is None or overwritten_since(region):
        return MISS
    return read(region)                      # one sequential NVMe read
```

Two properties matter. First, writes are **sequential**, which is the access pattern NVMe (and the
filesystem/page cache beneath it) is fastest at — no scattered small writes, no rewrite amplification
from shuffling an LRU. Second, eviction is **implicit**: the oldest data is simply whatever the write
pointer is about to land on, so there is no separate eviction pass and no global lock around a recency
structure.

### Why FIFO can beat LRU at NVMe speeds

The intuition is **lazy promotion / cheap eviction**. LRU does work on *every read* (move the entry to
the front of a list, under a lock); FIFO does essentially nothing on a read and decides what to drop only
at eviction time. At DRAM-to-NVMe speeds, the bookkeeping LRU adds per access can cost more than the
slightly worse hit ratio FIFO accepts, and the lock contention caps throughput under concurrency. This is
not a turbopuffer-specific claim: the SOSP '23 paper *"FIFO Queues are All You Need for Cache Eviction"*
(S3-FIFO), evaluated on 6594 traces from 14 datasets, reports "lower miss ratios than state-of-the-art
algorithms across traces" while achieving **6× higher throughput than optimized LRU at 16 threads**,
precisely because FIFO queues avoid LRU's per-request promotion work and locking (see Sources). turbopuffer's skip-ahead is a small,
hand-rolled lazy-promotion heuristic in the same spirit: keep the structure FIFO-simple, do the minimum
to protect genuinely hot regions.

> Caveat: turbopuffer does not publicly state *why* it chose FIFO over LRU, nor that it was influenced by
> S3-FIFO. The S3-FIFO result is offered here as the general, peer-reviewed reason a FIFO ring can be the
> right call at this tier — not as turbopuffer's stated rationale.

## What our clone does today, and the gap

tpuf implements **two** of the three tiers, exactly as the mapping table in
[`../05-clone-mapping.md`](../05-clone-mapping.md) records ("Three-tier DRAM/NVMe/S3 cache → in-memory
**DRAM tier** ... NVMe ring-buffer tier omitted"):

- **S3 tier** — `internal/storage` (`storage.ObjectStore`, with `s3.go` over MinIO and `memory.go` for
  tests).
- **DRAM tier** — `internal/cache/cache.go`. The whole engine talks to object storage *through* a
  `cache.Store`, which wraps an `storage.ObjectStore`. `Get`, `PutCAS`, `PutIfAbsent`, `Put`, and `List`
  pass straight through (the manifest and WAL must always be read fresh — correctness rule 2). Only
  **`GetCached`** memoizes, and *only* for the immutable `index/v{epoch}/*` objects, which are write-once
  and therefore safe to serve from a map forever.

`GetCached` is the single chokepoint where any cache tier would slot in. In the query path it is the only
reader of index objects: `internal/engine/query.go` calls `store.GetCached` for `centroidsKey` and
`clusterKey` (vector path) and for `bm25Key` and `docsKey` (BM25 path). The DRAM tier is already a
**bounded LRU**: `cache.NewWithCapacity(backend, capacity)` evicts the least-recently-used object on
overflow (`insertLocked` in `cache.go`), and `cache.New` is the unbounded variant. The bench
(`cmd/tpuf-bench`) drives this deliberately: with `--cache-objects` set below the resident working set of
a multi-tenant run, the shared DRAM cache evicts and **cold-start misses recur** — which the package doc
calls out as "exactly the behavior turbopuffer's (here-omitted) NVMe tier exists to soften."

So the gap is precise: today a `GetCached` miss falls **straight through to S3** (`s.backend.Get` in
`cache.go`). There is no warm tier to catch it. Every DRAM eviction becomes a full object-storage
round-trip on the next access. The NVMe ring buffer is the layer that would turn most of those misses
into a fast local-disk read instead.

## How it would hook into tpuf

The clean insertion point is **inside `GetCached`'s miss path**, leaving every caller and the
`storage.ObjectStore` contract untouched. Conceptually a third lookup level between the DRAM map and the
backend:

```
GetCached(key):                       # internal/cache/cache.go
    if key in DRAM map:  return hit                 # tier 1 (exists today)
    if key in NVMe ring: promote to DRAM; return hit # tier 2 (NEW)
    body = backend.Get(key)                          # tier 3: S3 cold (exists today)
    NVMe ring.put(key, body)                          # NEW: populate warm tier
    DRAM map.insert(key, body)
    return body
```

Concretely, in this codebase:

- Add an optional disk tier to `cache.Store` (constructed via a new option alongside
  `NewWithCapacity`), holding the ring file path, the size cap, and the `key -> on-disk region` index. A
  `nil` disk tier preserves today's behavior, so all existing tests and `cache.New` callers are
  unaffected.
- In `GetCached` (`cache.go`), between the DRAM-map check and `s.backend.Get`, consult the ring; on a
  ring hit, promote into the DRAM map and return; on the S3 fetch, write the body into the ring before
  returning.
- This stays correct **only because `GetCached` is contractually immutable-only** (its doc comment:
  "correct ONLY for immutable objects (the `index/v{epoch}/*` files) ... Never call it for the manifest
  or WAL"). Caching mutable objects on disk would reintroduce the staleness problem the DRAM tier already
  avoids by passing `Get`/`PutCAS` straight through.

**CAS / manifest / epoch implications.** The ring buffer needs *zero* coordination changes, and that is
the elegant part:

- The manifest stays the CAS-coordinated source of truth ([`../01-architecture.md`](../01-architecture.md),
  correctness rule 1). It is read through `Store.Get` / `LoadManifest`, never `GetCached`, so it never
  touches the ring.
- Index keys are **epoch-scoped and write-once** (`index/v{epoch}/...`). When `tpuf index` builds a new
  epoch and CAS-swaps the manifest pointer (`internal/engine/indexer.go`, `SaveManifestCAS` in
  `manifest.go`), the new epoch's keys simply differ from the old ones. Stale entries from the previous
  epoch are never *read* again (no query references them after the swap) and age out naturally as the
  write pointer laps them. **Epoch swaps need no cache invalidation** — the write-once key scheme makes
  the ring self-consistent across reindexes, the same reason `GetCached` is safe in the first place.
- The **WAL-tail scan** ([`../01-architecture.md`](../01-architecture.md), query path) keeps bypassing
  the cache entirely — it reads recent WAL segments fresh so brand-new writes are searchable before
  indexing. The ring only ever holds indexed-epoch objects.

### What the bench's cache stats would then show

Today `cache.CacheStats` (in `cache.go`) is a two-way split: a `GetCached` call is a **DRAM hit** or a
**miss that went to S3**, plus an eviction counter. The bench prints this as `N hits / N misses / N
evictions → X% hot` (`printCacheLine` / `printCacheFull` in `cmd/tpuf-bench/main.go`). With an NVMe tier
the interesting number becomes a **three-way** split, because a "miss" is no longer monolithic:

| Outcome | Today | With NVMe tier |
|---|---|---|
| Served from DRAM map | **hit** | **DRAM-hit** |
| Served from local disk | (counts as a miss) | **NVMe-hit** (fast, no network) |
| Fell through to object storage | **miss** | **S3-cold** (the only true cold read) |

The multi-tenant bench would show the payoff most clearly: under `--cache-objects` pressure with tenant
churn, today's run reports DRAM evictions turning into S3 misses; with the ring, those same evictions
would become **NVMe-hits**, and the `S3-cold` count would collapse to roughly the first touch of each
distinct epoch object. The cold→hot experiment (`runColdStart` in `cmd/tpuf-bench/main.go`) would gain a
middle data point: cold = S3, *second-cold* = NVMe (DRAM evicted but disk warm), hot = DRAM.

## What's genuinely hard / what to get right

- **Persistence and restart.** A real ring buffer survives process restarts (that is half its value: a
  node rejoining doesn't re-cold-start from S3). That means a durable on-disk index of `key -> region`
  and validating, on read, that a region still holds the key it claims (the write pointer may have lapped
  it). The pseudocode's `overwritten_since(region)` check is load-bearing — get it wrong and you serve a
  *different epoch's* bytes for a key, which the immutable-key invariant otherwise rules out.
- **Concurrency.** The DRAM tier already fetches outside the lock and tolerates a duplicate concurrent
  miss because immutable bodies are identical (see `GetCached`'s comment in `cache.go`). The ring must
  preserve that: concurrent writers advancing one shared write pointer need careful locking, and the
  skip-ahead "is this region hot?" test must not become the contention point that FIFO was chosen to
  avoid.
- **The skip-ahead heuristic.** "Hot" needs *some* signal, which is exactly the recency/frequency
  bookkeeping FIFO is trying to avoid. turbopuffer keeps it minimal (it is part of the ~200 lines). For a
  faithful-but-clear clone, the honest first cut is **plain FIFO with no skip-ahead** — simpler, and it
  still demonstrates the tier; skip-ahead is a tunable refinement, not the core mechanism.
- **Sizing and alignment.** Region sizing, fragmentation when objects vary in size, and aligning writes
  to the device's erase/page granularity are where a real implementation earns its NVMe latency. A clone
  can ignore alignment and just track byte offsets — but should *say so*, the way the rest of `docs/`
  flags its simplifications.
- **Don't over-claim.** Our clone runs against MinIO over the loopback, not real S3 across a network, so
  even the existing bench notes that "the cache's payoff is avoiding the NETWORK, which only the s3
  backend exposes" (`runColdStart`, `cmd/tpuf-bench/main.go`). An NVMe tier's benefit is most honest when
  measured against the `s3` backend; against the in-memory backend, DRAM, "NVMe," and "S3" are all just
  RAM and the tiers are pedagogical, not performance-bearing.

## Sources

- turbopuffer, **Architecture** — three-tier cache (memory / NVMe SSD / object storage), "cached on NVMe
  SSD" after first query, same-node routing for cache locality, cold p50 ≈ 874 ms vs warm p50 ≈ 14 ms for
  1M documents. <https://turbopuffer.com/docs/architecture> (fetched 2026-06-17).
- turbopuffer, **fast search on object storage** (founding blog) — SSD/memory cache layer; latency tiers
  (object storage ≈ 250 ms p90 for <1 MB; warm vector query 10 ms p90 / cold 444 ms p90; warm full-text
  18 ms p90 / cold 285 ms p90); "maximum of three roundtrips for sub-second cold latency."
  <https://turbopuffer.com/blog/turbopuffer> (fetched 2026-06-17).
- Ajay Edupuganti, **"How Turbopuffer Serves 2.5 Trillion Vectors on S3"** (Medium, 2026) — the ring-buffer
  specifics quoted here: *"The SSD cache is one file the size of the entire disk. Data writes sequentially,
  wrapping around when full — a ring buffer,"* *"When the write pointer is about to overwrite
  frequently-accessed data, it skips ahead,"* and *"About 200 lines of code. No eviction heuristics. No LRU
  complexity."* **Flagged: third-party deep-dive, not turbopuffer's own documentation; turbopuffer does not
  publicly publish the cache source or the 200-line figure. The article attributes these specifics to the
  Database School interview with Simon Eskildsen ("ring buffer cache"), which is a turbopuffer-founder
  account rather than turbopuffer's written docs — treat the exact figures as second-hand.**
  <https://ajay-edupuganti.medium.com/how-turbopuffer-serves-2-5-trillion-vectors-on-s3-7d7ab7f9a7fa>
  (fetched 2026-06-17).
- Yang, Zhang, Qiu, Yue, Rashmi, **"FIFO Queues Are All You Need for Cache Eviction"** (S3-FIFO),
  SOSP '23 — basis for "FIFO can rival/beat LRU": "lower miss ratios than state-of-the-art algorithms
  across traces" evaluated on 6594 traces from 14 datasets, and **6× higher throughput than optimized LRU
  at 16 threads** (abstract), via lazy promotion / quick demotion. Used here as the general reason a FIFO
  ring suits this tier — **not** stated by turbopuffer as its rationale. <https://yazhuozhang.com/assets/publication/sosp23-s3fifo.pdf>;
  repo: <https://github.com/Thesys-lab/sosp23-s3fifo> (fetched 2026-06-17).
- This repo: [`../01-architecture.md`](../01-architecture.md) (three-tier cache table, cold-vs-warm query
  path, CAS coordination), [`../05-clone-mapping.md`](../05-clone-mapping.md) (NVMe tier listed as a
  deliberate non-goal), `internal/cache/cache.go` (`Store`, `GetCached`, `CacheStats`, `NewWithCapacity`,
  `insertLocked`), `internal/storage/storage.go` (`ObjectStore` contract), `internal/engine/query.go`
  (`GetCached` call sites), `internal/engine/indexer.go` + `internal/engine/manifest.go` (epoch swap via
  `SaveManifestCAS`), and `cmd/tpuf-bench/main.go` (`printCacheLine`, `printCacheFull`, `runColdStart`,
  the `--cache-objects` multi-tenant regime).
