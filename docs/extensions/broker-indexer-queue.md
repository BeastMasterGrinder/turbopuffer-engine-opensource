# Extension — The broker, indexer processes, and `queue.json`

> **Implemented (2026-06-18).** Async-indexing daemons `cmd/tpuf-broker` + `cmd/tpuf-indexer` coordinate
> through a CAS-guarded `queue.json` (same `If-Match`/412 shape as the manifest; primitives in
> `internal/engine/queue.go`): indexers claim a job via CAS (exactly one wins), a backlog drains, a
> crashed claim is reclaimable, and index publish stays one manifest CAS. Run it via the compose
> `indexer` profile + `deploy/queue-demo.sh`. The text below is the design rationale; the `queue.json`
> format is our design (turbopuffer does not publish theirs).

> An expansion of the [`05-clone-mapping.md`](../05-clone-mapping.md) non-goal:
> *"**Live broker + indexer processes / `queue.json`** — we index inline. Hook: a daemon that watches
> `IndexedUpTo` vs `WALSeq` and CAS-claims jobs."*

**What this is.** turbopuffer keeps indexing *off the query/write path*. Writers commit to the WAL on
object storage and return; a separate fleet of **indexer nodes** asynchronously folds that WAL into
search indexes. The two fleets are decoupled by an **indexing job queue** — itself a single JSON object
on object storage (`queue.json`), coordinated by compare-and-swap, fronted by a **stateless broker** that
group-commits all queue mutations. This is the same "object storage is the source of truth, coordinated by
CAS" bet as the manifest, applied to *work scheduling* instead of *data*. Why bother: indexing is
CPU-heavy (k-means / SPFresh, BM25 inversion); running it on the query nodes would make heavy reindexing
compete with the ~10–100 ms reads turbopuffer sells ("compute-compute separation"). Our clone collapses
all of this into one synchronous `tpuf index` call — faithful in *result* (an atomic epoch swap), absent
in *scheduling*.

## How turbopuffer does it

Three stateless compute roles share one object store as the only durable state, so any node can do any
namespace's work:

| Role | Does | Touches `queue.json`? |
|---|---|---|
| **Query node** | Serves reads; accepts writes into a buffer, commits them to the WAL; signals that a namespace needs (re)indexing | enqueues (via broker) |
| **Indexer node** | Claims a job, reads the WAL delta, builds new index state on object storage, marks the job done | claims + heartbeats + completes (via broker) |
| **Broker** | Single writer to `queue.json`; runs one group-commit loop on behalf of all clients | the *only* direct writer |

Sources confirm the split: *"After data is committed to the WAL, it is asynchronously indexed by separate
indexing nodes,"* and *"Indexing nodes maintain the indexes asynchronously, writing to object storage new
index states that query nodes discover"* — with *"compute-compute separation [meaning] expensive indexing
operations don't impact query performance"* ([Concepts](https://turbopuffer.com/docs/concepts)). The
[Architecture](https://turbopuffer.com/docs/architecture) page shows the indexer binaries as a fleet
distinct from the query nodes. (turbopuffer's docs do not use the words "auto-scaled" for the indexer
fleet in these pages, nor state here that query nodes themselves accept writes; both are inferred from the
broader architecture and treated as plausible, not verbatim-confirmed.)

### The queue is a notification system, not the write path

A critical clarification from turbopuffer: *"The queue is not part of the write path; it's purely a
notification system used to schedule asynchronous indexing work"*
([Object-storage queue blog](https://turbopuffer.com/blog/object-storage-queue)). Durability comes from
the WAL commit; the queue only tells indexers *there is work to do*. If the queue lagged or dropped a
notification, no data is lost — at worst, indexing is delayed and queries fall back to the exhaustive WAL
scan (turbopuffer: *"Unindexed data is still searched exhaustively for strongly consistent queries"* —
[Concepts](https://turbopuffer.com/docs/concepts)). That fallback is exactly what makes a *best-effort*
queue acceptable.

### `queue.json`: a distributed queue in one JSON file

turbopuffer's blog [*"How to build a distributed queue in a single JSON file on object storage"*](https://turbopuffer.com/blog/object-storage-queue)
walks the design (the file is named **`queue.json`** there). The core idea is the same CAS loop the
manifest uses:

```
# enqueue / claim / complete — all the same primitive
read queue.json (body + CAS token)
mutate the job list in memory
conditional-write queue.json (only if unchanged since the read)
  success → done
  changed → re-read and retry
```

Quoting the blog: *"The write only succeeds if `queue.json` hasn't changed since it was read. If it has
changed, the client reads the new contents and tries again."* That is `UPDATE … WHERE version = N` — and
it is precisely the manifest CAS our clone already implements (see `SaveManifestCAS` in
[`internal/engine/manifest.go`](../../internal/engine/manifest.go)).

**Job lifecycle.** A job moves through states the blog draws as `○` (unclaimed) → `◐` (claimed,
in-progress) → claimed-with-heartbeat → removed when done. A worker *claims* a job by CASing its state
from unclaimed to in-progress; **the loser of a concurrent claim simply re-reads `queue.json` and retries**
— no lock, no lease server. Liveness uses **heartbeats**: *"If the last heartbeat for a job in the queue
is ever more than some timeout, we assume the original worker is gone and the next worker takes over where
it left off."* (turbopuffer does not publish the exact timeout.) The guarantees are **FIFO execution** and
**at-least-once** delivery — at-least-once is why indexing must be **idempotent** (see risks below).

### Why a broker (and group commit)

Naive per-client CAS on one object does not scale. The blog: a CAS write is ~200 ms and CAS forces *"each
write to be non-overlapping in time,"* capping throughput at *"~5 writes per second"*; GCS additionally
limits a single object to *~1 request/second*; and *"tens or hundreds of clients will contend over the
single queue object."* The fix is a **stateless broker** that becomes the sole writer: *"All clients must
now liaise with the broker instead of writing to object storage directly,"* and it *"runs a single group
commit loop on behalf of all clients, so no one contends for the queue object… it doesn't acknowledge a
write until the group commit has landed in object storage."* In-flight requests buffer in memory and flush
as the next CAS write — the same group-commit shape turbopuffer uses for the WAL (1 entry/sec/namespace).
Reported result: *"10x lower tail latency versus our prior implementation."*

> Flag: the precise enqueue/dequeue payload schema and exact field names inside `queue.json` are **not
> published**. The filename `queue.json`, the CAS-retry mechanic, group commit, heartbeats, FIFO, and
> at-least-once are confirmed by the blog; the rest is inferred from it. The specific claim that *query
> nodes* are the parties that "request reindexing" appears in secondary write-ups
> ([Medium: serving 2.5T vectors](https://ajay-edupuganti.medium.com/how-turbopuffer-serves-2-5-trillion-vectors-on-s3-7d7ab7f9a7fa))
> rather than verbatim in turbopuffer's own docs — treat the *who-enqueues* detail as plausible, not
> officially confirmed.

## What our clone does today, and the gap

We index **inline and synchronously**. There is no queue, no broker, no daemon:

- `tpuf index <ns>` (CLI in [`cmd/tpuf/main.go`](../../cmd/tpuf/main.go)) calls
  `Namespace.Index` → `BuildIndex` ([`internal/engine/indexer.go`](../../internal/engine/indexer.go)),
  which runs k-means + BM25 *in the calling process* and blocks until the epoch is published.
- The honest mapping already calls this out: *"Async indexing… ⚠️ a command, not a live broker/indexer"*
  ([`05-clone-mapping.md`](../05-clone-mapping.md)).
- We *do* faithfully reproduce the **end state**: `BuildIndex` snapshots `walUpTo = m.WALSeq` at start
  (correctness rule 3), writes every `index/v{epoch}/*` object under a fresh prefix with unconditional
  `Put`, then flips `IndexEpoch`/`IndexedUpTo` in one `SaveManifestCAS` (rule 4 — the atomic swap). So the
  *publish* is already CAS-coordinated; only the *scheduling* is missing.
- The manifest already carries the exact signal a scheduler needs: `WALSeq` vs `IndexedUpTo`
  ([`internal/engine/types.go`](../../internal/engine/types.go), `Manifest`). The lag
  `WALSeq - IndexedUpTo` is the clone's analog of turbopuffer's `unindexed_bytes`. Queries already overlay
  that tail via `RunQuery` (rule 5), so an async indexer would change *latency-to-indexed*, never
  *correctness*.

We even already have a long-lived process to host this: `cmd/tpuf-node`
([`cmd/tpuf-node/main.go`](../../cmd/tpuf-node/main.go)) is a stateless HTTP query node over the shared
store. It is read-only today (query + info); it does not write the WAL or index.

## How it would hook into tpuf

The goal is to keep the engine API intact and add scheduling *around* it. Two pieces, both reusing the
existing CAS helpers:

**1. A queue object + helpers, mirroring the manifest.** Add `{ns}/queue.json` (or one shared
`_queue/queue.json` for all namespaces) with the same load/CAS pattern as
[`internal/engine/manifest.go`](../../internal/engine/manifest.go). Reuse the storage contract verbatim:
`PutCAS` (maps to S3 `If-Match`, returns `ErrPreconditionFailed` on 412) for claim/complete, `PutIfAbsent`
to create the queue once, and a bounded retry loop like `maxCASAttempts`
([`internal/storage/storage.go`](../../internal/storage/storage.go)). A job is just
`{ns, requestedUpTo, state, worker, heartbeatAt}`.

```
# indexer daemon loop (pseudocode — NOT Go)
for {
    job := claimNextUnclaimed(queue)        # CAS ○→◐; on 412 reload+retry, loser backs off
    if job == nil { sleep; continue }
    heartbeat(job) every T                  # CAS-refresh heartbeatAt while building
    engine.Open(store, job.ns).Index(ctx)   # the EXISTING BuildIndex — unchanged
    complete(job)                            # CAS ◐→done / remove
}
```

**2. Enqueue when the WAL runs ahead.** A writer (or a watcher daemon) compares the manifest after each
upsert and enqueues if behind:

```
m := engine.Open(store, ns).Info(ctx)       # reads manifest fresh (uncached, rule 2)
if m.WALSeq - m.IndexedUpTo >= threshold {  # clone analog of unindexed_bytes
    enqueueReindex(queue, ns, m.WALSeq)     # idempotent: re-CAS; dedupe on ns
}
```

CAS/manifest/epoch implications to preserve:
- **The indexer calls the unchanged `BuildIndex`.** It already snapshots `WALSeq` at start and publishes
  via a single manifest CAS, so two indexers that race the same namespace either both produce a valid
  epoch (the second's CAS bumps `IndexEpoch` again — wasteful but correct) or one's CAS loses and retries.
  At-least-once duplicate execution is therefore *safe* because the build is a pure function of the WAL
  prefix `[0, walUpTo)`.
- **Never cache `queue.json`.** Like the manifest (rule 2 in
  [`internal/engine/manifest.go`](../../internal/engine/manifest.go)), every CAS iteration must observe the
  current ETag, so it must go through `Store.Get`, never `GetCached`.
- **Keep durability on the WAL, not the queue.** `Upsert` stays durable-before-return; enqueueing is a
  best-effort *notification* after the fact, exactly as turbopuffer frames it.
- A real broker (one group-commit goroutine fronting all `queue.json` writes) is an optional second step;
  for a single-process educational clone the bare CAS loop already demonstrates the mechanism, and the
  broker only earns its keep under the hundreds-of-clients contention turbopuffer cites.

## What's genuinely hard / what to get right

- **Idempotent, at-least-once execution.** A worker can die after building but before `complete`; the next
  worker re-runs. Safe here *only because* `BuildIndex` is deterministic over a WAL prefix and publishes
  with an atomic CAS — preserve that property if the indexer ever gains incremental SPFresh-style updates
  (the [`05`](../05-clone-mapping.md) "LIRE incremental" hook), where a partially-applied split/merge would
  *not* be a pure rebuild.
- **Heartbeat liveness vs. false reclaim.** Too short a timeout reclaims a slow-but-alive worker (then two
  workers build the same namespace); too long stalls indexing after a real crash. turbopuffer does not
  publish its timeout; pick one deliberately and document it.
- **Single-object contention is real.** The naive CAS-only queue tops out around the per-object write rate
  (turbopuffer: ~5 writes/s, GCS ~1 req/s). Per-namespace `queue.json` sidesteps it for the clone; one
  global queue would need the broker/group-commit to scale.
- **Lost-notification recovery.** Because the queue is best-effort, you must not *rely* on it for
  correctness. A periodic sweep that enqueues any namespace whose `WALSeq - IndexedUpTo` exceeds the
  threshold (independent of whether a prior notification was delivered) is the safety net — and queries
  remain correct meanwhile via the WAL-tail scan.
- **Don't reinvent coordination.** The temptation is to add a lock service or a real queue (SQS/Kafka). The
  whole point — and the clone's thesis — is that one CAS'd JSON object is enough. Reuse `PutCAS` /
  `ErrPreconditionFailed`; do not add a dependency.

## Sources

- turbopuffer — *How to build a distributed queue in a single JSON file on object storage*:
  https://turbopuffer.com/blog/object-storage-queue — **primary source.** Confirms the filename
  `queue.json`, the CAS read-modify-conditional-write loop (*"The write only succeeds if `queue.json`
  hasn't changed since it was read…"*), the `○`/`◐` job-state symbols, heartbeats and the dead-worker
  takeover rule, FIFO + at-least-once guarantees, the *"not part of the write path… purely a notification
  system"* framing, the ~5 writes/s and ~1 req/s (GCS) contention limits, the stateless broker running
  *"a single group commit loop on behalf of all clients,"* and the *"10x lower tail latency"* result.
- turbopuffer — *Architecture*: https://turbopuffer.com/docs/architecture — shows the indexer binaries as
  a fleet distinct from the query nodes, and that *"Any data that has not yet been indexed is still
  available to search… with a slower exhaustive search of recent data in the log."* (This page does not
  use "auto-scaled," does not say "expensive computation," and does not explicitly state that query nodes
  accept writes — those framings are sourced from Concepts and the secondary write-up below, not here.)
- turbopuffer — *Concepts*: https://turbopuffer.com/docs/concepts — *"After data is committed to the WAL,
  it is asynchronously indexed by separate indexing nodes,"* *"Indexing nodes maintain the indexes
  asynchronously, writing to object storage new index states that query nodes discover,"* *"Unindexed data
  is still searched exhaustively for strongly consistent queries,"* and the `unindexed_bytes` metadata
  field.
- *How Turbopuffer Serves 2.5 Trillion Vectors on S3* (Ajay Edupuganti, Medium, 2026) — **secondary,
  unofficial:** https://ajay-edupuganti.medium.com/how-turbopuffer-serves-2-5-trillion-vectors-on-s3-7d7ab7f9a7fa
  — source for the explicit "query nodes send requests to the broker to indicate a namespace needs
  reindexing" framing. Flagged above as not verbatim-confirmed in turbopuffer's own docs.
- This clone's code, read directly: `internal/engine/manifest.go` (`SaveManifestCAS`, `LoadManifest`,
  `maxCASAttempts`, the never-cache rule), `internal/engine/indexer.go` (`BuildIndex`, the `walUpTo`
  snapshot and single-CAS epoch swap), `internal/engine/types.go` (`Manifest.WALSeq` / `IndexedUpTo` /
  `IndexEpoch`), `internal/engine/namespace.go` (`Upsert`, `Index`, `Info`), `internal/storage/storage.go`
  (`PutCAS` / `PutIfAbsent` / `ErrPreconditionFailed`), `cmd/tpuf-node/main.go` (the stateless node that
  would host the daemon), and [`05-clone-mapping.md`](../05-clone-mapping.md) / [`01-architecture.md`](../01-architecture.md)
  for the existing prose this expands.

> **Uncertainty markers.** The internal field layout of `queue.json`, the heartbeat timeout value, and the
> exact party that enqueues reindex jobs are **not publicly documented** by turbopuffer and are marked as
> inferred where they appear above. Everything attributed to the blog/docs is quoted from the pages linked
> here.
