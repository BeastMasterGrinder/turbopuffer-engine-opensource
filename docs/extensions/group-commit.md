# Extension — Group commit for the WAL

> **Implemented (2026-06-18).** An opt-in per-namespace committer in `internal/engine/commit.go`
> coalesces concurrent upserts into one WAL PUT + one manifest CAS (still `PutIfAbsent`,
> durable-before-return); `cmd/tpuf-bench --group-commit` shows ~8× fewer durable PUTs at high
> concurrency. It is intentionally NOT wired into the CLI's default per-command path. The text below is
> the design rationale (incl. turbopuffer's reported ~1 WAL entry/sec/namespace — their figure).

> One of the deliberate non-goals from [`05-clone-mapping.md`](../05-clone-mapping.md): *"**Group
> commit** (1 WAL entry/sec/namespace) — our CLI commits per `upsert`. Hook: buffer batches in a
> goroutine before the WAL write in `namespace.Upsert`."* This doc expands that hook into a real
> design entry.

**What this is.** Group commit batches many concurrent writes to a namespace into a *single* durable
object-storage write, instead of one write per caller. The point is to **decouple write throughput
from per-PUT object-storage latency**: a PUT to S3/MinIO costs ~hundreds of milliseconds whether it
carries one document or a thousand, so amortizing that fixed cost over a batch is almost pure win for
throughput. turbopuffer does exactly this — its [architecture page](https://turbopuffer.com/docs/architecture)
states *"Each namespace can currently write 1 WAL entry per second. Concurrent writes to the same
namespace are batched into the same entry,"* and labels the step *"group commit (<= 1/s)"* in the
write-flow diagram. This is the same classical technique relational engines use to amortize the cost
of an `fsync` across transactions; here the expensive primitive is an object-storage PUT, not a disk
sync.

---

## 1. The mechanism (why batching the slow primitive wins)

Group commit is an old database idea. The shared shape across systems: **while one durable write is
in flight, buffer every new request that arrives; when the write completes, flush the whole buffer as
the next single durable write.** The cost of the slow operation (a log `fsync`, or here an object PUT)
is then paid *once per batch* rather than once per request.

The canonical statements:

- **MariaDB** (binary-log group commit): *"The idea with group commit is to amortize the costs of
  each flush to disk over multiple commits from multiple parallel transactions. For example, if there
  are 10 transactions trying to commit in parallel, then we can force all of them to be flushed disk
  at once with a single system call, rather than do one system call for each commit."*
  ([MariaDB KB](https://mariadb.com/kb/en/group-commit-for-the-binary-log/))
- **PostgreSQL** (`commit_delay`): *"the purpose of `commit_delay` is to allow the cost of each flush
  operation to be amortized across concurrently committing transactions (potentially at the expense
  of transaction latency)."* Even with `commit_delay = 0`, *"a form of group commit"* still happens —
  a group forms from *"sessions that reach the point where they need to flush their commit records
  during the window in which the previous flush operation (if any) is occurring."*
  ([PostgreSQL WAL config docs](https://www.postgresql.org/docs/current/wal-configuration.html))

turbopuffer applies the identical pattern to object storage. Its
[object-storage-queue blog](https://turbopuffer.com/blog/object-storage-queue) describes the buffering
loop directly: *"Whenever a write is in flight, we buffer incoming requests in memory. As soon as the
write finishes, we flush the buffer as the next CAS write,"* and names it: *"This technique is commonly
called group commit, and it's the same pattern turbopuffer uses for batching writes to the WAL."* The
payoff it reports: *"Group commit solves our throughput problem by decoupling write rate from request
rate. The scaling bottleneck shifts from write latency (~200ms/write) to network bandwidth (~10
GB/s)."*

The trade is **latency for throughput**. A caller that arrives just after a flush starts waits for
that flush *plus* the next one — turbopuffer notes *"If a new batch is started within one second of
the previous one, it will take up to 1 second to commit."* PostgreSQL's `commit_delay` makes the same
trade explicit: it *adds* a deliberate sleep before the flush to widen the batching window.

```
time ───────────────────────────────────────────────────────────────►
caller A ─┐
caller B ─┼─► [ in-memory buffer ]                 (one PUT in flight)
caller C ─┘            │
                       └─► flush as ONE WAL segment ──► one PutIfAbsent ──► one manifest CAS
caller D ──────────────────► [ buffer ] ──► (next flush) ...
```

A useful framing: **a queue of pending writes coordinated on object storage is itself just group
commit applied to a JSON file** — the queue accumulates requests during the in-flight window and
commits them as one CAS write. (turbopuffer's broker/`queue.json` indexing-dispatch path, sketched in
[`01-architecture.md`](../01-architecture.md), uses the same buffering trick; this doc is only about
the *WAL write* path.)

> **Flagged as not publicly confirmed:** turbopuffer publishes the *behavior* ("1 WAL entry/sec/namespace",
> "buffer in flight → flush next") and the headline numbers (~200 ms/write, ~10 GB/s bandwidth ceiling,
> 10,000+ vectors/sec). It does **not** publish the internal data structures of its in-memory buffer,
> how leadership/flush is scheduled, or whether the 1 s cadence is a fixed timer vs. purely
> in-flight-driven. The diagram label "<= 1/s" implies a cap, not necessarily a fixed period. Treat the
> goroutine design below as *our* faithful reconstruction, not turbopuffer's actual code.

---

## 2. What our clone does today, and the gap

Today there is **no batching**: every call to `Namespace.Upsert` performs its own WAL append and its
own manifest CAS. From [`internal/engine/namespace.go`](../../internal/engine/namespace.go), `Upsert`:

1. loads the manifest fresh (`LoadManifest`),
2. validates dimensions (`validateDocs`),
3. claims a WAL seq with `AppendWAL` → `store.PutIfAbsent(walKey(ns, seq), …)` (write-once, correctness
   rule 1), climbing to the next seq on a 412, and
4. advances the manifest with `SaveManifestCAS` (bump `WALSeq` + `DocCount`).

So **N concurrent upserts cost N WAL PUTs and N manifest CAS round-trips** — and worse, under
contention they actively fight: each `AppendWAL` 412 forces a probe to the next seq (bounded by
`maxUpsertAttempts = 64`), and each `SaveManifestCAS` 412 forces a reload-and-retry (bounded by
`maxCASAttempts = 10`). Every writer pays the full ~hundreds-of-ms PUT latency serially. This is the
exact throughput wall group commit removes: turbopuffer's *"decoupling write rate from request rate."*

For an educational single-process clone driven by a CLI (`tpuf upsert demo --file …`), this is the
*right* default — it keeps `Upsert` a linear, traceable read-modify-CAS that mirrors the five
correctness rules in [`06-implementation-blueprint.md`](../06-implementation-blueprint.md). Group
commit is the optimization you reach for once many goroutines write the same namespace concurrently.

---

## 3. How it would hook into tpuf

The hook is a per-namespace **committer goroutine** that sits *in front of* the existing `AppendWAL` +
`SaveManifestCAS` sequence. It does not change those primitives or the on-disk shapes — it changes
*who calls them and how often*.

### Shape (pseudocode, not Go)

```
type pendingWrite:
    docs   []Document
    result chan error      # caller blocks here until its batch is durable

# one committer goroutine per active namespace
committer(ns):
    loop:
        first := <- ns.inbox            # block until at least one write arrives
        batch := [first]
        # drain whatever else is already queued (and/or wait a small window)
        drain ns.inbox into batch  (until empty, or a max-batch / max-window bound)

        ops    := concat(b.docs for b in batch)        # one merged []Document
        err    := commitOnce(ns, ops)                  # the existing WAL + CAS path, ONCE
        for b in batch: b.result <- err                # wake every caller with the shared outcome

# Upsert becomes a thin enqueue-and-wait
Upsert(ctx, docs):
    validate docs vs manifest.Dimension     # keep this BEFORE enqueue (see risks)
    w := pendingWrite{docs, make(chan error, 1)}
    ns.inbox <- w
    select { case err := <-w.result: return err; case <-ctx.Done(): return ctx.Err() }
```

`commitOnce` is **today's body of `Upsert` after validation**: the `AppendWAL` seq-claim loop followed
by the single `SaveManifestCAS` that bumps `WALSeq` and `DocCount`. The merged `ops` slice becomes the
`WALSegment.Ops` of one segment — `WALSegment` already holds `Ops []Document`
([`types.go`](../../internal/engine/types.go)), so a 1-document batch and a 500-document batch use the
identical wire shape. **One PUT, one CAS, for the whole batch.**

### Where it physically lives

- The buffer/goroutine is per-namespace state. The current `Namespace` is deliberately stateless ("a
  thin, stateless façade … safe to share across goroutines" — `namespace.go`), so the committer
  cannot hang off a `Namespace` value. It belongs on a longer-lived owner — e.g. the `cache.Store`, or
  a new small registry (`map[string]*committer` guarded by a mutex, lazily started per namespace). Open
  would look it up rather than allocate per call.
- `AppendWAL`, `MaterializeLive*`, `SaveManifestCAS`, `LoadManifest` are unchanged.

### CAS / manifest / epoch implications

- **Correctness rule 1 still holds, and gets easier.** With a single committer per namespace, only one
  goroutine ever claims a WAL seq at a time, so the `AppendWAL` 412 probe loop almost never fires
  *within a process*. It must stay anyway: another process (a second `tpuf` invocation) can still race
  for the same seq, and `PutIfAbsent` is what keeps that safe. Group commit reduces *intra-process*
  contention; it does not replace the write-once guarantee.
- **One manifest CAS per batch, not per write.** `SaveManifestCAS` already reloads fresh each iteration
  (correctness rule 2 — never cache the manifest), so merging callers changes only the *count* of CAS
  round-trips, not their logic. `DocCount` must be summed across the whole batch (sum the per-doc
  `+1`/`-1` `liveDelta` over all merged docs), exactly as the current single-call code does for its own
  docs.
- **Last-writer-wins inside the batch.** If two callers in one batch upsert the same `ID`, the merge
  order decides the survivor. `MaterializeLive*` already resolves dup IDs *within* a segment by
  iterating `seg.Ops` in order ([`wal.go`](../../internal/engine/wal.go)), so as long as the committer
  concatenates batch members in arrival order, the existing materialize logic gives a well-defined
  result — no new rule needed (this is correctness rule 5 applied intra-segment).
- **No epoch/indexer impact.** Group commit touches only the WAL-write path. `IndexedUpTo` is still the
  `WALSeq` snapshot at index start (rule 3), the index still publishes via one manifest CAS (rule 4),
  and queries still scan the live index plus the tail `[IndexedUpTo, WALSeq)` (rule 5). Bigger, fewer
  segments actually *help* the indexer (fewer objects to `List`/`Get` when materializing).

---

## 4. What's genuinely hard / what to get right

- **Durability semantics must not regress.** `Upsert` is durable-before-return today (returns only
  after the WAL PUT + manifest CAS are acked). A caller must therefore block until *its* batch's
  `commitOnce` succeeds — it cannot return when merely *enqueued*. The `result` channel enforces this:
  every member of a batch receives the same error/success only after the shared durable write lands.
- **All-or-nothing failure fate is shared.** If `commitOnce` fails (storage error, CAS exhaustion),
  *every* caller in that batch gets the same error and must treat the whole batch as not committed.
  Upsert is idempotent by `ID` (last-writer-wins materialize), so a blanket caller-level retry is safe,
  but the design must never report success to some batch members and failure to others.
- **Validate before enqueue, or one bad doc poisons the batch.** Dimension validation (`validateDocs`)
  reads `manifest.Dimension`. A present-but-wrong-length vector must be rejected to *that caller only*
  (the current contract: error names both dimensions). Validating inside the merged batch would fail
  the whole group. So keep validation in `Upsert` *before* the doc enters `inbox`. (The manifest read
  for validation can be cheap/shared, but the manifest is never cached for CAS — rule 2.)
- **Latency tax and the batching window.** The classic trade (PostgreSQL `commit_delay`): a deliberate
  wait widens the batch but slows the fastest writer. The simplest honest choice for the clone is **no
  artificial delay** — drain only what is *already* queued when the previous flush finishes, matching
  PostgreSQL's `commit_delay = 0` "form of group commit" and keeping the demo's latency story truthful.
  A `maxBatchDocs` / `maxBatchBytes` cap is still worth having so a giant batch can't blow past a single
  reasonable object size.
- **Shutdown / context cancellation.** A `ctx.Done()` caller may walk away while its write is mid-flush;
  the committer still completes the durable write (the data is in the segment), so the result is
  "committed but the caller stopped waiting" — acceptable and idempotent, but the goroutine must drain
  and close cleanly on shutdown rather than leak per namespace.
- **Concurrency tests are the payoff.** Per [`07-project-layout-and-testing.md`](../07-project-layout-and-testing.md)
  and the project's `go test ./... -race` discipline, the headline test is: fire M goroutines at
  `Upsert`, assert (a) all M sets of docs are durable and queryable, (b) materialize is last-writer-wins
  on duplicate IDs, and (c) the number of WAL segments produced is far less than M (the batching
  actually happened). The in-memory `ObjectStore` makes this runnable with no MinIO.

**Bottom line for the clone:** group commit is a thin buffering layer in front of the *existing*
`AppendWAL` + `SaveManifestCAS` path. It earns its complexity only under concurrent multi-writer load —
which the CLI does not generate — so it stays a documented extension, faithful to turbopuffer's design
but out of the default build, exactly as [`05-clone-mapping.md`](../05-clone-mapping.md) records.

---

## Sources

- turbopuffer — Architecture: *"Each namespace can currently write 1 WAL entry per second. Concurrent
  writes to the same namespace are batched into the same entry."*; *"If a new batch is started within
  one second of the previous one, it will take up to 1 second to commit."*; *"~10,000+ vectors/sec …
  p50=165ms for 500kB"*; diagram label *"group commit (<= 1/s)"*. <https://turbopuffer.com/docs/architecture>
- turbopuffer — *How to build a distributed queue in a single JSON file on object storage*: *"Whenever a
  write is in flight, we buffer incoming requests in memory. As soon as the write finishes, we flush the
  buffer as the next CAS write."*; *"This technique is commonly called group commit, and it's the same
  pattern turbopuffer uses for batching writes to the WAL."*; *"Group commit solves our throughput
  problem by decoupling write rate from request rate. The scaling bottleneck shifts from write latency
  (~200ms/write) to network bandwidth (~10 GB/s)."* <https://turbopuffer.com/blog/object-storage-queue>
- PostgreSQL 18 — *28.5. WAL Configuration*: `commit_delay` amortizes *"the cost of each flush operation
  … across concurrently committing transactions"*; with `commit_delay = 0`, *"a form of group commit"*
  still occurs. <https://www.postgresql.org/docs/current/wal-configuration.html>
- MariaDB Knowledge Base — *Group Commit for the Binary Log*: *"The idea with group commit is to amortize
  the costs of each flush to disk over multiple commits from multiple parallel transactions … force all
  of them to be flushed disk at once with a single system call, rather than do one system call for each
  commit."* <https://mariadb.com/kb/en/group-commit-for-the-binary-log/>
- This clone's code (read directly): WAL write path and seq-claim loop in
  [`internal/engine/namespace.go`](../../internal/engine/namespace.go) (`Upsert`, `maxUpsertAttempts`)
  and [`internal/engine/wal.go`](../../internal/engine/wal.go) (`AppendWAL`, `MaterializeLive*`); manifest
  CAS in [`internal/engine/manifest.go`](../../internal/engine/manifest.go) (`SaveManifestCAS`,
  `maxCASAttempts`); shapes in [`internal/engine/types.go`](../../internal/engine/types.go) (`WALSegment`,
  `Manifest`).
- Architecture + non-goal context: [`01-architecture.md`](../01-architecture.md) (write path / group
  commit flag), [`05-clone-mapping.md`](../05-clone-mapping.md) (the non-goal + hook),
  [`06-implementation-blueprint.md`](../06-implementation-blueprint.md) (the five correctness rules),
  [`07-project-layout-and-testing.md`](../07-project-layout-and-testing.md) (race testing strategy).

> *Inferred / not confirmed by turbopuffer:* the committer-goroutine structure, the `result`-channel
> blocking model, the drain-without-delay choice, and "fewer segments help the indexer" are this
> document's reconstruction, consistent with the published behavior but not stated by turbopuffer.
