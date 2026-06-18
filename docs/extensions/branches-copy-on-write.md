# Extension — Branches (copy-on-write namespaces)

> **Implemented (2026-06-18).** `tpuf branch <parent> <child>` forks in O(1) (one manifest PUT) via
> `internal/engine/branch.go`: the child shares the parent's immutable WAL/index objects and diverges
> only on write — verified end-to-end against MinIO (child sees parent data at the fork point; the
> child's writes never appear in the parent). The text below is the design rationale. **Hazard:** GC is
> still unbuilt, and a branch *pins* its parent's objects — do not add object deletion without
> branch-awareness.

A **branch** is a cheap, near-instant fork of a namespace: a new namespace that initially shares all of
the parent's immutable WAL and index objects on object storage, and diverges only as new data is
written into either side. Because tpuf's whole design rests on the bet that *object storage is the
source of truth and a namespace is just a prefix + a manifest pointer*, a fork costs nothing to copy —
you write a new manifest that **points at the parent's existing objects** and let copy-on-write handle
divergence. turbopuffer ships this as **Namespace Branching**: "an instant, copy-on-write clone of a
namespace via `branch_from`", **constant-time regardless of namespace size**, after which "both the
source and branched namespaces are fully independent" ([turbopuffer.com/docs/branching](https://turbopuffer.com/docs/branching)).
The canonical use cases are dev/test sandboxes over real data: embed a codebase once and branch per
local checkout so only changed files get re-indexed; branch production, run tests, tear it down; give
each developer a sandbox of a shared dataset (same source).

This document is a **design note**, not implemented code — tpuf today has no branch command. Branches
appear in `docs/05-clone-mapping.md` under the deliberate non-goals ("branches/CMEK … documented, not
built"). What follows is how the feature works in turbopuffer, where the clone stands, and the concrete
manifest/prefix/GC changes a faithful tpuf implementation would need.

## How it works in turbopuffer

The mechanism is the same trick that makes namespaces cheap multi-tenancy in the first place: a
namespace is a **prefix** on object storage with a manifest as its head pointer, and all the heavy
objects under it (WAL segments, index files) are **write-once and immutable**. Immutable objects can be
shared by reference; you never need to copy bytes to fork.

turbopuffer's branching page illustrates this with a diagram in which the source namespace and the new
branch **both point at the same underlying object** (rendered as `source/1.bin` and `branch/1.bin`
referring to shared data), diverging only when one side writes
([turbopuffer.com/docs/branching](https://turbopuffer.com/docs/branching)). The published properties:

| Property | turbopuffer behavior | Source |
|---|---|---|
| Creation | `branch_from` parameter; **instant, constant-time regardless of namespace size** | docs/branching |
| Sharing | copy-on-write: branch and source initially reference the same immutable objects | docs/branching (diagram) |
| Independence | after creation, "reads, writes, queries, and deletes on either namespace don't affect the other" | docs/branching |
| Multi-level | branch from branches; "no limit on child branches per namespace … nor on length of branch chains" | docs/branching |
| Deletion | "Deleting a branch does not affect the source namespace, and deleting the source does not affect any branches" | docs/branching |
| Billing | flat creation fee, then "billed on logical bytes at standard rates — each branch is billed as if it were an independent namespace" | docs/branching |

That last row is the tell: branches are billed on **logical** bytes (what the branch logically
contains), *not* physical bytes (what is uniquely stored after sharing). turbopuffer notes plans to
reduce this "once branching in production has been observed"
([turbopuffer.com/docs/branching](https://turbopuffer.com/docs/branching)) — i.e. the physical sharing
is real but not yet passed through to the bill. *(turbopuffer does not publicly document the internal
manifest format, the exact key layout of a branch, or its garbage-collection algorithm; the
prefix/manifest mechanics below are inferred from turbopuffer's stated architecture — "each namespace
has its own prefix on object storage", a WAL of append-only files, asynchronously built immutable
indexes — and from the general copy-on-write pattern, not from turbopuffer source.)*

Conceptually a branch's manifest is a parent pointer plus a fork point in the parent's WAL:

```text
branch(parent, child):
    pm := load parent manifest        # the fork point is parent's current head
    cm := new manifest {
        parent:        parent
        forkWALSeq:    pm.WALSeq       # child inherits parent WAL [0, forkWALSeq)
        forkIndexEpoch:pm.IndexEpoch   # child inherits parent's live index epoch
        walSeq:        pm.WALSeq       # child's own writes start here
        indexedUpTo:   pm.IndexedUpTo  # parent's index already covers this
        ...config copied from parent (dimension, metric, textField)
    }
    write child manifest write-once (must not already exist)
    # zero data objects copied
```

Reads on the child then **union the inherited (parent) objects with the child's own**, and writes on
the child only ever create *new* objects under the child's prefix — never mutating anything the parent
shares. That is the copy-on-write boundary.

## What our clone does today, and the gap

tpuf already has every primitive a branch needs, but no notion of one namespace referencing another:

- A namespace **is** a prefix. `manifestKey` is `ns + "/manifest.json"`, `walKey` is
  `{ns}/wal/{seq:020d}.json` (`internal/engine/wal.go`), and an index epoch lives under
  `{ns}/index/v{epoch}/` (`indexPrefix` in `internal/engine/indexer.go`).
- The `Manifest` struct (`internal/engine/types.go`) is **self-contained**: `WALSeq`, `IndexedUpTo`,
  `IndexEpoch`, `DocCount`, plus the immutable config (`Dimension`, `Metric`, `TextField`). There is no
  `Parent`/`ForkWALSeq` field, so a manifest cannot reference another namespace's objects.
- WAL segments and index objects are **write-once / immutable**: `AppendWAL` uses `PutIfAbsent`
  (`internal/engine/wal.go`); index objects use unconditional `Put` under a fresh epoch prefix
  (`putJSON` / `BuildIndex` in `internal/engine/indexer.go`). Immutability is exactly what makes sharing
  by reference safe.
- Reads are **prefix-local**. `MaterializeLive`/`MaterializeLiveAndDeleted` (`internal/engine/wal.go`)
  walk `wal/{seq}.json` for *one* `ns`; `RunQuery` (`internal/engine/query.go`) reads that one
  namespace's live epoch and WAL tail. None of them can fold in a parent's objects.
- There is **no delete-namespace path and no garbage collector** at all today — `Index` writes a new
  epoch and CAS-swaps the manifest pointer (`BuildIndex`), but old epochs and superseded WAL segments
  are simply left in the bucket. tpuf never reclaims storage.

So the gap is: (1) the manifest can't express "inherit from parent up to seq N"; (2) the read paths are
single-prefix; (3) there is no lifecycle (delete) and therefore no GC that would have to reason about
shared objects. Of these, (3) is the one branches make genuinely hard.

## How it would hook into tpuf

The design keeps tpuf's invariant intact: **the manifest is the CAS-coordinated source of truth, and
the heavy objects are immutable.** A branch adds a parent reference and a fork point; nothing about the
CAS rules in `docs/06` changes for the child's *own* writes.

**1. Extend the manifest (`internal/engine/types.go`).** Add optional fields, zero-valued for ordinary
namespaces so existing manifests stay valid:

```text
Parent         string  // "" for a root namespace; else the parent namespace name
ForkWALSeq     int64   // child inherits parent WAL segments [0, ForkWALSeq)
ForkIndexEpoch int64   // parent index epoch frozen at fork (0 = none)
```

`Dimension`, `Metric`, `TextField` are copied from the parent at fork time (a branch must keep the
parent's vector shape, since it shares the parent's index). These already exist and are immutable.

**2. A `branch_from` entry point (`internal/engine/namespace.go`).** Mirror `Create`: read the parent
manifest with `LoadManifest`, then write the child manifest **write-once** via the same mechanism
`CreateManifest` uses (`store.PutIfAbsent` — `internal/engine/manifest.go`), so two concurrent
`branch_from`s onto the same child name can't both win (the loser gets the existing-namespace error).
The fork point is the parent's `WALSeq`/`IndexEpoch` at read time. **Zero data objects are copied** —
this is the constant-time property. There is a benign race: if the parent commits between our
`LoadManifest` and our child `PutIfAbsent`, the child simply forks from a slightly earlier head; those
later parent writes land in the parent's tail and are not inherited, which is correct (a branch is a
point-in-time fork).

**3. Make reads parent-aware.** This is the real work, and it should live behind the existing helpers so
`RunQuery` doesn't have to learn about branches:

- *WAL materialize.* When a manifest has a `Parent`, the child's logical WAL is
  `parent_wal[0, ForkWALSeq) ++ child_wal[0, child.WALSeq)`. `MaterializeLiveAndDeleted`
  (`internal/engine/wal.go`) would fold the parent's `[0, ForkWALSeq)` first, then the child's own
  segments on top, preserving last-writer-wins and tombstone semantics across the boundary (a child
  tombstone shadows an inherited parent document — the `deleted` set already handles exactly this for
  the index/tail overlap, so the same logic extends to the parent/child overlap). Multi-level branches
  recurse up the parent chain to the root.
- *Index epoch.* The child has no epoch of its own until it runs `Index`. Until then, `RunQuery`'s
  indexed read should target the **parent's** `{parent}/index/v{ForkIndexEpoch}/` objects (immutable, so
  safe to read and cache), with the child's inherited+own WAL tail overlaid exactly as today. The
  moment the child runs its own `Index` (`BuildIndex`), it materializes its *full* logical WAL
  (parent-inherited + own) into a fresh `{child}/index/v1/` and from then on reads its own epochs — the
  parent index is no longer consulted. This is the copy-on-write boundary for the index.

**4. CAS / epoch implications.** The child's manifest is its *own* CAS object — `SaveManifestCAS`
(`internal/engine/manifest.go`) on the child never touches the parent's manifest, so concurrent writes
to parent and child are independent races on two different keys, matching turbopuffer's "fully
independent" guarantee. The five CAS correctness rules in `docs/06` apply unchanged to the child: WAL
write-once, manifest never cached, snapshot-WAL-at-index-start, atomic epoch swap, query-scans-tail. The
only subtlety is that the child's index-start snapshot (rule 3) covers the child's **logical** WAL
length, and its first `IndexedUpTo` must be expressed against the child's own seq space (its inherited
parent segments are folded in by the materializer, not counted in the child's `WALSeq`).

The cache (`internal/cache`, the DRAM tier from `docs/01`) needs care: it is epoch-keyed over immutable
objects, and a child reading `{parent}/index/v{ForkIndexEpoch}/...` uses the **parent's** keys, so the
cache stays correct as long as keys are namespaced by their full object path (they are). No object is
ever mutated in place, so there is no stale-cache hazard from sharing.

> **Implementation note (confirmed building `internal/engine/branch.go`):** the child's `IndexedUpTo` at
> fork must be the **parent's `IndexedUpTo`**, *not* the fork seq `ForkWALSeq`. The inherited epoch covers
> only `[0, parent.IndexedUpTo)`; the parent's *unindexed* tail `[parent.IndexedUpTo, ForkWALSeq)` is
> inherited as plain WAL the child must still scan. Setting the child's `IndexedUpTo := parent.IndexedUpTo`
> makes the child's query tail `[IndexedUpTo, WALSeq)` cover exactly that inherited tail plus the child's
> own future writes, so a branch forked off an *unindexed* (or only partially indexed) parent still sees
> all of the parent's data. The clone resolves a branch's reads through a `readView` (the ordered list of
> per-prefix WAL source runs spanning the parent chain, plus the resolved index `{ns}`/epoch); for a root
> namespace the view is a single source over its own prefix, so the non-branch fast path is unchanged. The
> first `tpuf index` on a branch materializes the *full* logical WAL (parent chain + own) into the branch's
> own `index/v1/` and flips the branch to read its own epoch — the CoW "flatten on write" boundary, after
> which steady-state branch reads no longer walk the chain.

## What's genuinely hard / what to get right

- **Garbage collection of shared objects is the whole difficulty.** tpuf has *no* GC today, so adding
  branches forces the question that single-namespace tpuf dodged: when is it safe to delete a parent
  object? An object under `{parent}/wal/` or `{parent}/index/v{E}/` may now be referenced by N branches.
  Deleting the parent (turbopuffer: "deleting the source does not affect any branches") cannot blindly
  delete its objects. The faithful approaches:
  - **Reference-by-reachability (mark-and-sweep):** an object is garbage only if **no live manifest** in
    the bucket references it. A GC pass lists all `*/manifest.json`, computes each manifest's reachable
    set (its own epochs + WAL, plus inherited parent ranges via `Parent`/`ForkWALSeq`/`ForkIndexEpoch`),
    and deletes only unreferenced objects. Simple and correct; cost is a full bucket scan.
  - **Tombstoned parent:** "deleting" a parent that still has branches just marks its manifest deleted
    (hidden from `info`/listing) but keeps its objects until the last branch referencing them is gone.
  Either way, **CAS races during GC must not delete a still-reachable object** — the safe order is *read
  all manifests → compute the reachable set → only then delete*, and a concurrent `branch_from` that
  forks just before deletion is fine because the new child manifest now references the objects and the
  next GC pass will see it. Deleting an object while a query is mid-read is the real hazard; immutability
  helps (the bytes don't change) but liveness still requires the reachability check to be conservative.
- **Tombstones across the fork boundary.** A delete written into the child must shadow an inherited
  parent document *and* survive the child's own `Index` (which materializes the union). The existing
  `deleted`-set logic in `MaterializeLiveAndDeleted` already models "a tombstone hides a document that
  exists elsewhere", so the boundary case is covered if the parent range is folded *before* the child's
  ops — but it must be tested explicitly (delete-in-child of a parent-only id; re-upsert-in-child of a
  parent-deleted id).
- **Schema must match the parent.** A branch shares the parent's vector index, so `Dimension`/`Metric`/
  `TextField` are inherited, not chosen. `branch_from` must copy them and reject any attempt to override.
- **Multi-level chains amplify read cost.** A branch chain A→B→C means a cold C query may materialize
  three WAL ranges and read A's index epoch. turbopuffer allows unbounded chains; tpuf should at least
  *compact a branch on its first `Index`* (materialize the full logical WAL into the child's own epoch)
  so steady-state reads don't walk the whole chain. This is the natural CoW "flatten on write" moment.
- **Billing vs. physical storage** is a turbopuffer product decision (logical-byte billing today, with
  stated plans to reduce it), not an engine correctness issue; the clone is educational and doesn't
  bill, so this is out of scope beyond noting *why* the physical sharing and the logical accounting
  differ.

## Sources

- turbopuffer — **Namespace Branching**: `branch_from`, "instant, copy-on-write clone … constant-time
  regardless of namespace size", full independence after creation, branch-from-branches with no
  child/chain limit, deletion independence, flat creation fee + logical-byte billing, and the
  source/branch shared-object diagram. <https://turbopuffer.com/docs/branching> (fetched 2026-06-17).
- turbopuffer — **Architecture**: "each namespace has its own prefix on object storage"; WAL as
  append-only files in the namespace prefix; asynchronous indexing of committed data; strong consistency
  by default. Grounds the prefix/WAL/immutable-object model that makes copy-on-write forks cheap.
  <https://turbopuffer.com/docs/architecture> (fetched 2026-06-17).
- This repo, `docs/01-architecture.md` — namespace = prefix with `wal/` + `index/v{epoch}/`, manifest as
  CAS head pointer, the CAS-on-JSON coordination model.
- This repo, `docs/05-clone-mapping.md` — lists branches under deliberate non-goals ("branches/CMEK …
  documented, not built").
- This repo, engine code referenced above: `internal/engine/manifest.go` (`LoadManifest`,
  `CreateManifest`, `SaveManifestCAS`), `internal/engine/types.go` (`Manifest`, `NamespaceConfig`),
  `internal/engine/wal.go` (`walKey`, `AppendWAL`, `MaterializeLive`, `MaterializeLiveAndDeleted`),
  `internal/engine/indexer.go` (`indexPrefix`, `BuildIndex`, `putJSON`),
  `internal/engine/namespace.go` (`Create`, `Upsert`, `Index`, `Query`),
  `internal/storage/storage.go` (`ObjectStore`: `PutIfAbsent`/`PutCAS`/`Put`/`List`).

> **Flagged as inferred, not turbopuffer-confirmed:** the manifest fields (`Parent`, `ForkWALSeq`,
> `ForkIndexEpoch`), the exact branch key layout, the read-union-with-parent mechanics, and the
> mark-and-sweep / tombstoned-parent GC strategies are this clone's *design*, derived from turbopuffer's
> published architecture and the standard copy-on-write pattern. turbopuffer does not publicly document
> its internal branch manifest format or garbage-collection algorithm.
