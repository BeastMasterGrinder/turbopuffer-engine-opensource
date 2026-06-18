# SPFresh / LIRE — the hard non-goal

Of all the deliberate non-goals in [`../05-clone-mapping.md`](../05-clone-mapping.md), this is the one
that actually matters and the one that is genuinely hard. turbopuffer's vector index is publicly stated to
be **"based on SPFresh"**, and SPFresh's (SOSP '23) core contribution is **LIRE** — Lightweight
Incremental REbalancing — the protocol that keeps a centroid/IVF posting-list index correct and balanced
under a continuous stream of inserts and deletes *without ever rebuilding it globally*. Our clone instead
rebuilds a fresh index epoch on every `tpuf index` run: correct, but the easy path. These three documents
explain what LIRE is, exactly how it works, and what it would take to implement it on top of tpuf's
CAS-coordinated manifest — including an honest account of why it is the hardest item in the ledger. The
summary view lives in [`../02-spfresh-spann-index.md`](../02-spfresh-spann-index.md); this series is the
deep dive. Everything is sourced from the two local papers in [`../papers/`](../papers/); turbopuffer's
own claims are flagged as theirs, not independently verified internals.

Read in order: **00 → 01 → 02.**

| # | Doc | What it covers |
|---|-----|----------------|
| 00 | [`00-background.md`](./00-background.md) | SPFresh & SPANN background: SPANN's memory/disk posting-list (IVF) split and why it's the right shape for object storage, and precisely why naive in-place insertion degrades a balanced IVF index over time — the gap LIRE exists to close. |
| 01 | [`01-lire-protocol.md`](./01-lire-protocol.md) | The LIRE protocol itself: the split / merge / reassign operations and the NPA (Nearest Partition Assignment) rule, sourced section-by-section from the SPFresh paper, with the local rewrites that keep a well-partitioned index well-partitioned. |
| 02 | [`02-implementation-in-tpuf.md`](./02-implementation-in-tpuf.md) | The implementation design: replacing tpuf's full per-epoch rebuild in `internal/engine/indexer.go` with incremental split/merge/reassign, and the genuinely hard part — what that does to the CAS-coordinated manifest. |
