package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// Branches are copy-on-write forks of a namespace
// (docs/extensions/branches-copy-on-write.md). A branch is just a NEW manifest
// under its own prefix that points at the parent's existing — immutable — WAL
// segments and index epoch. Forking copies ZERO data objects: the constant-time
// property turbopuffer advertises. New writes on the branch go to the branch's
// own WAL starting at the fork point; the parent stays untouched, so the two
// namespaces are fully independent after the fork (a write on either side is a
// CAS on a different manifest key and cannot affect the other).
//
// This file holds the fork entry point (BranchFrom) and the read-resolution that
// makes a branch's logical WAL and indexed reads span the parent chain. The read
// path elsewhere (wal.go, query.go, indexer.go) stays prefix-simple for root
// namespaces and only consults the resolver below when a manifest IsBranch.
//
// GC HAZARD (stated explicitly, per the KB note): tpuf has NO garbage collector
// today, and branches make that absence load-bearing. A branch PINS its parent's
// objects: every WAL segment in [0, ForkWALSeq) and every object under the
// parent's index/v{ForkIndexEpoch}/ prefix is referenced by reference, not
// copied. If a GC were ever added it MUST treat an object as live while ANY
// manifest in the bucket reaches it (its own + every branch's inherited range);
// deleting a parent object a branch still points at would corrupt the branch.
// Because we never delete and the parent is immutable, sharing is safe today —
// but the pin is real and must survive any future lifecycle work.

// maxBranchDepth bounds how far the resolver walks up the parent chain. The KB
// doc notes turbopuffer allows unbounded branch chains; this educational clone
// caps the walk so a manifest cycle (which can only arise from a corrupt
// Parent pointer, never from BranchFrom) surfaces as an error instead of an
// infinite loop. It is generous for any human-built chain of branches.
const maxBranchDepth = 64

// BranchFrom creates a copy-on-write branch named child that forks parent at the
// parent's current head. It mirrors CreateManifest: the child manifest is written
// write-once with PutIfAbsent, so two concurrent BranchFroms onto the same child
// name can never both win (the loser gets the same "already exists" error Create
// gives). ZERO data objects are copied — the fork is a single manifest PUT,
// hence O(1) regardless of how much data the parent holds.
//
// The fork point is the parent's WALSeq/IndexEpoch at read time. There is a
// benign race the KB doc calls out: if the parent commits between our
// LoadManifest and our PutIfAbsent, the child simply forks from a slightly
// earlier head and those later parent writes are not inherited — correct, since
// a branch is a point-in-time fork.
//
// The branch inherits the parent's immutable vector shape (Dimension, Metric,
// TextField): it shares the parent's index, so it cannot choose a different
// schema. The child's own WAL numbering continues the parent's logical seq space
// (WALSeq starts at the fork point), and IndexedUpTo starts at the fork too,
// because the parent's frozen epoch already covers [0, ForkWALSeq).
func BranchFrom(ctx context.Context, store *cache.Store, parent, child string) error {
	if parent == child {
		return fmt.Errorf("branching %q: a namespace cannot branch from itself", child)
	}

	pm, _, err := LoadManifest(ctx, store, parent)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return fmt.Errorf("branching %q: parent namespace %q does not exist", child, parent)
		}
		return fmt.Errorf("branching %q: loading parent %q: %w", child, parent, err)
	}

	cm := Manifest{
		Version:   1,
		Dimension: pm.Dimension, // schema is inherited, never chosen for a branch.
		Metric:    pm.Metric,
		TextField: pm.TextField,
		// The child's own writes begin at the fork point in the shared logical seq
		// space. IndexedUpTo is the parent's OWN IndexedUpTo, not the fork seq:
		// the inherited epoch covers only [0, pm.IndexedUpTo), and the parent's
		// unindexed tail [pm.IndexedUpTo, ForkWALSeq) is inherited as WAL the
		// branch must still scan. Inheriting pm.IndexedUpTo makes the branch's
		// query tail [IndexedUpTo, WALSeq) cover exactly that inherited tail plus
		// the branch's own future writes — so a branch forked off an UNINDEXED (or
		// partially indexed) parent still sees all the parent's data.
		WALSeq:      pm.WALSeq,
		IndexedUpTo: pm.IndexedUpTo,
		IndexEpoch:  0, // the branch has no epoch of its own until it reindexes.
		DocCount:    pm.DocCount,

		Parent:         parent,
		ForkWALSeq:     pm.WALSeq,
		ForkIndexEpoch: pm.IndexEpoch,
	}

	body, err := marshalManifest(cm)
	if err != nil {
		return fmt.Errorf("branching %q: %w", child, err)
	}
	if _, err := store.PutIfAbsent(ctx, manifestKey(child), body); err != nil {
		if errors.Is(err, storage.ErrPreconditionFailed) {
			return fmt.Errorf("namespace %q already exists", child)
		}
		return fmt.Errorf("branching %q: %w", child, err)
	}
	return nil
}

// walSource is one contiguous run of WAL segments resolved to the physical
// namespace prefix that holds them. A branch's logical WAL is a concatenation of
// such runs: the inherited parent range(s) up the chain, then the branch's own
// segments. Each run carries the [from, to) logical seq window AND the prefix
// (ns) whose wal/{seq}.json objects actually back those seqs — and because the
// branch reuses the parent's logical seq numbers verbatim, no offset arithmetic
// is needed: seq s in this run lives at {ns}/wal/{s}.json.
type walSource struct {
	ns   string // the namespace prefix whose wal/ objects back this run
	from int64  // inclusive logical seq
	to   int64  // exclusive logical seq
}

// readView is the resolved read plan for one (possibly branched) manifest: the
// ordered list of WAL sources that make up its full logical WAL, plus the
// namespace and epoch its indexed reads should target. For a root namespace this
// is trivial — a single WAL source over its own prefix and its own epoch — so the
// fast path stays a single prefix with no chain walking.
//
// A branch's indexEpoch points at the PARENT's frozen epoch until the branch
// runs its own Index, at which point BuildIndex writes the branch's own epoch and
// flips the branch manifest to read it (the CoW boundary for the index).
type readView struct {
	sources    []walSource // logical [0, WALSeq), oldest first
	indexNS    string      // namespace prefix whose index/v{epoch}/ to read
	indexEpoch int64       // 0 = no indexed data anywhere up the chain
}

// resolveReadView walks the parent chain of m and builds its read plan: the WAL
// source runs covering [0, m.WALSeq) and the indexed read target. It loads each
// ancestor manifest fresh (rule 2 — manifests are never cached) to learn that
// ancestor's own fork point. The walk is bounded by maxBranchDepth so a corrupt
// Parent cycle errors rather than spins.
//
// WAL resolution: the branch's own segments cover [ForkWALSeq, WALSeq) under its
// own prefix; everything below ForkWALSeq is inherited from the parent, which in
// turn may inherit from ITS parent. We collect the runs from the leaf down to the
// root, then reverse so the materializer folds them oldest-first (preserving
// last-writer-wins across fork boundaries: a child op shadows an inherited parent
// op for the same id, exactly as a tail op shadows an indexed one).
//
// Index resolution: if the leaf has its own epoch (IndexEpoch > 0) it reads its
// own; otherwise it reads the nearest ancestor's epoch frozen at the fork
// (ForkIndexEpoch), walking up until one is found or the root is reached.
func resolveReadView(ctx context.Context, store *cache.Store, ns string, m Manifest) (readView, error) {
	// WAL sources, leaf-first; reversed to oldest-first before returning.
	var rev []walSource
	cur, curM := ns, m
	for depth := 0; ; depth++ {
		if depth > maxBranchDepth {
			return readView{}, fmt.Errorf("resolving %q: branch chain exceeds depth %d (corrupt parent pointer?)", ns, maxBranchDepth)
		}
		// This namespace owns the logical range [fork, WALSeq): its own writes.
		// fork is 0 for a root, ForkWALSeq for a branch.
		fork := int64(0)
		if curM.IsBranch() {
			fork = curM.ForkWALSeq
		}
		if curM.WALSeq > fork {
			rev = append(rev, walSource{ns: cur, from: fork, to: curM.WALSeq})
		}
		if !curM.IsBranch() {
			break // reached a root: the inherited range below is fully covered.
		}
		// Descend into the parent for the inherited range [0, ForkWALSeq).
		parent := curM.Parent
		pm, _, err := LoadManifest(ctx, store, parent)
		if err != nil {
			return readView{}, fmt.Errorf("resolving %q: loading parent %q: %w", ns, parent, err)
		}
		// The parent contributes only up to THIS branch's fork point, regardless
		// of how far the parent has since advanced — a branch is a point-in-time
		// fork and must not see writes the parent made after the fork. Clamp the
		// parent's effective head to our fork seq before recursing.
		pm.WALSeq = minInt64(pm.WALSeq, curM.ForkWALSeq)
		cur, curM = parent, pm
	}

	// Reverse to oldest-first so MaterializeView folds root → leaf.
	sources := make([]walSource, len(rev))
	for i, s := range rev {
		sources[len(rev)-1-i] = s
	}

	indexNS, indexEpoch, err := resolveIndex(ctx, store, ns, m)
	if err != nil {
		return readView{}, err
	}
	return readView{sources: sources, indexNS: indexNS, indexEpoch: indexEpoch}, nil
}

// resolveIndex finds the index epoch a (possibly branched) manifest reads. The
// leaf's own epoch wins the moment it has one (it reindexed, materializing its
// full logical WAL into its own prefix — the CoW boundary for the index). Until
// then a branch reads the nearest ancestor's epoch frozen at its fork point,
// walking up the chain. Returns epoch 0 when no ancestor has an index.
func resolveIndex(ctx context.Context, store *cache.Store, ns string, m Manifest) (string, int64, error) {
	cur, curM := ns, m
	for depth := 0; ; depth++ {
		if depth > maxBranchDepth {
			return "", 0, fmt.Errorf("resolving index for %q: branch chain exceeds depth %d (corrupt parent pointer?)", ns, maxBranchDepth)
		}
		if curM.IndexEpoch > 0 {
			return cur, curM.IndexEpoch, nil // this namespace's own live epoch.
		}
		if !curM.IsBranch() {
			return cur, 0, nil // root with no index built yet.
		}
		// No own epoch: read the parent's epoch frozen at the fork. If the fork
		// captured no epoch (parent was unindexed at fork), keep walking up — a
		// grandparent might be indexed and still cover the shared range.
		if curM.ForkIndexEpoch > 0 {
			return curM.Parent, curM.ForkIndexEpoch, nil
		}
		pm, _, err := LoadManifest(ctx, store, curM.Parent)
		if err != nil {
			return "", 0, fmt.Errorf("resolving index for %q: loading parent %q: %w", ns, curM.Parent, err)
		}
		cur, curM = curM.Parent, pm
	}
}

// MaterializeView folds the logical WAL window [from, to) of a (possibly
// branched) manifest, spanning the parent chain, and returns the surviving
// documents (live, last-writer-wins) and the set of ids whose final op was a
// tombstone (deleted). It is the branch-aware generalization of
// MaterializeLiveAndDeleted: for a root namespace v has a single source over its
// own prefix and this reduces to the original single-prefix fold; for a branch it
// stitches the inherited parent segments and the branch's own segments into one
// continuous fold, so a child tombstone correctly shadows an inherited parent
// document.
//
// Folding is oldest-first across the whole logical range (v.sources is already
// ordered root → leaf), so last-writer-wins holds across fork boundaries exactly
// as it does within one namespace: a later (child) op for an id overwrites an
// earlier (inherited) one, and a child re-upsert of a parent-deleted id revives
// it. The [from, to) window is intersected with each source's run so a query tail
// scan that starts mid-chain only reads the segments it needs.
func MaterializeView(ctx context.Context, store *cache.Store, v readView, from, to int64) (live map[string]Document, deleted map[string]bool, err error) {
	live = make(map[string]Document)
	deleted = make(map[string]bool)

	for _, src := range v.sources {
		// Intersect the requested window with this source's logical run.
		lo := maxInt64(from, src.from)
		hi := minInt64(to, src.to)
		for seq := lo; seq < hi; seq++ {
			seg, err := ReadWAL(ctx, store, src.ns, seq)
			if err != nil {
				return nil, nil, fmt.Errorf("materializing [%d,%d): %w", from, to, err)
			}
			for _, op := range seg.Ops {
				if op.Deleted {
					delete(live, op.ID)
					deleted[op.ID] = true
					continue
				}
				live[op.ID] = op
				delete(deleted, op.ID)
			}
		}
	}
	return live, deleted, nil
}

// MaterializeLiveView is the live-only wrapper around MaterializeView, mirroring
// MaterializeLive over a single prefix. The indexer uses it to fold a branch's
// full logical WAL (parent chain + own segments) into a fresh epoch.
func MaterializeLiveView(ctx context.Context, store *cache.Store, v readView, from, to int64) (map[string]Document, error) {
	live, _, err := MaterializeView(ctx, store, v, from, to)
	if err != nil {
		return nil, err
	}
	return live, nil
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
