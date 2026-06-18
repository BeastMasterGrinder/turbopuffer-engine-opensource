package engine

// LIRE — Lightweight Incremental REbalancing (SPFresh §3.2–§3.4) — Phase 1.
//
// This file holds the incremental-rebalancing MATH: the in-memory cluster model
// the indexer copies forward from the previous epoch, plus the three internal
// LIRE operations that keep it balanced and NPA-correct without re-clustering
// from scratch — Split (an oversized posting), Merge (an under-full posting), and
// Reassign (the small boundary set a split/merge may have knocked out of Nearest
// Partition Assignment). The publish path that turns this model into a fresh,
// write-once epoch under one manifest CAS lives in indexer.go; everything here is
// pure, deterministic, and unit-tested in isolation.
//
// PHASE 1 / OPTION A (docs/spfresh-lire/02-implementation-in-tpuf.md). We keep the
// clone's exact publish model — immutable objects under index/v{epoch}/, one final
// SaveManifestCAS — and only change HOW the new epoch is computed: copy the prior
// epoch's centroids/clusters forward and apply the LIRE deltas implied by the WAL
// tail, recomputing solely the TOUCHED clusters, instead of running KMeans over
// the whole live set. Coordination is unchanged (see indexer.go for the rule-by-
// rule statement). Full Option B (per-cluster versioned objects + manifest-as-
// catalog) and the background Local Rebuilder are the DEFERRED next phases.
//
// SPFresh runs split/merge/reassign as background, multi-threaded jobs guarded by
// per-posting locks and a per-vector version-map CAS (§4.1, §4.2.2). We have no
// daemon and no shared mutable state: the whole pass runs single-threaded inside
// one index build, against an in-memory snapshot, and is published atomically. So
// the only piece of SPFresh's concurrency machinery we need is the version NUMBER
// itself — Reassign appends a higher-version copy before dropping the old one, and
// the version is what shadows the stale replica (ClusterEntry.Version, types.go).

import "sort"

// lireCluster is one posting list in the in-memory working index the incremental
// indexer mutates. It mirrors the on-disk ClusterFile (a centroid plus members)
// but is the live, mutable form split/merge/reassign operate on before any of it
// is serialized. ID is the cluster's stable identifier across the rebalance: a
// copied-forward cluster keeps the previous epoch's id, and a split mints fresh
// ids from a monotonic counter so two clusters never collide. The on-disk
// cluster-{i}.json files are renumbered densely [0, K) only at write time, so this
// ID is internal bookkeeping, not a storage key.
type lireCluster struct {
	id       int
	centroid []float32
	members  []ClusterEntry
}

// lireIndex is the mutable working set of posting lists plus the parameters that
// govern rebalancing. nextID hands out fresh cluster ids for splits; metric and
// dim are fixed for the namespace. splitCap / mergeMin are the SPANN posting-
// length-limit analogues (§3.2): a posting strictly larger than splitCap is split,
// a posting strictly smaller than mergeMin is merged into its nearest neighbor.
//
// reassignTopN bounds Reassign's neighbor scan to the N nearest postings to the
// changed centroid (SPFresh §3.3 — "only examines nearby postings"; §5.5 finds
// top-64 enough on billion-scale data). It is a recall/cost knob, NOT a
// correctness bound: a larger N checks more candidates but the NPA re-check still
// only MOVES a vector whose true-nearest centroid actually changed, so widening it
// can only find more legitimate moves, never make a wrong one. We default it
// generously relative to this clone's tiny K (see indexer.go) so at demo scale the
// scan is effectively exhaustive and the produced epoch is fully NPA-correct.
type lireIndex struct {
	clusters     []lireCluster
	nextID       int
	metric       string
	dim          int
	splitCap     int
	mergeMin     int
	reassignTopN int
}

// clusterByID returns a pointer to the cluster with the given id, or nil. Linear
// scan is fine: K is small (≈√N) and a rebalance touches only a handful of
// clusters, so this is never the hot path.
func (li *lireIndex) clusterByID(id int) *lireCluster {
	for i := range li.clusters {
		if li.clusters[i].id == id {
			return &li.clusters[i]
		}
	}
	return nil
}

// dropCluster removes the cluster with the given id from the working set. Used by
// Merge, which empties a posting into its neighbor and then deletes it.
func (li *lireIndex) dropCluster(id int) {
	out := li.clusters[:0]
	for _, c := range li.clusters {
		if c.id != id {
			out = append(out, c)
		}
	}
	li.clusters = out
}

// nearestClusterID returns the id of the cluster whose centroid is nearest to v,
// excluding any id in skip, and whether one was found. It is the NPA primitive:
// "the posting whose centroid is genuinely nearest" (SPFresh §3.2). Ties break on
// the lower cluster id so the assignment is deterministic and reproducible,
// matching nearestClusters in query.go.
func (li *lireIndex) nearestClusterID(v []float32, skip map[int]bool) (int, bool) {
	best, found := -1, false
	bestDist := 0.0
	for i := range li.clusters {
		c := &li.clusters[i]
		if skip[c.id] {
			continue
		}
		d := Distance(li.metric, v, c.centroid)
		if !found || d < bestDist || (d == bestDist && c.id < best) {
			best, bestDist, found = c.id, d, true
		}
	}
	return best, found
}

// insertNearest appends entry to whichever current cluster centroid is nearest —
// LIRE's Insert (SPFresh §3.2): "append the vector to its nearest posting." It
// returns the id of the cluster it landed in so the caller can mark that posting
// touched (it may now exceed splitCap). The entry's own Version is preserved; an
// inserted-fresh vector carries version 0, the original-assignment version.
func (li *lireIndex) insertNearest(entry ClusterEntry) int {
	id, ok := li.nearestClusterID(entry.Vector, nil)
	if !ok {
		// No clusters exist yet (every prior cluster was merged away, or the index
		// started empty): seed a brand-new posting around this vector.
		id = li.nextID
		li.nextID++
		li.clusters = append(li.clusters, lireCluster{
			id:       id,
			centroid: cloneVec(entry.Vector),
			members:  []ClusterEntry{entry},
		})
		return id
	}
	c := li.clusterByID(id)
	c.members = append(c.members, entry)
	return id
}

// recenter recomputes a cluster's centroid as the mean of its current members.
// LIRE moves centroids to track the live distribution (the whole point versus
// frozen-centroid naive insert, docs/spfresh-lire/00-background.md); we recenter a
// posting whenever its membership changes (insert, split, merge, reassign) so its
// centroid keeps faithfully representing it. An empty cluster keeps its old
// centroid (it is about to be dropped by Merge anyway).
func (c *lireCluster) recenter() {
	if len(c.members) == 0 {
		return
	}
	pts := make([][]float32, len(c.members))
	for i, m := range c.members {
		pts[i] = m.Vector
	}
	c.centroid = meanVec(pts)
}

// splitOversized splits every posting strictly larger than splitCap into two with
// a local 2-means pass, repeating until no posting exceeds the cap (a split child
// can itself still be oversized, so we loop — and a split can cascade into more
// splits, which SPFresh proves terminates, §3.4: each split adds exactly one
// centroid and |C| ≤ |V|). It returns the set of cluster ids that were created or
// disturbed, so the caller knows which postings to feed Reassign and which to
// rewrite. The old (split) posting's id is RETIRED — split deletes the old
// centroid and adds two new ones (§3.2) — so the returned set names the two
// children, never the consumed parent.
func (li *lireIndex) splitOversized() map[int]bool {
	touched := map[int]bool{}
	// Guard against pathological non-termination: at most one split per vector can
	// ever happen (|C| ≤ |V|, §3.4), so bounding by the total member count is a safe
	// belt-and-braces ceiling on top of the cap-driven loop condition.
	maxSplits := li.totalMembers() + 1
	for splits := 0; splits < maxSplits; splits++ {
		// Find the next posting over the cap. Scanning lowest-id-first keeps the
		// sequence of splits deterministic.
		victim := -1
		for i := range li.clusters {
			if len(li.clusters[i].members) > li.splitCap {
				victim = li.clusters[i].id
				break
			}
		}
		if victim < 0 {
			return touched // no posting exceeds the cap: balanced.
		}
		a, b, ok := li.split2(victim)
		if !ok {
			// 2-means could not actually divide the posting (all members identical, or
			// degenerate): leave it as-is rather than loop forever. An oversized but
			// indivisible posting is correct, merely unbalanced — acceptable, and the
			// honest small-N edge the docs flag.
			return touched
		}
		delete(touched, victim)
		touched[a] = true
		touched[b] = true
	}
	return touched
}

// split2 runs one balanced 2-means split of the posting with id victim into two
// fresh postings, returning their new ids. It reuses the deterministic kMeans the
// flat build uses (k=2, fixed seed) so the split is reproducible. The two children
// get fresh ids from nextID and the old posting is dropped — exactly LIRE's "run
// balanced clustering to split it into two postings with two new centroids; delete
// the old centroid, add the two new ones" (SPFresh §3.2). ok is false when the
// split fails to produce two non-empty children (e.g. all residuals identical), so
// the caller can stop rather than spin.
//
// NOTE: this only re-partitions the members already IN the posting. Restoring NPA
// for neighbors a split may have disturbed is Reassign's job (§3.2 — every split is
// followed by a reassign), driven from the touched set this returns.
func (li *lireIndex) split2(victim int) (int, int, bool) {
	c := li.clusterByID(victim)
	if c == nil || len(c.members) < 2 {
		return 0, 0, false
	}
	pts := make([][]float32, len(c.members))
	for i, m := range c.members {
		pts[i] = m.Vector
	}
	centroids, assign := KMeans(pts, 2, li.metric, kmeansIters)
	if len(centroids) < 2 {
		return 0, 0, false // KMeans collapsed to one cluster: indivisible.
	}

	var left, right []ClusterEntry
	for i, m := range c.members {
		if assign[i] == 0 {
			left = append(left, m)
		} else {
			right = append(right, m)
		}
	}
	if len(left) == 0 || len(right) == 0 {
		return 0, 0, false // every member landed on one side: not a real split.
	}

	idA, idB := li.nextID, li.nextID+1
	li.nextID += 2
	li.dropCluster(victim)
	ca := lireCluster{id: idA, centroid: centroids[0], members: left}
	cb := lireCluster{id: idB, centroid: centroids[1], members: right}
	ca.recenter()
	cb.recenter()
	li.clusters = append(li.clusters, ca, cb)
	return idA, idB, true
}

// mergeUnderfull merges every posting strictly smaller than mergeMin into its
// nearest OTHER posting (SPFresh §3.2 — "delete the small posting, append its
// vectors directly into its nearest posting; then reassign only the moved
// vectors"). Merges never cause splits because vectors only move OUT of the
// dissolved posting, but a merge target can itself become oversized — that is
// fine, the subsequent split pass catches it. It returns the set of surviving
// target ids that received members, so the caller reassigns the moved vectors and
// rewrites those targets. A merge into a posting that itself gets merged later is
// handled naturally by re-scanning each iteration.
//
// The last surviving posting is never merged away: if only one posting remains it
// stays, however small, because there is nowhere to merge it to (and an index must
// have at least one posting to answer a query).
func (li *lireIndex) mergeUnderfull() map[int]bool {
	touched := map[int]bool{}
	maxMerges := len(li.clusters) + 1
	for merges := 0; merges < maxMerges; merges++ {
		if len(li.clusters) <= 1 {
			return touched // nothing to merge into.
		}
		victim := -1
		for i := range li.clusters {
			if len(li.clusters[i].members) < li.mergeMin {
				victim = li.clusters[i].id
				break
			}
		}
		if victim < 0 {
			return touched // every posting is at or above the floor.
		}
		target, ok := li.nearestClusterID(li.clusterByID(victim).centroid, map[int]bool{victim: true})
		if !ok {
			return touched // no other posting to merge into (shouldn't happen with >1).
		}
		src := li.clusterByID(victim)
		dst := li.clusterByID(target)
		dst.members = append(dst.members, src.members...)
		dst.recenter()
		li.dropCluster(victim)
		// A previously-touched id that was the victim is now gone; the target is what
		// survives and may need reassignment of its newly-absorbed members.
		delete(touched, victim)
		touched[target] = true
	}
	return touched
}

// reassign restores Nearest Partition Assignment for the boundary vectors that the
// postings in `changed` (split children, merge targets) may have knocked out of
// place, using SPFresh's two necessary conditions (§3.3, Eq. 1–2) to check as few
// vectors as possible. It MOVES a vector by appending a higher-version copy to its
// true-nearest posting and dropping the old copy (the version-map mechanism,
// §3.3 / §4.2.1). It returns the set of cluster ids whose membership changed
// (the changed postings plus any posting that gained or lost a reassigned vector)
// so the caller rewrites exactly those and no more.
//
// The two necessary conditions prune the vast majority of vectors before the exact
// NPA re-check. For a posting P that changed (centroid A_new) relative to the prior
// centroids:
//
//   - Eq. 1 (members of a SPLIT posting): a member v stays a candidate only if the
//     OLD centroid A_o was at least as close as the new one — D(v,A_o) ≤ D(v,A_new)
//     — because if a new centroid is already closer, no neighboring posting can beat
//     it and v is fine where it is.
//   - Eq. 2 (vectors in a NEARBY posting B): v is a candidate only if some changed
//     centroid A_new got at least as close as v's current centroid — D(v,A_new) ≤
//     D(v,A_cur) — because otherwise the change cannot have pulled v out of B.
//
// We have the OLD centroids available (passed in oldCentroids, keyed by the cluster
// id the vector currently sits in / the retired parent id) to evaluate Eq. 1/Eq. 2
// exactly. After pruning, every surviving candidate gets the exact NPA re-check
// (nearestClusterID over the whole working set); only those whose true-nearest
// posting actually differs from their current one are moved. False positives are
// dropped silently, exactly as §3.3 prescribes ("if a vector actually does not need
// reassignment, the reassign operation is aborted").
//
// forceIDs is the set of vectors that must be NPA-rechecked UNCONDITIONALLY,
// bypassing the Eq. 1/Eq. 2 prune. The two necessary conditions are derived for
// vectors whose *centroids* changed under a split/merge; they do NOT cover a vector
// whose OWN position changed — a fresh insert or a re-upsert that moved the
// vector — because such a vector is effectively a new Insert and may now belong to
// any posting, not just a neighbor of the changed centroid. The incremental driver
// passes every inserted/moved id here so it always gets the exact re-check, which
// is what keeps the produced epoch fully NPA-correct (and therefore recall-correct)
// rather than only locally-repaired.
//
// CORRECTNESS NOTE on the version bump: within ONE Option-A epoch we drop the stale
// copy in the same pass, so the published epoch holds at most one copy of each id —
// the bump's job here is to make the version-map mechanism real groundwork and to
// keep the moved copy distinguishable from its origin during the move. The deferred
// Option-B phase (per-cluster versioned objects) is where two copies can persist
// ACROSS objects until GC, and the same Version field shadows the stale one there.
func (li *lireIndex) reassign(changed map[int]bool, oldCentroids map[int][]float32, forceIDs map[string]bool) map[int]bool {
	dirty := map[int]bool{}
	for id := range changed {
		dirty[id] = true
	}
	if len(changed) == 0 && len(forceIDs) == 0 {
		return dirty
	}

	// The new centroids of the changed postings: the A_new set the conditions test
	// against. nearbyPostings limits Eq. 2's scan to the reassignTopN postings
	// nearest a changed centroid (§3.3), the only ones a local change can disturb.
	changedCentroids := map[int][]float32{}
	for id := range changed {
		if c := li.clusterByID(id); c != nil {
			changedCentroids[id] = c.centroid
		}
	}

	// Collect reassignment candidates as (clusterID, memberIndex) so we can prune
	// before doing any expensive moves. We gather first, then move, so moving never
	// invalidates indices mid-scan.
	type cand struct {
		fromCluster int
		id          string
	}
	var candidates []cand
	seen := map[string]bool{}

	// Forced candidates: every inserted/moved vector, gathered first so it always
	// reaches the exact NPA re-check regardless of the Eq. 1/Eq. 2 prune below.
	if len(forceIDs) > 0 {
		for ci := range li.clusters {
			c := &li.clusters[ci]
			for _, m := range c.members {
				if forceIDs[m.ID] && !seen[m.ID] {
					candidates = append(candidates, cand{fromCluster: c.id, id: m.ID})
					seen[m.ID] = true
				}
			}
		}
	}

	// Eq. 1: members of the changed (split) postings themselves.
	for id := range changed {
		c := li.clusterByID(id)
		if c == nil {
			continue
		}
		for _, m := range c.members {
			if seen[m.ID] {
				continue
			}
			// The old centroid of the posting this member came from. For a split child
			// the relevant A_o is the retired parent's centroid, recorded in
			// oldCentroids under the parent id; we look it up via the member's prior
			// home, which the caller threads through oldCentroids keyed by current id
			// too. If we have no old centroid (a freshly created posting with no
			// predecessor), treat the member as a candidate — being conservative here
			// only costs an exact NPA re-check, never correctness.
			ao, ok := oldCentroids[id]
			if ok && Distance(li.metric, m.Vector, c.centroid) < Distance(li.metric, m.Vector, ao) {
				// A new centroid is already strictly closer than the old one: Eq. 1
				// fails, v cannot belong to a neighbor, no check needed.
				continue
			}
			candidates = append(candidates, cand{fromCluster: id, id: m.ID})
			seen[m.ID] = true
		}
	}

	// Eq. 2: members of the postings NEAR a changed centroid. For each changed
	// centroid, find its reassignTopN nearest OTHER postings and test their members.
	for cid, anew := range changedCentroids {
		for _, nb := range li.nearbyPostings(anew, cid) {
			c := li.clusterByID(nb)
			if c == nil {
				continue
			}
			acur := c.centroid // v's current centroid is its posting's centroid.
			for _, m := range c.members {
				if seen[m.ID] {
					continue
				}
				// Eq. 2: some changed centroid is now at least as close as v's current
				// centroid. We already hold one changed centroid (anew); test it.
				if Distance(li.metric, m.Vector, anew) <= Distance(li.metric, m.Vector, acur) {
					candidates = append(candidates, cand{fromCluster: nb, id: m.ID})
					seen[m.ID] = true
				}
			}
		}
	}

	// Exact NPA re-check + move. For each candidate, find its true-nearest posting;
	// if it differs from where it sits, append a higher-version copy there and drop
	// the old copy. We resolve members by id at move time (membership may have
	// shifted) and skip any candidate already moved.
	moved := map[string]bool{}
	for _, cd := range candidates {
		if moved[cd.id] {
			continue
		}
		from := li.clusterByID(cd.fromCluster)
		if from == nil {
			continue
		}
		idx := memberIndex(from.members, cd.id)
		if idx < 0 {
			continue // already moved out by an earlier candidate.
		}
		entry := from.members[idx]
		target, ok := li.nearestClusterID(entry.Vector, nil)
		if !ok || target == cd.fromCluster {
			continue // NPA already satisfied: a false positive, abort the reassign.
		}
		// Append the higher-version copy to the NPA-correct posting BEFORE dropping the
		// old copy (SPFresh §3.3 order: append-then-delete). The version bump is what
		// shadows the stale copy in a world where both can coexist.
		moveEntry := entry
		moveEntry.Version = entry.Version + 1
		dst := li.clusterByID(target)
		dst.members = append(dst.members, moveEntry)
		from.members = removeMemberAt(from.members, idx)
		moved[cd.id] = true
		dirty[cd.fromCluster] = true
		dirty[target] = true
	}

	// Recenter every posting whose membership changed so its centroid tracks its new
	// members (the reassigned-into and reassigned-out-of postings both shifted).
	for id := range dirty {
		if c := li.clusterByID(id); c != nil {
			c.recenter()
		}
	}
	return dirty
}

// nearbyPostings returns the ids of the reassignTopN postings whose centroids are
// nearest to center, excluding excludeID. This is SPFresh's "select several A_o's
// nearest postings" (§3.3): Reassign only ever examines vectors in this local
// neighborhood, which is why it is cheap. Returned nearest-first, ties on lower id.
func (li *lireIndex) nearbyPostings(center []float32, excludeID int) []int {
	type cd struct {
		id   int
		dist float64
	}
	cds := make([]cd, 0, len(li.clusters))
	for i := range li.clusters {
		c := &li.clusters[i]
		if c.id == excludeID {
			continue
		}
		cds = append(cds, cd{id: c.id, dist: Distance(li.metric, center, c.centroid)})
	}
	sort.Slice(cds, func(i, j int) bool {
		if cds[i].dist != cds[j].dist {
			return cds[i].dist < cds[j].dist
		}
		return cds[i].id < cds[j].id
	})
	n := li.reassignTopN
	if n > len(cds) {
		n = len(cds)
	}
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = cds[i].id
	}
	return out
}

// totalMembers is the count of all members across all postings — the |V| ceiling
// the split loop uses as a non-termination guard.
func (li *lireIndex) totalMembers() int {
	n := 0
	for i := range li.clusters {
		n += len(li.clusters[i].members)
	}
	return n
}

// memberIndex returns the index of the member with the given id, or -1.
func memberIndex(members []ClusterEntry, id string) int {
	for i := range members {
		if members[i].ID == id {
			return i
		}
	}
	return -1
}

// removeMemberAt deletes the member at index i, preserving order. Order is kept so
// the produced epoch is byte-for-byte reproducible (the indexer also sorts members
// by id before writing, but stable removal keeps intermediate state predictable).
func removeMemberAt(members []ClusterEntry, i int) []ClusterEntry {
	out := make([]ClusterEntry, 0, len(members)-1)
	out = append(out, members[:i]...)
	out = append(out, members[i+1:]...)
	return out
}
