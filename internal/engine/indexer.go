package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// kmeansIters is the number of Lloyd's refinement passes the indexer runs when
// building the IVF clusters (docs/06). KMeans stops early once assignments
// stabilize, so this is an upper bound, generous for the small N this
// educational clone targets.
const kmeansIters = 25

// treeFanout and treeLeafCapacity parameterize the optional hierarchical centroid
// tree built over the flat IVF centroids (docs/extensions/hierarchical-centroid-tree.md).
// treeFanout is the per-level branching constant F — turbopuffer's ANN v3 uses
// ≈100 children per node; we keep a small F so even a handful of centroids forms a
// real multi-level tree to demonstrate the shape. treeLeafCapacity is the leaf cap
// (SPANN's posting-length-limit analogue, §4.2): a tree node holding at most this
// many flat centroids becomes a leaf. With a cap of 1 every leaf routes to exactly
// one flat cluster, so beam descent and the flat scan address the same posting
// lists — the cleanest setting for proving the same-top-K invariant. HONEST NOTE:
// at this clone's K ≈ √N a flat scan is already a handful of dot products, so the
// tree reduces nothing measurable here; BuildTree returns nil when no real
// hierarchy forms (K <= cap), and the query path then keeps the flat O(K) scan.
const (
	treeFanout       = 8
	treeLeafCapacity = 1
)

// rotationSeed is the fixed seed for the True RaBitQ random orthogonal rotation
// (docs/extensions/true-rabitq.md). Like KMeans's rand.NewSource(1), a fixed seed
// keeps the whole vector index deterministic: the same live set always produces
// the same rotation, the same codes, and therefore byte-for-byte reproducible
// epochs the indexer tests rely on. The rotation is stored in the epoch's
// CentroidsFile, so query-time uses exactly this matrix regardless of the seed.
const rotationSeed = 1

// indexPrefix returns the object-key prefix for a namespace's index epoch:
// {ns}/index/v{epoch}/. Every object written under it is write-once and
// immutable, so the cache may serve it via GetCached and the indexer writes it
// with an unconditional Put.
func indexPrefix(ns string, epoch int64) string {
	return fmt.Sprintf("%s/index/v%d/", ns, epoch)
}

func centroidsKey(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "centroids.json"
}

func clusterKey(ns string, epoch int64, cluster int) string {
	return fmt.Sprintf("%scluster-%d.json", indexPrefix(ns, epoch), cluster)
}

func bm25Key(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "bm25.json"
}

func docsKey(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "docs.json"
}

func attrsKey(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "bitmaps.json"
}

// BuildIndex folds the durable WAL into a fresh, immutable index epoch and
// publishes it with a single atomic manifest CAS. It is the heart of the
// "object storage is the source of truth, everything else hides its latency"
// bet: the index is just a derived snapshot of the WAL that queries can read
// from cached, write-once objects.
//
// The flow follows docs/06's indexer pseudocode and obeys the CAS correctness
// rules:
//
//   - walUpTo is snapshotted from the manifest at index START (rule 3): any
//     write that lands while we build belongs to the next epoch, and a query
//     overlays the tail [IndexedUpTo, WALSeq) so those writes stay searchable.
//   - All index/v{epoch}/* objects are written first, under the fresh epoch
//     prefix, with unconditional Put (write-once keys; no concurrent writer can
//     collide). The index goes live only at the final SaveManifestCAS that
//     flips IndexEpoch (rule 4 — atomic swap). Until then queries serve the old
//     epoch.
//
// An empty namespace (WALSeq 0) is handled gracefully: live is empty, no vector
// or BM25 work happens, docs.json is an empty map, and the manifest still
// advances to the new epoch so Info reflects an indexed-but-empty namespace.
func BuildIndex(ctx context.Context, store *cache.Store, ns string) error {
	m, _, err := LoadManifest(ctx, store, ns)
	if err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	// Rule 3: snapshot the WAL position NOW. This becomes IndexedUpTo, so the
	// query tail starts exactly where this epoch's coverage ends.
	walUpTo := m.WALSeq
	epoch := m.IndexEpoch + 1

	// The vector build may run incrementally (SPFresh LIRE Phase 1 / Option A,
	// docs/spfresh-lire/02-implementation-in-tpuf.md): instead of re-clustering the
	// whole live set, copy the PREVIOUS epoch's centroids/clusters forward and apply
	// only the LIRE deltas implied by the WAL tail [prevIndexedUpTo, walUpTo). We
	// snapshot the prior epoch number and its coverage from the SAME manifest read
	// that fixed walUpTo, so the delta window is exact. prevEpoch == 0 (or a branch,
	// or a dimension change) means there is nothing to carry forward and the vector
	// build falls back to a full rebuild — see clusterLive. Coordination is
	// unchanged either way: every index/v{epoch}/* object is still write-once under a
	// fresh prefix and the epoch still goes live at the single SaveManifestCAS below.
	prevEpoch := m.IndexEpoch
	prevIndexedUpTo := m.IndexedUpTo
	if m.IsBranch() {
		// A branch materializes its FULL logical WAL into its own first epoch
		// (copy-on-write flatten, resolveReadView below); there is no prior OWN epoch
		// to copy forward, so the incremental path does not apply to a branch's
		// initial build. Force the full-rebuild path by zeroing the prev pointer.
		prevEpoch = 0
	}

	// Resolve the read view so a BRANCH materializes its FULL logical WAL — the
	// inherited parent segments [0, ForkWALSeq) plus its own — into a fresh epoch
	// under its OWN prefix (docs/extensions/branches-copy-on-write.md). This is the
	// copy-on-write "flatten on write" moment: from here the branch reads its own
	// epoch and no longer consults the parent index, so steady-state branch reads
	// don't walk the chain. For a root namespace the view is a single source over
	// its own prefix and this reduces to the original single-prefix materialize.
	v, err := resolveReadView(ctx, store, ns, m)
	if err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}
	live, err := MaterializeLiveView(ctx, store, v, 0, walUpTo)
	if err != nil {
		return fmt.Errorf("indexing %q: materializing [0,%d): %w", ns, walUpTo, err)
	}

	// Assign every live document a stable dense [0, N) ordinal once, in sorted-id
	// order, and reuse it across the vector and attribute indexes so a bitmap bit
	// translates back to the same id everywhere. clusterOf records which IVF
	// cluster each id's vector landed in (-1 for a vector-less doc), filled by the
	// vector build and consumed by the attribute build for cluster-level pruning.
	ordinals := assignOrdinals(live)
	clusterOf := make(map[string]int, len(live))

	if err := buildVectorIndex(ctx, store, ns, epoch, prevEpoch, prevIndexedUpTo, walUpTo, m.Metric, live, clusterOf); err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	if m.TextField != "" {
		if err := buildBM25Index(ctx, store, ns, epoch, m.TextField, live); err != nil {
			return fmt.Errorf("indexing %q: %w", ns, err)
		}
	}

	if err := buildAttributeIndex(ctx, store, ns, epoch, m.TextField, live, ordinals, clusterOf); err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	if err := writeDocs(ctx, store, ns, epoch, live); err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	// Rule 4: a single CAS makes the new epoch live. Everything above is already
	// durable under the fresh prefix, so this flip is the only externally
	// visible moment of the swap.
	docCount := len(live)
	if _, err := SaveManifestCAS(ctx, store, ns, func(m *Manifest) {
		m.IndexEpoch = epoch
		m.IndexedUpTo = walUpTo
		m.DocCount = docCount
	}); err != nil {
		return fmt.Errorf("indexing %q: publishing epoch %d: %w", ns, epoch, err)
	}
	return nil
}

// splitCapMultiplier and mergeMinDivisor set the LIRE posting-length band relative
// to the ideal balanced posting size N/K (SPANN's balanced-clustering target,
// §3.2.1). A copy-forward epoch keeps postings inside [N/K / mergeMinDivisor,
// N/K * splitCapMultiplier]: a posting that grows past the upper cap is split, one
// that shrinks below the lower floor is merged. The band is deliberately wide
// (2×/2÷) so the incremental pass only rebalances genuinely skewed postings — the
// "only ~0.4% of inserts trigger rebalancing" regime LIRE is built for
// (docs/spfresh-lire/01-lire-protocol.md), not every insert.
const (
	splitCapMultiplier = 2
	mergeMinDivisor    = 2
)

// buildVectorIndex clusters the live documents that carry a vector and writes
// centroids.json plus one cluster-{i}.json per IVF cluster. Documents with no
// vector (e.g. text-only records in a namespace that also does BM25) are
// skipped — only vectors participate in clustering. When no live document has a
// vector, nothing is written and the namespace simply has no vector index for
// this epoch.
//
// Clustering runs incrementally when a prior epoch can be carried forward (SPFresh
// LIRE Phase 1 / Option A) and falls back to a full from-scratch k-means
// otherwise; clusterLive makes that choice. Either way it returns the SAME shape —
// final centroids plus per-cluster members — so the serialization below (RaBitQ
// codes, the centroid tree, the clusterOf map the bitmap index reads) is identical
// regardless of how the partition was produced.
func buildVectorIndex(ctx context.Context, store *cache.Store, ns string, epoch, prevEpoch, prevIndexedUpTo, walUpTo int64, metric string, live map[string]Document, clusterOf map[string]int) error {
	// Gather the vector-bearing docs in sorted-id order. Ranging the live map
	// directly would visit ids in Go's randomized order, which would feed KMeans a
	// different initial-centroid set (the first k points) on every build and make
	// the published epoch — centroids, cluster membership, RaBitQ codes — vary
	// run-to-run. Sorting makes the whole vector index byte-for-byte reproducible,
	// matching assignOrdinals' deterministic ordering. Collecting ids and points in
	// lockstep keeps each point paired with its document for the per-doc code.
	ids := make([]string, 0, len(live))
	for id, d := range live {
		if d.Vector != nil {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	sort.Strings(ids)
	points := make([][]float32, len(ids))
	dim := 0
	for i, id := range ids {
		points[i] = live[id].Vector
		dim = len(live[id].Vector)
	}

	centroids, memberIDs, versionByID := clusterLive(ctx, store, ns, prevEpoch, prevIndexedUpTo, walUpTo, metric, ids, live)
	// The realized cluster count may be smaller than ChooseK for tiny N (KMeans
	// clamps k), or differ from the prior epoch's K after splits/merges; trust the
	// length of the returned centroids, not any precomputed K.
	numClusters := len(centroids)

	// Sample this epoch's True RaBitQ rotation once, from a fixed seed, and store
	// it on the CentroidsFile (docs/extensions/true-rabitq.md). It is immutable for
	// the epoch's lifetime, so query-time reads back the exact matrix the encoder
	// used and the binary-scan estimator stays self-consistent with the codes.
	rot := NewRotation(dim, rotationSeed)

	// Bucket members per cluster, computing each document's True RaBitQ code: the
	// sign bits of P⁻¹·(normalized residual) plus the ‖oᵣ−c‖ and ⟨ō,o⟩ scalars the
	// unbiased estimator divides into at query time. We keep the legacy lite Code
	// too so a stale reader (or a recall A/B in the bench) can still score either
	// way; the query path prefers RaBitQ when the epoch carries a rotation.
	//
	// CRITICAL (docs/spfresh-lire/01, "Recomputing residual codes"): a vector's
	// RaBitQ/lite code is the sign-bit residual against ITS centroid, and the
	// incremental pass may have moved a vector to a new centroid (split/merge/
	// reassign) or recentered its posting. We therefore (re)encode every member here
	// against the FINAL centroid it sits in — never carrying a stale code forward —
	// so the prefilter always scores against the right frame.
	vectorByID := make(map[string][]float32, len(ids))
	for i, id := range ids {
		vectorByID[id] = points[i]
	}
	members := make([][]ClusterEntry, numClusters)
	sizes := make([]int, numClusters)
	for c := 0; c < numClusters; c++ {
		for _, id := range memberIDs[c] {
			vec := vectorByID[id]
			code := ResidualCode(vec, centroids[c])
			rabitq := EncodeRaBitQ(vec, centroids[c], rot)
			members[c] = append(members[c], ClusterEntry{
				ID:      id,
				Vector:  vec,
				Code:    code,
				RaBitQ:  &rabitq,
				Attrs:   live[id].Attributes,
				Version: versionByID[id], // 0 for a full rebuild or an un-reassigned member
			})
			sizes[c]++
			// Record the cluster each vector landed in so the attribute index can build
			// the cluster-level pruning map over the SAME assignment the query probes.
			clusterOf[id] = c
		}
	}

	// Build the optional hierarchical centroid tree OVER the flat centroids
	// (docs/extensions/hierarchical-centroid-tree.md). Its leaves route to the SAME
	// flat clusters bucketed above, so it re-routes nothing: the rotated RaBitQ
	// codes (F5) and the bitmap ClusterOf assignment (F4) stay exactly as written.
	// BuildTree returns nil at this clone's scale (K <= leaf cap ⇒ no real
	// hierarchy), in which case the query path keeps the flat O(K) centroid scan.
	tree := BuildTree(centroids, treeFanout, treeLeafCapacity, metric)

	centroidsFile := CentroidsFile{
		Metric:    metric,
		Dimension: dim,
		K:         numClusters,
		Centroids: centroids,
		Sizes:     sizes,
		Rotation:  rot,
		Tree:      tree,
	}
	if err := putJSON(ctx, store, centroidsKey(ns, epoch), centroidsFile); err != nil {
		return fmt.Errorf("writing centroids: %w", err)
	}

	for c := 0; c < numClusters; c++ {
		cf := ClusterFile{
			Cluster:  c,
			Centroid: centroids[c],
			Members:  members[c],
		}
		if err := putJSON(ctx, store, clusterKey(ns, epoch, c), cf); err != nil {
			return fmt.Errorf("writing cluster %d: %w", c, err)
		}
	}
	return nil
}

// clusterLive partitions the live vectors into IVF clusters, returning the final
// centroids and, for each cluster, the sorted ids of its members. It chooses
// between two strategies and is the single seam where SPFresh LIRE Phase 1 plugs
// in (docs/spfresh-lire/02-implementation-in-tpuf.md):
//
//   - INCREMENTAL (Option A): when a prior epoch exists, is loadable, agrees on
//     dimension, and carries the same set of already-indexed ids that the live set
//     still has, copy that epoch's centroids/clusters forward and apply only the
//     LIRE deltas implied by the WAL tail [prevIndexedUpTo, walUpTo): insert new
//     vectors into their nearest posting, split oversized postings, merge under-full
//     ones, and reassign the boundary set that the split/merge knocked out of NPA.
//     Most postings are carried byte-for-byte; only touched ones are recomputed.
//   - FULL REBUILD: otherwise, run k-means over the whole live set exactly as the
//     clone always did. This is the correct fallback for the first build, a branch's
//     initial flatten, a dimension change, or any case too entangled to reconcile
//     incrementally — "a fallback to a correct full rebuild ... is acceptable"
//     (docs/spfresh-lire/02). The fallback path is also what every pre-LIRE test
//     exercises, so existing behavior is preserved bit-for-bit.
//
// CORRECTNESS: both strategies must enforce the SAME invariant — every vector sits
// in the posting whose final centroid is nearest (NPA, SPFresh §3.2) — so a query
// returns the same top-K either way. The incremental path therefore runs Reassign
// to repair NPA after every split/merge; the tests assert incremental and full
// produce the same query results.
func clusterLive(ctx context.Context, store *cache.Store, ns string, prevEpoch, prevIndexedUpTo, walUpTo int64, metric string, ids []string, live map[string]Document) (centroids [][]float32, memberIDs [][]string, versionByID map[string]int) {
	if prevEpoch > 0 {
		if cs, ms, vs, ok := incrementalCluster(ctx, store, ns, prevEpoch, prevIndexedUpTo, walUpTo, metric, ids, live); ok {
			return cs, ms, vs
		}
		// Incremental reconciliation was not safe (unloadable prior epoch, a dimension
		// change, or an indexed id the live set no longer recognizes): fall through to
		// a correct full rebuild. Falling back is always safe — it just costs a full
		// re-cluster — and never produces a wrong epoch.
	}
	return fullCluster(metric, ids, live)
}

// fullCluster is the original from-scratch path: ChooseK + KMeans over every live
// vector, bucketed into per-cluster sorted id lists. It is deterministic (KMeans
// seeds rand.NewSource(1) and the inputs arrive sorted), so the published epoch is
// byte-for-byte reproducible. Every member is freshly assigned, so its version is
// the implicit 0 (a nil version map, read as 0 for any id).
func fullCluster(metric string, ids []string, live map[string]Document) (centroids [][]float32, memberIDs [][]string, versionByID map[string]int) {
	points := make([][]float32, len(ids))
	for i, id := range ids {
		points[i] = live[id].Vector
	}
	k := ChooseK(len(points))
	cs, assign := KMeans(points, k, metric, kmeansIters)
	members := make([][]string, len(cs))
	for i, id := range ids {
		c := assign[i]
		members[c] = append(members[c], id)
	}
	return cs, members, nil
}

// incrementalCluster is SPFresh LIRE Phase 1 (Option A). It loads the prior epoch's
// centroids and cluster members, copies them into the mutable lireIndex working
// set, and applies the LIRE deltas implied by the WAL tail [prevIndexedUpTo,
// walUpTo): the documents the prior epoch did NOT yet cover (new inserts) are added
// to their nearest posting, removed ids (tombstoned or re-upserted as text-only)
// are dropped, then the index is rebalanced — split oversized postings, merge
// under-full ones, reassign the boundary set to restore NPA. It returns the final
// centroids and per-cluster sorted member ids, plus ok=false when the prior epoch
// cannot be reconciled (so the caller falls back to a full rebuild).
//
// The working set is reconciled against the AUTHORITATIVE live set (ids/live, the
// same materialization the full rebuild and the BM25/docs/attribute builds all
// use) rather than trusting the tail delta blindly: any id the prior epoch held but
// the live set no longer has is removed, and any live id absent from the prior
// clusters is inserted. This makes the incremental result provably cover exactly
// the live set — no double-count, no drop — even if the tail delta and the prior
// epoch disagree at the edges. Reconciling against the live set is the belt that
// makes Option A correct without a per-vector journal.
func incrementalCluster(ctx context.Context, store *cache.Store, ns string, prevEpoch, prevIndexedUpTo, walUpTo int64, metric string, ids []string, live map[string]Document) ([][]float32, [][]string, map[string]int, bool) {
	li, oldCentroids, ok := loadPrevEpoch(ctx, store, ns, prevEpoch, metric)
	if !ok {
		return nil, nil, nil, false
	}

	// liveSet is the authoritative set of vector-bearing live ids.
	liveSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		liveSet[id] = true
	}

	// 1. Remove every prior member the live set no longer carries (deletes, and docs
	//    re-upserted without a vector). A removed member's posting is touched.
	present := map[string]bool{}
	touched := map[int]bool{}
	for ci := range li.clusters {
		c := &li.clusters[ci]
		kept := c.members[:0]
		for _, m := range c.members {
			if liveSet[m.ID] {
				kept = append(kept, m)
				present[m.ID] = true
			} else {
				touched[c.id] = true
			}
		}
		c.members = kept
	}

	// 2. Insert every live vector the prior epoch did not already hold into its
	//    nearest posting (LIRE Insert, SPFresh §3.2). A re-upsert whose vector
	//    changed is treated as a fresh insert at its new position. moved collects
	//    every inserted-or-repositioned id: these vectors' OWN positions changed, so
	//    the Eq. 1/Eq. 2 prune (which assumes only the CENTROID moved) does not cover
	//    them — they are force-rechecked in the reassign pass so each lands in its
	//    true-nearest posting, keeping the epoch fully NPA-correct.
	moved := map[string]bool{}
	for _, id := range ids {
		vec := live[id].Vector
		if present[id] {
			// Already carried forward: refresh its stored vector in place in case the
			// re-upsert changed the payload, and force a recheck only if it actually
			// moved (an unchanged vector stays NPA-correct where it sits).
			cid, changed := refreshMemberVector(li, id, vec)
			if cid >= 0 {
				touched[cid] = true
				if changed {
					moved[id] = true
				}
			}
			continue
		}
		landed := li.insertNearest(ClusterEntry{ID: id, Vector: vec, Attrs: live[id].Attributes})
		touched[landed] = true
		moved[id] = true
	}

	// Recenter every posting that gained or lost a member before rebalancing, so the
	// split/merge size decisions and NPA checks run against current centroids.
	for id := range touched {
		if c := li.clusterByID(id); c != nil {
			c.recenter()
		}
	}

	// 3. Rebalance: split oversized postings, merge under-full ones, and reassign the
	//    boundary set both disturbed (SPFresh §3.2). Each rebalancing op records the
	//    old centroids of the postings it changes so Reassign can evaluate the Eq. 1/
	//    Eq. 2 necessary conditions exactly.
	n := li.totalMembers()
	li.splitCap = splitCap(n, len(li.clusters))
	li.mergeMin = mergeMin(n, len(li.clusters))

	splitTouched := li.splitOversized()
	mergeTouched := li.mergeUnderfull()
	for id := range mergeTouched {
		splitTouched[id] = true
	}
	for id := range touched {
		splitTouched[id] = true
	}
	li.reassign(splitTouched, oldCentroids, moved)

	// Drop any posting that ended up empty (all members reassigned or merged out) so
	// the epoch never publishes a zero-member cluster.
	li.pruneEmpty()

	centroids, memberIDs, versionByID, ok := li.snapshot()
	return centroids, memberIDs, versionByID, ok
}

// splitCap / mergeMin derive the LIRE posting-length band from the live count and
// current cluster count: the ideal balanced size is N/K, and a posting is split
// above N/K * splitCapMultiplier or merged below N/K / mergeMinDivisor. Both clamp
// to at least 1 so a tiny namespace never splits forever or merges to nothing.
func splitCap(n, k int) int {
	if k < 1 {
		k = 1
	}
	cap := (n / k) * splitCapMultiplier
	if cap < 1 {
		cap = 1
	}
	return cap
}

func mergeMin(n, k int) int {
	if k < 1 {
		k = 1
	}
	min := (n / k) / mergeMinDivisor
	if min < 1 {
		min = 1
	}
	return min
}

// loadPrevEpoch reads the prior epoch's centroids.json and every cluster-{i}.json
// (immutable index/v{epoch}/* objects, so served via GetCached, rule-2-safe) into a
// mutable lireIndex working set, and returns the prior centroid of each posting
// keyed by its cluster id (for Reassign's Eq. 1/Eq. 2 checks). ok=false on any read
// or decode failure or a dimension mismatch, so the caller falls back to a full
// rebuild rather than risk an inconsistent carry-forward. The prior cluster's index
// i becomes the lireCluster id, and nextID starts past the largest so splits mint
// non-colliding ids.
func loadPrevEpoch(ctx context.Context, store *cache.Store, ns string, prevEpoch int64, metric string) (*lireIndex, map[int][]float32, bool) {
	body, err := store.GetCached(ctx, centroidsKey(ns, prevEpoch))
	if err != nil {
		return nil, nil, false // no prior centroids (text-only or unreadable): full rebuild.
	}
	var cf CentroidsFile
	if err := json.Unmarshal(body, &cf); err != nil {
		return nil, nil, false
	}
	if len(cf.Centroids) == 0 {
		return nil, nil, false
	}

	li := &lireIndex{metric: metric, dim: cf.Dimension}
	oldCentroids := make(map[int][]float32, len(cf.Centroids))
	maxID := -1
	for i := range cf.Centroids {
		cbody, err := store.GetCached(ctx, clusterKey(ns, prevEpoch, i))
		if err != nil {
			return nil, nil, false
		}
		var clf ClusterFile
		if err := json.Unmarshal(cbody, &clf); err != nil {
			return nil, nil, false
		}
		// Carry members forward with their stored Version; reassign bumps it from
		// here. The on-disk Vector is the source of truth for the carried position.
		members := make([]ClusterEntry, len(clf.Members))
		copy(members, clf.Members)
		li.clusters = append(li.clusters, lireCluster{
			id:       i,
			centroid: cloneVec(cf.Centroids[i]),
			members:  members,
		})
		oldCentroids[i] = cloneVec(cf.Centroids[i])
		if i > maxID {
			maxID = i
		}
	}
	li.nextID = maxID + 1
	return li, oldCentroids, true
}

// refreshMemberVector updates the stored vector of an already-carried member whose
// re-upsert may have changed its payload, returning the id of the posting it lives
// in (so the caller marks it touched, or -1 if not found) and whether the vector
// actually changed. The member stays in its current posting; if the new vector
// belongs elsewhere, the caller force-reassigns it (changed == true) and the
// Reassign pass moves it to its true-nearest posting. An unchanged re-upsert (same
// vector, new attributes) needs no move, so changed == false skips the recheck.
func refreshMemberVector(li *lireIndex, id string, vec []float32) (clusterID int, changed bool) {
	for ci := range li.clusters {
		c := &li.clusters[ci]
		if idx := memberIndex(c.members, id); idx >= 0 {
			changed = !equalVec(c.members[idx].Vector, vec)
			c.members[idx].Vector = vec
			return c.id, changed
		}
	}
	return -1, false
}

// equalVec reports whether two float32 vectors are element-wise identical. Used to
// decide whether a re-upsert actually moved a vector (and so needs an NPA recheck)
// or only changed non-vector payload.
func equalVec(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// pruneEmpty removes any posting left with zero members after rebalancing so the
// snapshot never emits an empty cluster. It keeps at least one posting alive even
// if every vector was somehow removed (an all-delete tail), because snapshot/write
// expect a non-empty centroid set when there are live vectors — and there are, or
// incrementalCluster would not have run.
func (li *lireIndex) pruneEmpty() {
	out := li.clusters[:0]
	for _, c := range li.clusters {
		if len(c.members) > 0 {
			out = append(out, c)
		}
	}
	li.clusters = out
}

// snapshot freezes the working set into the (centroids, per-cluster sorted member
// ids, per-id version) shape buildVectorIndex serializes. Clusters are emitted in
// ascending id order and each cluster's members sorted by id, so the produced epoch
// is byte-for-byte reproducible (matching the full-rebuild path's determinism). The
// version map carries each member's current version — bumped by Reassign — out to
// the writer so the on-disk ClusterEntry.Version reflects the move (the version-map
// groundwork persisting to storage). ok=false when the working set is empty, which
// cannot happen for a non-empty live vector set but is guarded so the caller falls
// back rather than write nothing.
func (li *lireIndex) snapshot() ([][]float32, [][]string, map[string]int, bool) {
	if len(li.clusters) == 0 {
		return nil, nil, nil, false
	}
	order := make([]int, len(li.clusters))
	for i := range li.clusters {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return li.clusters[order[a]].id < li.clusters[order[b]].id })

	centroids := make([][]float32, len(order))
	memberIDs := make([][]string, len(order))
	versionByID := make(map[string]int)
	for out, ci := range order {
		c := li.clusters[ci]
		centroids[out] = cloneVec(c.centroid)
		ids := make([]string, len(c.members))
		for i, m := range c.members {
			ids[i] = m.ID
			if m.Version != 0 {
				versionByID[m.ID] = m.Version
			}
		}
		sort.Strings(ids)
		memberIDs[out] = ids
	}
	return centroids, memberIDs, versionByID, true
}

// buildBM25Index builds the inverted index over the configured text field of
// every live document and writes bm25.json. A document whose text field is
// absent or non-string contributes an empty document (zero-length, no terms),
// which BuildBM25 handles. The field value is read from a document's attributes
// because the text lives alongside the other payload, not on a dedicated
// Document field.
func buildBM25Index(ctx context.Context, store *cache.Store, ns string, epoch int64, textField string, live map[string]Document) error {
	texts := make(map[string]string, len(live))
	for id, d := range live {
		texts[id] = textFieldValue(d.Attributes, textField)
	}

	idx := BuildBM25(texts)
	if err := putJSON(ctx, store, bm25Key(ns, epoch), idx); err != nil {
		return fmt.Errorf("writing bm25: %w", err)
	}
	return nil
}

// textFieldValue extracts the text-field value from a document's attributes,
// returning "" when the field is missing or not a string. Returning "" rather
// than erroring keeps a malformed or text-less document indexable (it simply
// matches no query term) instead of failing the whole build.
func textFieldValue(attrs map[string]any, field string) string {
	v, ok := attrs[field]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// writeDocs writes docs.json: a map of document id -> attributes for every live
// document. BM25 hits read their attributes from here (vector hits carry attrs
// inline in the cluster file). It is always written, even for an empty
// namespace, so query-time can rely on its presence under a published epoch.
func writeDocs(ctx context.Context, store *cache.Store, ns string, epoch int64, live map[string]Document) error {
	docs := make(map[string]map[string]any, len(live))
	for id, d := range live {
		docs[id] = d.Attributes
	}
	if err := putJSON(ctx, store, docsKey(ns, epoch), DocsFile{Docs: docs}); err != nil {
		return fmt.Errorf("writing docs: %w", err)
	}
	return nil
}

// assignOrdinals gives every live document a stable dense [0, N) ordinal in
// sorted-id order. A deterministic order (rather than Go's randomized map range)
// keeps the attribute index — and therefore query results that read it — byte-for-byte
// reproducible across builds of the same data, which the indexer tests rely on.
func assignOrdinals(live map[string]Document) map[string]int {
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	ord := make(map[string]int, len(ids))
	for i, id := range ids {
		ord[id] = i
	}
	return ord
}

// High-cardinality fields are skipped: a field whose distinct values approach the
// document count (a UUID, a per-row timestamp) produces one near-singleton bitmap
// per value — pure storage overhead with no pruning value, since almost every
// query value then matches almost nothing or everything. Skipping such fields is
// the honest scope for this clone (docs/extensions/bitmap-attribute-indexes.md).
// The query path falls back to Filter.Match for any field left unindexed, so this
// only affects speed, never correctness.
//
// A field is skipped only when its distinct-value count exceeds BOTH an absolute
// floor and a fraction of N. The floor keeps small categorical fields (a handful
// of langs or tiers) always indexed regardless of dataset size — where the ratio
// alone would wrongly drop a 3-value field in a 5-document test — while the ratio
// catches the genuinely unique-per-document fields at scale.
const (
	minFieldCardinality      = 64  // never skip a field with at most this many values
	maxFieldCardinalityRatio = 0.5 // above this fraction of N (and the floor) ⇒ skip
)

// buildAttributeIndex materializes the bitmap attribute index — bitmaps.json — for
// the epoch. It walks the same live document set the vector and BM25 builds read,
// assigns each (field, value) the sorted ordinals of the documents carrying it,
// and records the id and cluster of every ordinal so the query planner can both
// translate bits back to documents and prune clusters that cannot match a filter
// (docs/extensions/bitmap-attribute-indexes.md).
//
// It is written with the same unconditional putJSON as every other epoch object —
// write-once under the fresh epoch prefix, no extra CAS — and goes live atomically
// with the rest of the epoch at BuildIndex's single SaveManifestCAS (rule 4).
//
// Excluded fields: the configured text field (that is the BM25 surface, not a
// filter predicate), any non-equality-comparable value (only scalars an "eq"
// filter can target are indexable), and any field whose cardinality exceeds
// maxFieldCardinalityRatio of N (high-cardinality fields give no pruning value).
func buildAttributeIndex(ctx context.Context, store *cache.Store, ns string, epoch int64, textField string, live map[string]Document, ordinals map[string]int, clusterOf map[string]int) error {
	n := len(live)
	af := AttrsFile{
		Ords:      make([]string, n),
		ClusterOf: make([]int, n),
		Values:    map[string]map[string][]uint32{},
	}

	// values[field][valueKey] accumulates a bitmap of the ordinals carrying that
	// value. Bitmaps stay sorted/compact internally and serialize as their ordinal
	// list (bitmap.toSorted).
	values := map[string]map[string]*bitmap{}

	for id, ord := range ordinals {
		af.Ords[ord] = id
		c, ok := clusterOf[id]
		if !ok {
			c = -1 // vector-less document: it sits in no IVF cluster.
		}
		af.ClusterOf[ord] = c

		for field, raw := range live[id].Attributes {
			if field == textField {
				continue // the BM25 text surface is never a bitmap filter field.
			}
			key, ok := valueKey(raw)
			if !ok {
				continue // non-scalar values cannot be an "eq" target; skip them.
			}
			fv := values[field]
			if fv == nil {
				fv = map[string]*bitmap{}
				values[field] = fv
			}
			bm := fv[key]
			if bm == nil {
				bm = newBitmap()
				fv[key] = bm
			}
			bm.add(uint32(ord))
		}
	}

	// Drop high-cardinality fields, then freeze the surviving bitmaps to their
	// sorted-ordinal on-disk form.
	threshold := int(float64(n) * maxFieldCardinalityRatio)
	for field, fv := range values {
		if len(fv) > minFieldCardinality && len(fv) > threshold {
			continue // too many distinct values to be worth indexing.
		}
		out := make(map[string][]uint32, len(fv))
		for key, bm := range fv {
			out[key] = bm.toSorted()
		}
		af.Values[field] = out
	}

	if err := putJSON(ctx, store, attrsKey(ns, epoch), af); err != nil {
		return fmt.Errorf("writing attribute bitmaps: %w", err)
	}
	return nil
}

// valueKey maps a scalar attribute value to the canonical string key the bitmap
// index buckets it under, reporting ok=false for non-scalar values that an "eq"
// filter cannot target (maps, slices). Numbers are formatted through float64 so
// that, exactly like Filter.Match's numeric coercion, JSON-decoded numbers and Go
// int/float literals land in the same bucket: a document attribute 5, a JSON 5.0,
// and a query filter Value of int(5) all key to the same value.
func valueKey(v any) (string, bool) {
	if f, ok := numericValue(v); ok {
		return "n:" + strconv.FormatFloat(f, 'g', -1, 64), true
	}
	switch x := v.(type) {
	case string:
		return "s:" + x, true
	case bool:
		return "b:" + strconv.FormatBool(x), true
	default:
		return "", false
	}
}

// putJSON marshals v and writes it unconditionally under key. Index objects are
// write-once under a fresh epoch prefix, so an unconditional Put is correct: no
// concurrent writer can target the same key, and the publish only becomes
// visible at the final manifest CAS.
func putJSON(ctx context.Context, store *cache.Store, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling %q: %w", key, err)
	}
	if _, err := store.Put(ctx, key, body); err != nil {
		return fmt.Errorf("putting %q: %w", key, err)
	}
	return nil
}
