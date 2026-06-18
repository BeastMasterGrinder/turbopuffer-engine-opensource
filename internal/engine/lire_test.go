package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
)

// ─────────────────────────── LIRE math unit tests ───────────────────────────
//
// These exercise the split / merge / reassign primitives directly on the
// in-memory lireIndex working set (no storage), the way the SPFresh paper reasons
// about LIRE: a posting that crosses a length cap is split with a local 2-means,
// an under-full posting is merged into its nearest neighbor, and the small
// boundary set the change disturbs is reassigned to its NPA-correct posting with a
// bumped version. They are the unit of correctness the end-to-end epoch tests then
// confirm survives the publish path.

// makeEntries builds ClusterEntry members from id->vector pairs in sorted-id order
// so the working set is deterministic.
func makeEntries(vecs map[string][]float32) []ClusterEntry {
	ids := make([]string, 0, len(vecs))
	for id := range vecs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]ClusterEntry, len(ids))
	for i, id := range ids {
		out[i] = ClusterEntry{ID: id, Vector: vecs[id]}
	}
	return out
}

// memberIDsOf returns the sorted ids currently in the cluster with the given id.
func memberIDsOf(li *lireIndex, clusterID int) []string {
	c := li.clusterByID(clusterID)
	if c == nil {
		return nil
	}
	ids := make([]string, len(c.members))
	for i, m := range c.members {
		ids[i] = m.ID
	}
	sort.Strings(ids)
	return ids
}

func TestLIRESplitOversizedDividesAndRetiresParent(t *testing.T) {
	t.Parallel()
	// One oversized posting holding two well-separated blobs: a 2-means split must
	// cut it into two postings, retiring the parent id.
	left := map[string][]float32{
		"l0": {0, 0}, "l1": {0, 1}, "l2": {1, 0}, "l3": {1, 1},
	}
	right := map[string][]float32{
		"r0": {10, 10}, "r1": {10, 11}, "r2": {11, 10}, "r3": {11, 11},
	}
	all := map[string][]float32{}
	for id, v := range left {
		all[id] = v
	}
	for id, v := range right {
		all[id] = v
	}
	li := &lireIndex{
		clusters:     []lireCluster{{id: 0, centroid: []float32{5, 5}, members: makeEntries(all)}},
		nextID:       1,
		metric:       MetricEuclidean,
		splitCap:     4, // 8 members > 4 ⇒ split
		mergeMin:     1,
		reassignTopN: 64,
	}

	touched := li.splitOversized()
	if len(li.clusters) != 2 {
		t.Fatalf("after split: got %d clusters, want 2", len(li.clusters))
	}
	if li.clusterByID(0) != nil {
		t.Errorf("parent cluster 0 must be retired after a split")
	}
	if len(touched) != 2 {
		t.Errorf("split touched set: got %d ids, want the 2 children", len(touched))
	}
	// Each child must hold exactly one blob (no member lost or duplicated).
	total := 0
	for id := range touched {
		ms := memberIDsOf(li, id)
		total += len(ms)
		if len(ms) != 4 {
			t.Errorf("child %d size: got %d, want 4 (one blob)", id, len(ms))
		}
	}
	if total != 8 {
		t.Errorf("total members after split: got %d, want 8", total)
	}
}

func TestLIRESplitIndivisibleIsLeftIntact(t *testing.T) {
	t.Parallel()
	// A single-member posting cannot split into two non-empty children no matter the
	// cap, so split2 reports ok=false and the loop must terminate leaving it intact
	// rather than spinning. (This is the hard non-termination guard; a multi-point
	// posting always splits because KMeans+reseed separates even identical points.)
	solo := map[string][]float32{"only": {1, 1}}
	li := &lireIndex{
		clusters:     []lireCluster{{id: 0, centroid: []float32{1, 1}, members: makeEntries(solo)}},
		nextID:       1,
		metric:       MetricEuclidean,
		splitCap:     0, // 1 member > 0 ⇒ flagged oversized, but it is indivisible
		mergeMin:     1,
		reassignTopN: 64,
	}
	li.splitOversized()
	if len(li.clusters) != 1 || len(li.clusterByID(0).members) != 1 {
		t.Fatalf("single-member posting must stay one cluster of 1, got %d clusters", len(li.clusters))
	}
}

func TestLIREMergeUnderfullFoldsIntoNearest(t *testing.T) {
	t.Parallel()
	// A tiny posting near a big one must be merged INTO the big one (its nearest
	// neighbor), and its id retired.
	big := makeEntries(map[string][]float32{"b0": {0, 0}, "b1": {0, 1}, "b2": {1, 0}})
	tiny := makeEntries(map[string][]float32{"t0": {0, 2}}) // 1 member, near big
	far := makeEntries(map[string][]float32{"f0": {50, 50}, "f1": {50, 51}, "f2": {51, 50}})
	li := &lireIndex{
		clusters: []lireCluster{
			{id: 0, centroid: []float32{0, 0}, members: big},
			{id: 1, centroid: []float32{0, 2}, members: tiny},
			{id: 2, centroid: []float32{50, 50}, members: far},
		},
		nextID:       3,
		metric:       MetricEuclidean,
		splitCap:     100,
		mergeMin:     2, // posting 1 has 1 member < 2 ⇒ merge
		reassignTopN: 64,
	}
	touched := li.mergeUnderfull()
	if li.clusterByID(1) != nil {
		t.Errorf("under-full posting 1 must be removed after merge")
	}
	if !touched[0] {
		t.Errorf("merge target 0 must be in the touched set, got %v", touched)
	}
	if got := memberIDsOf(li, 0); len(got) != 4 {
		t.Errorf("merge target size: got %d members %v, want 4 (3 + the absorbed 1)", len(got), got)
	}
	// The far posting is untouched.
	if got := memberIDsOf(li, 2); len(got) != 3 {
		t.Errorf("far posting must be untouched: got %d members, want 3", len(got))
	}
}

func TestLIREReassignMovesOnlyBoundaryAndShadowsStaleCopy(t *testing.T) {
	t.Parallel()
	// Two postings A (around x=0) and B (around x=10). A boundary vector "edge"
	// sits in A but is actually nearest to B's centroid — a stale assignment a
	// prior split/merge could leave. Reassign must MOVE only "edge", leave every
	// interior vector put, and stamp the moved copy with a bumped version.
	aMembers := makeEntries(map[string][]float32{
		"a0": {0, 0}, "a1": {0, 1}, "a2": {1, 0}, "edge": {9, 0},
	})
	bMembers := makeEntries(map[string][]float32{
		"b0": {10, 0}, "b1": {10, 1}, "b2": {11, 0},
	})
	li := &lireIndex{
		clusters: []lireCluster{
			{id: 0, centroid: []float32{0, 0}, members: aMembers},
			{id: 1, centroid: []float32{10, 0}, members: bMembers},
		},
		nextID:       2,
		metric:       MetricEuclidean,
		splitCap:     100,
		mergeMin:     1,
		reassignTopN: 64,
	}
	// Recenter so centroids reflect current members; "edge" at x=9 pulls A's
	// centroid right but its true-nearest centroid is still B's.
	for ci := range li.clusters {
		li.clusters[ci].recenter()
	}

	// Old centroids the conditions test against: pretend B (id 1) just changed.
	oldCentroids := map[int][]float32{
		0: {0, 0},
		1: {12, 0}, // B's centroid moved closer to "edge" than it used to be.
	}
	dirty := li.reassign(map[int]bool{1: true}, oldCentroids, nil)

	// "edge" must now live in B, not A.
	if memberIndex(li.clusterByID(0).members, "edge") >= 0 {
		t.Errorf("boundary vector 'edge' must have left posting A")
	}
	idx := memberIndex(li.clusterByID(1).members, "edge")
	if idx < 0 {
		t.Fatalf("boundary vector 'edge' must have moved into posting B")
	}
	// The moved copy carries a BUMPED version (the version-map shadow of the stale
	// copy, SPFresh §3.3 / §4.2.1).
	if v := li.clusterByID(1).members[idx].Version; v != 1 {
		t.Errorf("reassigned copy version: got %d, want 1 (bumped from 0)", v)
	}
	// Only the boundary vector moved: A's interior vectors stay, B keeps its own.
	for _, id := range []string{"a0", "a1", "a2"} {
		if memberIndex(li.clusterByID(0).members, id) < 0 {
			t.Errorf("interior vector %q must NOT have moved out of A", id)
		}
	}
	if !dirty[0] || !dirty[1] {
		t.Errorf("both postings A and B should be marked dirty after the move, got %v", dirty)
	}
	// No id is duplicated across postings (append-then-delete left exactly one copy).
	seen := map[string]int{}
	for ci := range li.clusters {
		for _, m := range li.clusters[ci].members {
			seen[m.ID]++
		}
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("id %q appears %d times across postings, want exactly 1 (stale copy not dropped)", id, n)
		}
	}
}

func TestLIREReassignAbortsFalsePositives(t *testing.T) {
	t.Parallel()
	// Every vector already sits in its true-nearest posting: reassign must move
	// NOTHING (every candidate is a false positive aborted by the NPA re-check).
	a := makeEntries(map[string][]float32{"a0": {0, 0}, "a1": {0, 1}})
	b := makeEntries(map[string][]float32{"b0": {10, 0}, "b1": {10, 1}})
	li := &lireIndex{
		clusters: []lireCluster{
			{id: 0, centroid: []float32{0, 0}, members: a},
			{id: 1, centroid: []float32{10, 0}, members: b},
		},
		nextID:       2,
		metric:       MetricEuclidean,
		splitCap:     100,
		mergeMin:     1,
		reassignTopN: 64,
	}
	for ci := range li.clusters {
		li.clusters[ci].recenter()
	}
	li.reassign(map[int]bool{0: true, 1: true}, map[int][]float32{0: {0, 0}, 1: {10, 0}}, nil)
	if got := memberIDsOf(li, 0); len(got) != 2 {
		t.Errorf("posting A must keep its 2 members, got %v", got)
	}
	if got := memberIDsOf(li, 1); len(got) != 2 {
		t.Errorf("posting B must keep its 2 members, got %v", got)
	}
}

// ─────────────────────── End-to-end epoch correctness ───────────────────────

// gridDocs returns n documents spread across well-separated blobs in the 4-dim
// test space so clustering forms real, recoverable partitions. Each blob centers
// far from the others (blob*100 on two axes) with a small deterministic jitter, so
// every doc's nearest blob is unambiguous and exact nearest-neighbor top-K is
// stable. Each doc carries the text field too.
func gridDocs(n int) []Document {
	docs := make([]Document, n)
	for i := 0; i < n; i++ {
		blob := i % 6
		bx := float32(blob) * 100
		by := float32(blob) * 100
		docs[i] = vecDoc(
			fmt.Sprintf("d%03d", i),
			[]float32{bx + float32(i%4), by + float32((i/6)%4), float32(i % 3), float32(i % 5)},
			fmt.Sprintf("doc number %d in blob %d", i, blob),
		)
	}
	return docs
}

// exactTopK returns the ids of the k nearest docs to query by full-precision
// distance — the ground-truth ranking an approximate IVF epoch is judged against.
// Ties on distance break on id, matching the engine's deterministic ordering.
func exactTopK(docs []Document, metric string, query []float32, k int) []string {
	type hit struct {
		id   string
		dist float64
	}
	hits := make([]hit, 0, len(docs))
	for _, d := range docs {
		if d.Vector == nil {
			continue
		}
		hits = append(hits, hit{id: d.ID, dist: Distance(metric, query, d.Vector)})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dist != hits[j].dist {
			return hits[i].dist < hits[j].dist
		}
		return hits[i].id < hits[j].id
	})
	if k > len(hits) {
		k = len(hits)
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		out[i] = hits[i].id
	}
	return out
}

func TestIncrementalEpochReturnsCorrectTopK(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig()

	// Build the SAME logical data two ways:
	//   A) one full rebuild over all docs (the established-correct baseline),
	//   B) an initial build over the first half, then a SECOND build that folds the
	//      second half in incrementally (copy-forward + LIRE deltas).
	// Both partitions are approximate IVF, and two valid NPA partitions can probe
	// different clusters — so the honest correctness claim is not "identical bytes"
	// but "both return the EXACT nearest neighbors under exhaustive probing." We
	// assert each epoch's top-K equals the brute-force ground truth, which proves the
	// incrementally-rebalanced epoch is as correct as a full rebuild.
	docs := gridDocs(48)
	firstHalf := docs[:24]
	secondHalf := docs[24:]

	fullStore := setupNS(ctx, t, cfg, [][]Document{docs})
	if err := BuildIndex(ctx, fullStore, testNS); err != nil {
		t.Fatalf("full BuildIndex: %v", err)
	}

	incStore := setupNS(ctx, t, cfg, [][]Document{firstHalf})
	if err := BuildIndex(ctx, incStore, testNS); err != nil {
		t.Fatalf("incremental first BuildIndex: %v", err)
	}
	seedTail(ctx, t, incStore, [][]Document{secondHalf})
	if err := BuildIndex(ctx, incStore, testNS); err != nil {
		t.Fatalf("incremental second BuildIndex: %v", err)
	}

	im, _, err := LoadManifest(ctx, incStore, testNS)
	if err != nil {
		t.Fatalf("LoadManifest inc: %v", err)
	}
	if im.IndexEpoch != 2 {
		t.Fatalf("incremental IndexEpoch: got %d, want 2", im.IndexEpoch)
	}
	if im.DocCount != 48 {
		t.Errorf("incremental DocCount: got %d, want 48", im.DocCount)
	}

	// Make the read lossless so we test the PARTITION, not the approximate RaBitQ
	// prefilter: exhaustive probing (nProbe ≫ K) fetches every cluster, and a TopK
	// large enough that TopK*shortlistMultiplier covers the whole corpus means the
	// prefilter shortlist keeps every candidate before the exact rerank. Under those
	// settings the IVF read degenerates to a brute-force exact scan, so a correct
	// epoch — full OR incrementally rebalanced — must return the exact nearest
	// neighbors. We assert both against the brute-force ground truth (and therefore
	// against each other): the incremental epoch is exactly as correct as a full
	// rebuild.
	const exhaustiveNProbe = 1000
	losslessTopK := (len(docs) / shortlistMultiplier) + 1 // TopK*mult >= corpus ⇒ no prefilter loss
	for qi := 0; qi < 10; qi++ {
		blob := qi % 6
		base := float32(blob) * 100
		q := []float32{base, base, float32(qi % 3), float32(qi % 5)}
		p := QueryParams{RankBy: RankBy{Vector: q}, TopK: losslessTopK, NProbe: exhaustiveNProbe}

		want := exactTopK(docs, cfg.Metric, q, losslessTopK)

		fullRes, err := RunQuery(ctx, fullStore, testNS, p)
		if err != nil {
			t.Fatalf("full RunQuery: %v", err)
		}
		if got := resultIDs(fullRes); !equalStrings(got, want) {
			t.Errorf("query %d FULL top-K: got %v, want exact %v", qi, got, want)
		}
		incRes, err := RunQuery(ctx, incStore, testNS, p)
		if err != nil {
			t.Fatalf("inc RunQuery: %v", err)
		}
		if got := resultIDs(incRes); !equalStrings(got, want) {
			t.Errorf("query %d INCREMENTAL top-K: got %v, want exact %v", qi, got, want)
		}
	}
}

func TestIncrementalEpochCoversExactLiveSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig()

	// Initial build, then a tail that inserts new docs, deletes one, and re-upserts
	// another with a moved vector. The incremental epoch must cover EXACTLY the live
	// set: every cluster member is live, every live vector appears once.
	store := setupNS(ctx, t, cfg, [][]Document{gridDocs(15)})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("first BuildIndex: %v", err)
	}
	seedTail(ctx, t, store, [][]Document{
		{
			vecDoc("new1", []float32{2, 2, 0, 0}, "fresh one"),
			vecDoc("new2", []float32{12, 12, 1, 1}, "fresh two"),
			tombstone("d000"), // delete an indexed doc
			vecDoc("d001", []float32{40, 40, 1, 4}, "moved far"), // re-upsert, moved vector
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("second BuildIndex: %v", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}

	// Gather every member across the published clusters.
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, m.IndexEpoch), &cf)
	seen := map[string]int{}
	for c := 0; c < cf.K; c++ {
		var clf ClusterFile
		loadJSON(ctx, t, store, clusterKey(testNS, m.IndexEpoch, c), &clf)
		for _, mem := range clf.Members {
			seen[mem.ID]++
		}
	}
	if seen["d000"] != 0 {
		t.Errorf("deleted id d000 must not appear in any cluster")
	}
	if seen["new1"] != 1 || seen["new2"] != 1 {
		t.Errorf("fresh ids must each appear once, got new1=%d new2=%d", seen["new1"], seen["new2"])
	}
	if seen["d001"] != 1 {
		t.Errorf("re-upserted id d001 must appear exactly once, got %d", seen["d001"])
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("id %q appears %d times, want exactly 1", id, n)
		}
	}

	// The published member set equals the live vector-bearing set: 15 - 1 deleted + 2
	// new = 16 (d001 stays, just moved).
	if len(seen) != 16 {
		t.Errorf("published member count: got %d, want 16", len(seen))
	}
}

func TestIncrementalEpochKeepsBalancedSpread(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig()

	// Dump a large skewed batch into one neighborhood after an initial balanced
	// build. Without LIRE the hot posting would balloon; the split pass must keep the
	// largest posting within the split cap band so the size spread stays bounded.
	store := setupNS(ctx, t, cfg, [][]Document{gridDocs(20)})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("first BuildIndex: %v", err)
	}

	// 30 nearly-identical vectors all landing in the same neighborhood (blob 0).
	skew := make([]Document, 30)
	for i := range skew {
		skew[i] = vecDoc(fmt.Sprintf("hot%03d", i), []float32{0, float32(i % 2), 0, 0}, "hot region doc")
	}
	seedTail(ctx, t, store, [][]Document{skew})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("second BuildIndex: %v", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, m.IndexEpoch), &cf)

	total, maxSize := 0, 0
	for _, s := range cf.Sizes {
		total += s
		if s > maxSize {
			maxSize = s
		}
	}
	if total != 50 {
		t.Fatalf("total indexed vectors: got %d, want 50", total)
	}
	// The hot region (~30 vectors) must have been split: no single posting may hold
	// the entire skew batch. The split cap is ~2*N/K; assert the largest posting is
	// comfortably under the un-split size (30) to prove rebalancing happened.
	if maxSize >= 30 {
		t.Errorf("largest posting holds %d/%d vectors — the skew batch was not split", maxSize, total)
	}
}

func TestClusterEntryVersionRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := NamespaceConfig{Dimension: 4, Metric: "euclidean", TextField: ""}

	// Build directly on testNS: seed an initial split-prone layout, then move a doc
	// across the boundary via re-upsert so the next incremental build reassigns it
	// and bumps its version — and that version must survive the JSON write/read of
	// the published cluster file.
	base := [][]Document{{
		{ID: "a0", Vector: []float32{0, 0, 0, 0}},
		{ID: "a1", Vector: []float32{0, 1, 0, 0}},
		{ID: "b0", Vector: []float32{20, 0, 0, 0}},
		{ID: "b1", Vector: []float32{20, 1, 0, 0}},
	}}
	store2 := setupNS(ctx, t, cfg, base)
	if err := BuildIndex(ctx, store2, testNS); err != nil {
		t.Fatalf("first BuildIndex: %v", err)
	}
	// Tail: a doc that starts indexed near A but we move it next to B via re-upsert,
	// forcing an NPA reassign on the next build.
	seedTail(ctx, t, store2, [][]Document{{
		{ID: "mover", Vector: []float32{1, 0, 0, 0}}, // near A first
	}})
	if err := BuildIndex(ctx, store2, testNS); err != nil {
		t.Fatalf("second BuildIndex: %v", err)
	}
	seedTail(ctx, t, store2, [][]Document{{
		{ID: "mover", Vector: []float32{20, 0, 0, 0}}, // now squarely in B
	}})
	if err := BuildIndex(ctx, store2, testNS); err != nil {
		t.Fatalf("third BuildIndex: %v", err)
	}

	// Read every published cluster and confirm: the version field decodes (round
	// trips), no id is duplicated, and any moved copy that carries a non-zero version
	// decodes to the same value it was written with. We assert the JSON tag works by
	// decoding into ClusterEntry and reading .Version without error.
	m, _, err := LoadManifest(ctx, store2, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	var cf CentroidsFile
	loadJSON(ctx, t, store2, centroidsKey(testNS, m.IndexEpoch), &cf)
	seen := map[string]bool{}
	maxVersion := 0
	for c := 0; c < cf.K; c++ {
		var clf ClusterFile
		loadJSON(ctx, t, store2, clusterKey(testNS, m.IndexEpoch, c), &clf)
		for _, mem := range clf.Members {
			if seen[mem.ID] {
				t.Errorf("id %q duplicated across clusters", mem.ID)
			}
			seen[mem.ID] = true
			if mem.Version > maxVersion {
				maxVersion = mem.Version
			}
			if mem.Version < 0 {
				t.Errorf("decoded version for %q is negative: %d", mem.ID, mem.Version)
			}
		}
	}
	// "mover" must be present exactly once and near B.
	if !seen["mover"] {
		t.Errorf("re-upserted 'mover' missing from the published epoch")
	}
	// A real reassign happened (mover crossed the A→B boundary), so at least one
	// member must carry a bumped, non-zero version that survived the JSON write/read.
	if maxVersion < 1 {
		t.Errorf("expected at least one reassigned member with version >= 1 on disk, got max %d", maxVersion)
	}

	// Explicitly prove the JSON tag round-trips a non-zero version by encoding a
	// known entry and decoding it back.
	enc := ClusterEntry{ID: "x", Vector: []float32{1, 2, 3, 4}, Version: 7}
	body, err := json.Marshal(enc)
	if err != nil {
		t.Fatalf("marshal ClusterEntry: %v", err)
	}
	var dec ClusterEntry
	if err := json.Unmarshal(body, &dec); err != nil {
		t.Fatalf("unmarshal ClusterEntry: %v", err)
	}
	if dec.Version != 7 {
		t.Errorf("version did not round-trip through JSON: got %d, want 7", dec.Version)
	}
	// And an omitted version decodes to 0 (the omitempty / pre-LIRE default).
	var zero ClusterEntry
	if err := json.Unmarshal([]byte(`{"ID":"y","Vector":[0,0,0,0]}`), &zero); err != nil {
		t.Fatalf("unmarshal versionless ClusterEntry: %v", err)
	}
	if zero.Version != 0 {
		t.Errorf("versionless entry must decode to version 0, got %d", zero.Version)
	}
}

func sortedCopy(in []string) []string {
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}
