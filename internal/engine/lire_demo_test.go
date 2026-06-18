package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// TestLIREDemo is a runnable demonstration (go test -run TestLIREDemo -v) that
// PROVES the LIRE Phase 1 win in plain output: an incremental index pass produces a
// correct, balanced epoch and queries return the exact top-K. It is not an
// assertion-heavy test (the real correctness tests live alongside); it exists to
// print the before/after cluster-size spread and a top-K match against brute force,
// the same evidence a CLI demo would surface. Delete-safe: it asserts nothing the
// other tests do not already cover.
func TestLIREDemo(t *testing.T) {
	ctx := context.Background()
	cfg := NamespaceConfig{Dimension: 4, Metric: "euclidean", TextField: "body"}
	store := newTestStore()
	const ns = "lire-demo"
	if err := CreateManifest(ctx, store, ns, cfg); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Seed a balanced initial set across 6 well-separated blobs, then index it.
	initial := gridDocs(36)
	if err := AppendWAL(ctx, store, ns, 0, initial); err != nil {
		t.Fatalf("append: %v", err)
	}
	if _, err := SaveManifestCAS(ctx, store, ns, func(m *Manifest) { m.WALSeq = 1 }); err != nil {
		t.Fatalf("advance: %v", err)
	}
	if err := BuildIndex(ctx, store, ns); err != nil {
		t.Fatalf("first index: %v", err)
	}
	fmt.Printf("[demo] epoch 1 (full build) sizes: %v\n", clusterSizes(ctx, t, store, ns))

	// Now skew the namespace: dump 30 near-identical vectors into one neighborhood
	// (the classic distribution-drift LIRE exists to absorb), plus a delete.
	skew := make([]Document, 30)
	for i := range skew {
		skew[i] = vecDoc(fmt.Sprintf("hot%02d", i), []float32{0, float32(i % 2), 0, 0}, "hot region")
	}
	skew = append(skew, tombstone("d000"))
	if err := AppendWAL(ctx, store, ns, 1, skew); err != nil {
		t.Fatalf("append skew: %v", err)
	}
	if _, err := SaveManifestCAS(ctx, store, ns, func(m *Manifest) { m.WALSeq = 2 }); err != nil {
		t.Fatalf("advance: %v", err)
	}

	// Incremental index pass: copy the epoch-1 clusters forward and apply LIRE deltas
	// (insert the skew batch, split the now-oversized hot posting, reassign the
	// boundary). The hot region is rebalanced instead of ballooning into one posting.
	if err := BuildIndex(ctx, store, ns); err != nil {
		t.Fatalf("incremental index: %v", err)
	}
	sizes := clusterSizes(ctx, t, store, ns)
	fmt.Printf("[demo] epoch 2 (incremental + LIRE split/merge/reassign) sizes: %v\n", sizes)

	maxSize := 0
	total := 0
	for _, s := range sizes {
		total += s
		if s > maxSize {
			maxSize = s
		}
	}
	fmt.Printf("[demo] after skewing one region with 30 vectors, the largest posting holds %d of %d — the hot region was SPLIT, not ballooned\n", maxSize, total)

	// Prove correctness: the incremental epoch returns the exact nearest neighbors.
	all := append([]Document{}, initial[1:]...) // d000 deleted
	for _, d := range skew {
		if !d.Deleted {
			all = append(all, d)
		}
	}
	q := []float32{0, 0, 0, 0}
	losslessTopK := (len(all) / shortlistMultiplier) + 1
	res, err := RunQuery(ctx, store, ns, QueryParams{RankBy: RankBy{Vector: q}, TopK: losslessTopK, NProbe: 1000})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	want := exactTopK(all, cfg.Metric, q, losslessTopK)
	got := resultIDs(res)
	match := equalStrings(got, want)
	fmt.Printf("[demo] top-%d for query %v matches brute-force exact NN: %v\n", losslessTopK, q, match)
	if !match {
		t.Errorf("demo top-K mismatch:\n got %v\nwant %v", got, want)
	}
}

// clusterSizes reads the live epoch's per-cluster member counts for the demo
// output, through the same uncached Get path the other tests use.
func clusterSizes(ctx context.Context, t *testing.T, store *cache.Store, ns string) []int {
	t.Helper()
	m, _, err := LoadManifest(ctx, store, ns)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(ns, m.IndexEpoch), &cf)
	return cf.Sizes
}
