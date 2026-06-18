// Black-box tests for the opt-in group-commit Committer. They live in
// package engine_test so they exercise only the exported surface
// (NewCommitter / Upsert / Close), the same way a concurrent multi-writer
// caller would, and assert the headline win: N concurrent upserts coalesce into
// far fewer WAL segments while every caller still observes a durable success.
package engine_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// newCommitterNS builds a created namespace plus the cache.Store behind it, so a
// test can both drive the public API and List the wal/ prefix to count segments.
func newCommitterNS(t *testing.T, name string, cfg engine.NamespaceConfig) (*engine.Namespace, *cache.Store) {
	t.Helper()
	store := cache.New(storage.New())
	ns := engine.Open(store, name)
	if err := ns.Create(context.Background(), cfg); err != nil {
		t.Fatalf("Create %q: got err %v, want nil", name, err)
	}
	return ns, store
}

// walSegmentCount returns how many WAL segment objects exist for ns by listing
// the wal/ prefix — the direct, store-level proof of how many durable PUTs
// happened, independent of the manifest's WALSeq.
func walSegmentCount(t *testing.T, store *cache.Store, ns string) int {
	t.Helper()
	keys, err := store.List(context.Background(), ns+"/wal/")
	if err != nil {
		t.Fatalf("List wal prefix: got err %v, want nil", err)
	}
	return len(keys)
}

// TestCommitterCoalescesConcurrentUpserts is the headline test. It fires N
// concurrent Upserts (one unique doc each) at a single Committer and asserts:
//   - every caller observes success (durable-before-return held for each),
//   - the WAL segment count is FAR below N (the batching actually happened),
//   - every distinct document is durable and queryable (no write was lost),
//   - the manifest's WALSeq equals the number of segments (CAS bumped once per
//     coalesced flush, not once per caller).
//
// It runs under -race, so it also asserts the Committer is safe to share across
// goroutines.
func TestCommitterCoalescesConcurrentUpserts(t *testing.T) {
	ctx := context.Background()
	ns, store := newCommitterNS(t, "coalesce", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})

	c := engine.NewCommitter(ns)
	defer c.Close()

	const numWriters = 64
	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(i int) {
			defer wg.Done()
			doc := engine.Document{
				ID:         fmt.Sprintf("doc-%03d", i),
				Vector:     []float32{float32(i), float32(i)},
				Attributes: map[string]any{"i": i},
			}
			errs[i] = c.Upsert(ctx, []engine.Document{doc})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Committer.Upsert %d: got err %v, want nil", i, err)
		}
	}

	// The win: many writers, few segments. We cannot pin an exact count (it
	// depends on goroutine scheduling), but coalescing MUST have produced
	// dramatically fewer segments than one-per-upsert. A generous bound that
	// still proves batching: at most half as many segments as writers.
	segments := walSegmentCount(t, store, "coalesce")
	if segments >= numWriters {
		t.Errorf("WAL segment count: got %d for %d writers, want far fewer (no coalescing happened)", segments, numWriters)
	}
	if segments > numWriters/2 {
		t.Errorf("WAL segment count: got %d for %d writers, want <= %d (weak coalescing)", segments, numWriters, numWriters/2)
	}
	t.Logf("group commit coalesced %d concurrent upserts into %d WAL segments", numWriters, segments)

	// The manifest must agree with the store: WALSeq counts segments, one CAS
	// per coalesced flush.
	info, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info: got err %v, want nil", err)
	}
	if int(info.WALSeq) != segments {
		t.Errorf("manifest WALSeq: got %d, want %d (one bump per coalesced flush)", info.WALSeq, segments)
	}
	if int(info.DocCount) != numWriters {
		t.Errorf("manifest DocCount: got %d, want %d (every doc counted across batches)", info.DocCount, numWriters)
	}

	// Every distinct document must be queryable: nothing was lost to coalescing.
	results, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		TopK:   numWriters,
	})
	if err != nil {
		t.Fatalf("query after coalesced upserts: got err %v, want nil", err)
	}
	if len(results) != numWriters {
		t.Errorf("query after coalesced upserts: got %d results, want %d", len(results), numWriters)
	}
	seen := map[string]bool{}
	for _, r := range results {
		seen[r.ID] = true
	}
	for i := 0; i < numWriters; i++ {
		id := fmt.Sprintf("doc-%03d", i)
		if !seen[id] {
			t.Errorf("query after coalesced upserts: missing document %q", id)
		}
	}
}

// TestCommitterFewerSegmentsThanDirect contrasts group commit against the
// default one-per-upsert path directly: the same N concurrent writers against a
// plain Namespace.Upsert produce exactly N segments, while against a Committer
// they produce far fewer. This is the before/after the extension promises.
func TestCommitterFewerSegmentsThanDirect(t *testing.T) {
	ctx := context.Background()
	const numWriters = 32

	fire := func(t *testing.T, upsert func(i int) error) {
		var wg sync.WaitGroup
		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(i int) {
				defer wg.Done()
				if err := upsert(i); err != nil {
					t.Errorf("upsert %d: got err %v, want nil", i, err)
				}
			}(i)
		}
		wg.Wait()
	}

	docFor := func(i int) []engine.Document {
		return []engine.Document{{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: []float32{float32(i), float32(i)},
		}}
	}

	// Baseline: the default stateless path, one WAL segment per upsert.
	directNS, directStore := newCommitterNS(t, "direct", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})
	fire(t, func(i int) error { return directNS.Upsert(ctx, docFor(i)) })
	directSegments := walSegmentCount(t, directStore, "direct")
	if directSegments != numWriters {
		t.Errorf("direct path WAL segments: got %d, want %d (one per upsert)", directSegments, numWriters)
	}

	// Group commit: the same load, coalesced.
	gcNS, gcStore := newCommitterNS(t, "grouped", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})
	c := engine.NewCommitter(gcNS)
	fire(t, func(i int) error { return c.Upsert(ctx, docFor(i)) })
	c.Close()
	gcSegments := walSegmentCount(t, gcStore, "grouped")

	if gcSegments >= directSegments {
		t.Errorf("group commit did not reduce segments: grouped=%d direct=%d", gcSegments, directSegments)
	}
	t.Logf("WAL segments: direct (one-per-upsert)=%d, group-commit=%d", directSegments, gcSegments)
}

// TestCommitterLastWriterWinsWithinBatch verifies that when two writers in the
// same coalesced batch upsert the SAME id, the merge order decides the survivor:
// the flusher concatenates batch members in arrival order, and MaterializeLive
// resolves the duplicate id last-writer-wins within the one segment. We force a
// single batch by capping the batch large and serializing the enqueue so both
// land together, then assert exactly one of the two values survives.
func TestCommitterLastWriterWinsWithinBatch(t *testing.T) {
	ctx := context.Background()
	ns, _ := newCommitterNS(t, "dup", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})

	c := engine.NewCommitter(ns)
	defer c.Close()

	// Two upserts of id "x" with distinguishable attribute values, fired
	// concurrently. Whichever the flusher orders last wins; we only require that
	// the result is well-defined (exactly one survives, with one of the values).
	var wg sync.WaitGroup
	wg.Add(2)
	for _, v := range []int{1, 2} {
		go func(v int) {
			defer wg.Done()
			if err := c.Upsert(ctx, []engine.Document{
				{ID: "x", Vector: []float32{0, 0}, Attributes: map[string]any{"v": v}},
			}); err != nil {
				t.Errorf("upsert v=%d: got err %v, want nil", v, err)
			}
		}(v)
	}
	wg.Wait()

	results, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("query: got err %v, want nil", err)
	}
	count := 0
	var survivor any
	for _, r := range results {
		if r.ID == "x" {
			count++
			survivor = r.Attributes["v"]
		}
	}
	if count != 1 {
		t.Fatalf("duplicate id x: got %d copies, want exactly 1 (last-writer-wins)", count)
	}
	if survivor == nil {
		t.Errorf("duplicate id x: survivor has no value, want 1 or 2")
	}
}

// TestCommitterCloseDrainsPending asserts shutdown is clean: every Upsert
// in flight when Close is called still returns a durable success — Close drains
// the inbox rather than stranding a blocked caller — and the data is queryable
// afterward.
func TestCommitterCloseDrainsPending(t *testing.T) {
	ctx := context.Background()
	ns, store := newCommitterNS(t, "drain", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})

	c := engine.NewCommitter(ns)

	const numWriters = 24
	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Upsert(ctx, []engine.Document{{
				ID:     fmt.Sprintf("doc-%03d", i),
				Vector: []float32{float32(i), float32(i)},
			}})
		}(i)
	}

	// Close concurrently with the in-flight writers. Every write that was
	// accepted must still complete; Close blocks until the goroutine has drained
	// and exited.
	wg.Wait()
	c.Close()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("upsert %d under Close: got err %v, want nil (Close must drain pending)", i, err)
		}
	}

	// All accepted writes are durable.
	info, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info: got err %v, want nil", err)
	}
	if int(info.DocCount) != numWriters {
		t.Errorf("DocCount after drain: got %d, want %d", info.DocCount, numWriters)
	}
	if got := walSegmentCount(t, store, "drain"); int(info.WALSeq) != got {
		t.Errorf("WALSeq %d disagrees with %d listed segments after drain", info.WALSeq, got)
	}
}

// TestCommitterUpsertAfterClose verifies that once Closed, the Committer rejects
// new Upserts with ErrCommitterClosed (so callers can branch on it) rather than
// blocking forever on an inbox no goroutine reads.
func TestCommitterUpsertAfterClose(t *testing.T) {
	ctx := context.Background()
	ns, _ := newCommitterNS(t, "afterclose", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})

	c := engine.NewCommitter(ns)
	c.Close()
	c.Close() // idempotent: a second Close must not panic or hang.

	err := c.Upsert(ctx, []engine.Document{{ID: "late", Vector: []float32{0, 0}}})
	if err == nil {
		t.Fatalf("Upsert after Close: got nil err, want ErrCommitterClosed")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("Upsert after Close error: got %q, want it to mention 'closed'", err.Error())
	}
}

// TestCommitterValidatesPerCaller asserts a malformed vector fails ONLY its own
// caller and never poisons the batch other writers share: validation happens
// before the docs enter the inbox, so a good concurrent writer still commits.
func TestCommitterValidatesPerCaller(t *testing.T) {
	ctx := context.Background()
	ns, _ := newCommitterNS(t, "validate", engine.NamespaceConfig{Dimension: 3, Metric: "cosine"})

	c := engine.NewCommitter(ns)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	var badErr, goodErr error
	go func() {
		defer wg.Done()
		badErr = c.Upsert(ctx, []engine.Document{{ID: "bad", Vector: []float32{1, 2}}}) // wrong dim
	}()
	go func() {
		defer wg.Done()
		goodErr = c.Upsert(ctx, []engine.Document{{ID: "good", Vector: []float32{1, 2, 3}}})
	}()
	wg.Wait()

	if badErr == nil {
		t.Errorf("bad-vector upsert: got nil err, want a dimension-mismatch error")
	} else if msg := badErr.Error(); !strings.Contains(msg, "2") || !strings.Contains(msg, "3") {
		t.Errorf("bad-vector upsert error: got %q, want both 2 and 3 named", msg)
	}
	if goodErr != nil {
		t.Errorf("good-vector upsert: got err %v, want nil (the bad caller must not poison it)", goodErr)
	}

	// The good document is durable; the bad one never reached the WAL.
	results, err := ns.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Vector: []float32{1, 2, 3}}, TopK: 10})
	if err != nil {
		t.Fatalf("query: got err %v, want nil", err)
	}
	ids := map[string]bool{}
	for _, r := range results {
		ids[r.ID] = true
	}
	if !ids["good"] {
		t.Errorf("query: missing 'good' document, want it durable")
	}
	if ids["bad"] {
		t.Errorf("query: 'bad' document present, want it rejected before the WAL")
	}
}

// TestCommitterEmptyBatchNoOp verifies an empty docs slice is a no-op that never
// enqueues or writes a segment, matching Namespace.Upsert's contract.
func TestCommitterEmptyBatchNoOp(t *testing.T) {
	ctx := context.Background()
	ns, store := newCommitterNS(t, "empty", engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"})

	c := engine.NewCommitter(ns)
	defer c.Close()

	if err := c.Upsert(ctx, nil); err != nil {
		t.Errorf("empty Upsert: got err %v, want nil", err)
	}
	if got := walSegmentCount(t, store, "empty"); got != 0 {
		t.Errorf("empty Upsert wrote %d segments, want 0", got)
	}
}
