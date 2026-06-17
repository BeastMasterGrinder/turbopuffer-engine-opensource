// Black-box tests for the public Namespace API. By living in package
// engine_test these exercise only the exported surface (Open/Create/Upsert/
// Index/Query/Info), the same way a CLI or downstream caller would — if the
// public lifecycle compiles and passes here, the unexported helpers are wired
// correctly.
package engine_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// newNS builds a namespace handle over a fresh MemStore-backed cache. Every
// test gets its own store, so cases never share state.
func newNS(t *testing.T, name string) *engine.Namespace {
	t.Helper()
	store := cache.New(storage.New())
	return engine.Open(store, name)
}

// resultIDs returns the ids of a result list in their returned (ranked) order.
func resultIDs(results []engine.QueryResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	return ids
}

// TestNamespaceLifecycle walks the full headline flow over a single namespace:
// Create, Upsert, Query before any index (tail-only), Index (epoch swap), Query
// after the index plus a fresh tail write, and Info. Each stage asserts on the
// public results and the manifest fields a caller can observe, so the test
// pins the contract the CLI depends on.
func TestNamespaceLifecycle(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t, "demo")

	cfg := engine.NamespaceConfig{Dimension: 2, Metric: "euclidean", TextField: "body"}
	if err := ns.Create(ctx, cfg); err != nil {
		t.Fatalf("Create: got err %v, want nil", err)
	}

	// A fresh namespace has an empty, unindexed manifest.
	info, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info after create: got err %v, want nil", err)
	}
	if info.Dimension != 2 || info.Metric != "euclidean" || info.TextField != "body" {
		t.Errorf("Info after create config: got dim=%d metric=%q text=%q, want 2/euclidean/body", info.Dimension, info.Metric, info.TextField)
	}
	if info.WALSeq != 0 || info.IndexEpoch != 0 || info.DocCount != 0 {
		t.Errorf("Info after create: got WALSeq=%d IndexEpoch=%d DocCount=%d, want 0/0/0", info.WALSeq, info.IndexEpoch, info.DocCount)
	}

	// Upsert three documents that carry both a vector and the BM25 text field.
	docs := []engine.Document{
		{ID: "a", Vector: []float32{0, 0}, Attributes: map[string]any{"body": "the quick brown fox", "n": 1}},
		{ID: "b", Vector: []float32{1, 1}, Attributes: map[string]any{"body": "lazy brown dog", "n": 2}},
		{ID: "c", Vector: []float32{5, 5}, Attributes: map[string]any{"body": "quick red fox", "n": 1}},
	}
	if err := ns.Upsert(ctx, docs); err != nil {
		t.Fatalf("Upsert: got err %v, want nil", err)
	}

	// Durable-before-return: the manifest now advertises one WAL segment, still
	// unindexed.
	info, err = ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info after upsert: got err %v, want nil", err)
	}
	if info.WALSeq != 1 {
		t.Errorf("Info after upsert WALSeq: got %d, want 1", info.WALSeq)
	}
	if info.IndexEpoch != 0 {
		t.Errorf("Info after upsert IndexEpoch: got %d, want 0 (not indexed yet)", info.IndexEpoch)
	}
	if info.DocCount != 3 {
		t.Errorf("Info after upsert DocCount: got %d, want 3", info.DocCount)
	}

	// Query BEFORE indexing: results come purely from the WAL tail (rule 5,
	// query-before-index headline).
	vecBefore, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		TopK:   3,
	})
	if err != nil {
		t.Fatalf("vector query before index: got err %v, want nil", err)
	}
	if got, want := resultIDs(vecBefore), []string{"a", "b", "c"}; !equalStrings(got, want) {
		t.Errorf("vector query before index order: got %v, want %v", got, want)
	}
	if vecBefore[0].Dist != 0 {
		t.Errorf("vector query before index nearest $dist: got %v, want 0", vecBefore[0].Dist)
	}

	textBefore, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Text: "fox"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("text query before index: got err %v, want nil", err)
	}
	// "fox" appears in a and c only; both must be returned, b must not.
	gotText := map[string]bool{}
	for _, r := range textBefore {
		gotText[r.ID] = true
		if r.Score <= 0 {
			t.Errorf("text query before index: doc %q has non-positive score %v", r.ID, r.Score)
		}
	}
	if !gotText["a"] || !gotText["c"] || gotText["b"] {
		t.Errorf("text query before index hits: got %v, want {a,c}", gotText)
	}

	// Build the index — single atomic epoch swap.
	if err := ns.Index(ctx); err != nil {
		t.Fatalf("Index: got err %v, want nil", err)
	}
	info, err = ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info after index: got err %v, want nil", err)
	}
	if info.IndexEpoch != 1 {
		t.Errorf("Info after index IndexEpoch: got %d, want 1", info.IndexEpoch)
	}
	if info.IndexedUpTo != 1 {
		t.Errorf("Info after index IndexedUpTo: got %d, want 1 (snapshot of WALSeq at build start)", info.IndexedUpTo)
	}
	if info.DocCount != 3 {
		t.Errorf("Info after index DocCount: got %d, want 3", info.DocCount)
	}

	// Upsert a fresh document AND a tombstone after the index. These fall in the
	// unindexed tail [IndexedUpTo, WALSeq) that a query must overlay.
	if err := ns.Upsert(ctx, []engine.Document{
		{ID: "d", Vector: []float32{0, 0}, Attributes: map[string]any{"body": "another quick fox", "n": 1}},
		{ID: "c", Deleted: true},
	}); err != nil {
		t.Fatalf("Upsert tail: got err %v, want nil", err)
	}

	// Query AFTER index: must merge indexed epoch with the tail. "d" (fresh,
	// distance 0) ties with "a" (indexed, distance 0); "c" was tombstoned in the
	// tail and must be gone.
	vecAfter, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		TopK:   10,
		NProbe: 8,
	})
	if err != nil {
		t.Fatalf("vector query after index: got err %v, want nil", err)
	}
	afterIDs := map[string]bool{}
	for _, r := range vecAfter {
		afterIDs[r.ID] = true
	}
	if !afterIDs["a"] || !afterIDs["b"] || !afterIDs["d"] {
		t.Errorf("vector query after index: got ids %v, want a,b,d present", resultIDs(vecAfter))
	}
	if afterIDs["c"] {
		t.Errorf("vector query after index: tombstoned id c still present in %v", resultIDs(vecAfter))
	}

	// The text query after index must also drop tombstoned c and surface fresh d.
	textAfter, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Text: "fox"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("text query after index: got err %v, want nil", err)
	}
	gotTextAfter := map[string]bool{}
	for _, r := range textAfter {
		gotTextAfter[r.ID] = true
	}
	if gotTextAfter["c"] {
		t.Errorf("text query after index: tombstoned id c still present in %v", resultIDs(textAfter))
	}
	if !gotTextAfter["a"] || !gotTextAfter["d"] {
		t.Errorf("text query after index: got %v, want a and d present", resultIDs(textAfter))
	}

	// Final manifest snapshot: a second WAL segment landed (the tail upsert),
	// still covered by epoch 1's IndexedUpTo=1.
	info, err = ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info final: got err %v, want nil", err)
	}
	if info.WALSeq != 2 {
		t.Errorf("Info final WALSeq: got %d, want 2", info.WALSeq)
	}
	if info.IndexedUpTo != 1 {
		t.Errorf("Info final IndexedUpTo: got %d, want 1", info.IndexedUpTo)
	}
}

// TestNamespaceFilterQuery checks a query with an attribute filter routes
// through the public API and returns only matching documents, with numeric eq
// coerced (JSON-style float64 vs Go int literal) as the canon requires.
func TestNamespaceFilterQuery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns := newNS(t, "filtered")
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: got err %v, want nil", err)
	}
	if err := ns.Upsert(ctx, []engine.Document{
		{ID: "a", Vector: []float32{0, 0}, Attributes: map[string]any{"cat": 1}},
		{ID: "b", Vector: []float32{0, 0}, Attributes: map[string]any{"cat": 2}},
		{ID: "c", Vector: []float32{0, 0}, Attributes: map[string]any{"cat": 1}},
	}); err != nil {
		t.Fatalf("Upsert: got err %v, want nil", err)
	}

	got, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		Filter: engine.Filter{Op: "eq", Field: "cat", Value: 1},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("filtered query: got err %v, want nil", err)
	}
	gotIDs := map[string]bool{}
	for _, r := range got {
		gotIDs[r.ID] = true
	}
	if len(gotIDs) != 2 || !gotIDs["a"] || !gotIDs["c"] {
		t.Errorf("filtered query: got %v, want exactly {a,c}", resultIDs(got))
	}
}

// TestNamespaceUpsertDimensionMismatch verifies a present-but-wrong-length
// vector is rejected before it reaches the WAL, with both dimensions named in
// the error, while a nil-vector (text-only) document is accepted.
func TestNamespaceUpsertDimensionMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns := newNS(t, "dims")
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: 3, Metric: "cosine", TextField: "body"}); err != nil {
		t.Fatalf("Create: got err %v, want nil", err)
	}

	err := ns.Upsert(ctx, []engine.Document{
		{ID: "bad", Vector: []float32{1, 2}},
	})
	if err == nil {
		t.Fatalf("Upsert dimension mismatch: got nil err, want a mismatch error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "2") || !strings.Contains(msg, "3") {
		t.Errorf("Upsert dimension mismatch error: got %q, want both 2 and 3 in the message", msg)
	}

	// A rejected upsert must not advance the WAL.
	info, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info after rejected upsert: got err %v, want nil", err)
	}
	if info.WALSeq != 0 {
		t.Errorf("Info after rejected upsert WALSeq: got %d, want 0 (nothing written)", info.WALSeq)
	}

	// A text-only document (nil vector) is valid in a vector namespace.
	if err := ns.Upsert(ctx, []engine.Document{
		{ID: "textonly", Attributes: map[string]any{"body": "hello world"}},
	}); err != nil {
		t.Fatalf("Upsert text-only doc: got err %v, want nil", err)
	}
}

// TestNamespaceQueryDimensionMismatch verifies a query vector of the wrong
// dimension is rejected with both numbers in the error.
func TestNamespaceQueryDimensionMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns := newNS(t, "qdims")
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: 4, Metric: "cosine"}); err != nil {
		t.Fatalf("Create: got err %v, want nil", err)
	}

	_, err := ns.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Vector: []float32{1, 2}}})
	if err == nil {
		t.Fatalf("query dimension mismatch: got nil err, want a mismatch error")
	}
	if msg := err.Error(); !strings.Contains(msg, "2") || !strings.Contains(msg, "4") {
		t.Errorf("query dimension mismatch error: got %q, want both 2 and 4 in the message", msg)
	}
}

// TestNamespaceCreateAlreadyExists verifies a second Create on the same
// namespace fails (write-once manifest), proving Open is purely a handle and
// Create is the materializing call.
func TestNamespaceCreateAlreadyExists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := cache.New(storage.New())
	ns := engine.Open(store, "dup")

	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("first Create: got err %v, want nil", err)
	}
	// A second handle to the same name on the same store must not be able to
	// re-create it.
	if err := engine.Open(store, "dup").Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err == nil {
		t.Fatalf("second Create: got nil err, want already-exists error")
	}
}

// TestNamespaceInfoNotFound verifies Info on an unopened/uncreated namespace
// surfaces storage.ErrNotFound so callers can branch on errors.Is.
func TestNamespaceInfoNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ns := newNS(t, "missing")
	if _, err := ns.Info(ctx); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("Info on missing namespace: got err %v, want errors.Is storage.ErrNotFound", err)
	}
}

// TestNamespaceConcurrentUpsert hammers a single namespace with many concurrent
// upserts, each writing one unique document. It exercises both the WAL
// PutIfAbsent race (correctness rule 1: the 412 loser reloads and rewrites at a
// fresh seq) and the manifest CAS retry. With -race this also asserts the
// Namespace handle and store are safe to share across goroutines.
//
// Post-conditions that can only hold if both retry paths are correct:
//   - exactly numWriters WAL segments exist (no two upserts clobbered a seq),
//   - every unique document is queryable (no write was lost),
//   - the manifest's DocCount equals numWriters (every CAS landed its delta).
func TestNamespaceConcurrentUpsert(t *testing.T) {
	ctx := context.Background()
	ns := newNS(t, "concurrent")
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: 2, Metric: "euclidean"}); err != nil {
		t.Fatalf("Create: got err %v, want nil", err)
	}

	const numWriters = 16
	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(i int) {
			defer wg.Done()
			doc := engine.Document{
				ID:         idFor(i),
				Vector:     []float32{float32(i), float32(i)},
				Attributes: map[string]any{"i": i},
			}
			errs[i] = ns.Upsert(ctx, []engine.Document{doc})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Upsert %d: got err %v, want nil", i, err)
		}
	}

	info, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("Info after concurrent upserts: got err %v, want nil", err)
	}
	if info.WALSeq != numWriters {
		t.Errorf("WALSeq after %d concurrent upserts: got %d, want %d (every PutIfAbsent claimed a distinct seq)", numWriters, info.WALSeq, numWriters)
	}
	if info.DocCount != numWriters {
		t.Errorf("DocCount after %d concurrent upserts: got %d, want %d (every manifest CAS landed its delta)", numWriters, info.DocCount, numWriters)
	}

	// Every distinct document must be queryable: nothing was lost to a clobbered
	// segment. A wide vector query that returns all of them confirms it.
	results, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: []float32{0, 0}},
		TopK:   numWriters,
	})
	if err != nil {
		t.Fatalf("query after concurrent upserts: got err %v, want nil", err)
	}
	if len(results) != numWriters {
		t.Errorf("query after concurrent upserts: got %d results, want %d", len(results), numWriters)
	}
	seen := map[string]bool{}
	for _, r := range results {
		seen[r.ID] = true
	}
	for i := 0; i < numWriters; i++ {
		if !seen[idFor(i)] {
			t.Errorf("query after concurrent upserts: missing document %q", idFor(i))
		}
	}
}

// idFor returns a stable document id for writer i.
func idFor(i int) string {
	return "doc-" + string(rune('a'+i))
}

// equalStrings reports whether two string slices are element-wise equal.
func equalStrings(a, b []string) bool {
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
