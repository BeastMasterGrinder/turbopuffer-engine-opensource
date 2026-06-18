package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// resultIDs returns the ids of a result list in their returned (ranked) order.
func resultIDs(results []QueryResult) []string {
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}
	return ids
}

// setupNS creates the namespace, seeds the given WAL segments, and returns the
// store ready to query. It does NOT build an index, so a query runs against the
// raw tail unless the test calls BuildIndex itself.
func setupNS(ctx context.Context, t *testing.T, cfg NamespaceConfig, segments [][]Document) *cache.Store {
	t.Helper()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, cfg); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}
	if len(segments) > 0 {
		seedWAL(ctx, t, store, testNS, segments)
	}
	return store
}

// seedTail appends WAL segments after whatever has already been written —
// starting at the current WALSeq rather than 0 — and advances the manifest to
// cover them. It mirrors an Upsert that lands after an index build, so the new
// segments fall into the unindexed tail [IndexedUpTo, WALSeq) that a query must
// overlay (correctness rule 5).
func seedTail(ctx context.Context, t *testing.T, store *cache.Store, segments [][]Document) {
	t.Helper()
	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest before tail seed: got err %v, want nil", err)
	}
	seq := m.WALSeq
	for _, ops := range segments {
		if err := AppendWAL(ctx, store, testNS, seq, ops); err != nil {
			t.Fatalf("AppendWAL(seq=%d): got err %v, want nil", seq, err)
		}
		seq++
	}
	if _, err := SaveManifestCAS(ctx, store, testNS, func(m *Manifest) {
		m.WALSeq = seq
	}); err != nil {
		t.Fatalf("advancing WALSeq to %d: got err %v, want nil", seq, err)
	}
}

func TestRunQueryVectorBeforeIndexServesTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// No index is ever built: IndexEpoch stays 0, so the query must serve purely
	// from the WAL tail [0, WALSeq). This is the query-before-index headline.
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "alpha"),
			vecDoc("b", []float32{0, 1, 0, 0}, "beta"),
		},
		{
			vecDoc("c", []float32{0, 0, 1, 0}, "gamma"),
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{0.9, 0.1, 0, 0}},
		TopK:   3,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}

	// Closest direction to [0.9,0.1,0,0] is a, then b, then c (orthogonal).
	wantOrder := []string{"a", "b", "c"}
	if ids := resultIDs(got); !equalStrings(ids, wantOrder) {
		t.Errorf("vector tail order: got %v, want %v", ids, wantOrder)
	}
	if got[0].Dist >= got[1].Dist {
		t.Errorf("distances not ascending: got %v, want %v < %v", resultIDs(got), got[0].Dist, got[1].Dist)
	}
}

func TestRunQueryVectorIndexedTailNewerWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "alpha"),
			vecDoc("b", []float32{0, 1, 0, 0}, "beta"),
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// After indexing, re-upsert a with a vector that now points away from the
	// query. The tail copy must overwrite the indexed copy (rule 5).
	seedTail(ctx, t, store, [][]Document{
		{vecDoc("a", []float32{0, 0, 0, 1}, "alpha moved")},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
		TopK:   2,
		NProbe: 3,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}

	// b ([0,1,0,0]) is now closer to [1,0,0,0] than the moved a ([0,0,0,1]):
	// both are orthogonal (cosine dist 1.0), so the tie breaks on id and a sorts
	// first — but the key assertion is that a's distance reflects the NEW vector,
	// not the indexed one (which would have been distance 0).
	byID := map[string]QueryResult{}
	for _, r := range got {
		byID[r.ID] = r
	}
	if byID["a"].Dist == 0 {
		t.Errorf("a still has its stale indexed distance 0; tail overwrite failed, got %+v", byID["a"])
	}
	if got, want := byID["a"].Attributes["body"], "alpha moved"; got != want {
		t.Errorf("a attributes: got %v, want %q (the tail version)", got, want)
	}
}

func TestRunQueryVectorTombstoneSubtracted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "alpha"),
			vecDoc("b", []float32{0, 1, 0, 0}, "beta"),
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// Delete a in the tail: even though the indexed epoch still holds it, the
	// tombstone must shadow it.
	seedTail(ctx, t, store, [][]Document{
		{tombstone("a")},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if ids := resultIDs(got); !equalStrings(ids, []string{"b"}) {
		t.Errorf("after tombstoning a: got %v, want [b]", ids)
	}
}

func TestRunQueryVectorFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "en1", Vector: []float32{1, 0, 0, 0}, Attributes: map[string]any{"lang": "en"}},
			{ID: "fr1", Vector: []float32{1, 0, 0, 0}, Attributes: map[string]any{"lang": "fr"}},
			{ID: "en2", Vector: []float32{0, 1, 0, 0}, Attributes: map[string]any{"lang": "en"}},
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
		Filter: Filter{Op: "eq", Field: "lang", Value: "en"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	// Only en* docs survive the filter, nearest (en1) first.
	if ids := resultIDs(got); !equalStrings(ids, []string{"en1", "en2"}) {
		t.Errorf("filtered vector results: got %v, want [en1 en2]", ids)
	}
}

func TestRunQueryVectorTopKTruncation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "a"),
			vecDoc("b", []float32{0.9, 0.1, 0, 0}, "b"),
			vecDoc("c", []float32{0.8, 0.2, 0, 0}, "c"),
			vecDoc("d", []float32{0, 1, 0, 0}, "d"),
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("TopK truncation: got %d results, want 2", len(got))
	}
	if ids := resultIDs(got); !equalStrings(ids, []string{"a", "b"}) {
		t.Errorf("top-2 nearest: got %v, want [a b]", ids)
	}
}

func TestRunQueryVectorIndexedAndTailMerge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "alpha"),
			vecDoc("b", []float32{0, 1, 0, 0}, "beta"),
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// A brand-new id only present in the tail must be searchable alongside the
	// indexed docs.
	seedTail(ctx, t, store, [][]Document{
		{vecDoc("c", []float32{0.95, 0.05, 0, 0}, "gamma")},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	// a (exact) then c (near) then b (orthogonal).
	if ids := resultIDs(got); !equalStrings(ids, []string{"a", "c", "b"}) {
		t.Errorf("merged vector results: got %v, want [a c b]", ids)
	}
}

func TestRunQueryBM25BeforeIndexServesTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "a", Attributes: map[string]any{"body": "the quick brown fox"}},
			{ID: "b", Attributes: map[string]any{"body": "lazy dog sleeps"}},
			{ID: "c", Attributes: map[string]any{"body": "quick fox quick fox"}},
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: "quick fox"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	// c mentions both terms twice, a once each, b not at all.
	if ids := resultIDs(got); !equalStrings(ids, []string{"c", "a"}) {
		t.Errorf("bm25 tail results: got %v, want [c a]", ids)
	}
	if got[0].Score <= got[1].Score {
		t.Errorf("bm25 scores not descending: got %v then %v", got[0].Score, got[1].Score)
	}
}

func TestRunQueryBM25IndexedTailNewerWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "a", Attributes: map[string]any{"body": "quick fox"}},
			{ID: "b", Attributes: map[string]any{"body": "lazy dog"}},
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// Re-upsert a so it no longer mentions the query term: the tail version must
	// overwrite the indexed score and drop a out of the results entirely.
	seedTail(ctx, t, store, [][]Document{
		{{ID: "a", Attributes: map[string]any{"body": "now about cats"}}},
		{{ID: "c", Attributes: map[string]any{"body": "quick quick fox"}}},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: "quick fox"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	// c (tail, matches strongly) is the only hit; a was overwritten to no longer
	// match; b never matched.
	if ids := resultIDs(got); !equalStrings(ids, []string{"c"}) {
		t.Errorf("bm25 newer-wins results: got %v, want [c]", ids)
	}
}

func TestRunQueryBM25TombstoneSubtracted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "a", Attributes: map[string]any{"body": "quick fox"}},
			{ID: "b", Attributes: map[string]any{"body": "quick dog"}},
		},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}
	seedTail(ctx, t, store, [][]Document{
		{tombstone("a")},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: "quick"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if ids := resultIDs(got); !equalStrings(ids, []string{"b"}) {
		t.Errorf("after tombstoning a: got %v, want [b]", ids)
	}
}

func TestRunQueryBM25Filter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "en", Attributes: map[string]any{"body": "quick fox", "lang": "en"}},
			{ID: "fr", Attributes: map[string]any{"body": "quick fox", "lang": "fr"}},
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: "quick fox"},
		Filter: Filter{Op: "eq", Field: "lang", Value: "en"},
		TopK:   10,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if ids := resultIDs(got); !equalStrings(ids, []string{"en"}) {
		t.Errorf("filtered bm25 results: got %v, want [en]", ids)
	}
}

func TestRunQueryBM25TopKTruncation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), [][]Document{
		{
			{ID: "a", Attributes: map[string]any{"body": "quick quick quick fox"}},
			{ID: "b", Attributes: map[string]any{"body": "quick quick fox"}},
			{ID: "c", Attributes: map[string]any{"body": "quick fox"}},
		},
	})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: "quick"},
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if len(got) != 2 {
		t.Fatalf("TopK truncation: got %d results, want 2", len(got))
	}
	// Highest term frequency first.
	if ids := resultIDs(got); !equalStrings(ids, []string{"a", "b"}) {
		t.Errorf("top-2 bm25: got %v, want [a b]", ids)
	}
}

func TestRunQueryErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name    string
		cfg     NamespaceConfig
		params  QueryParams
		wantSub string
	}{
		{
			name:    "neither rank mode",
			cfg:     testConfig(),
			params:  QueryParams{TopK: 3},
			wantSub: "no rank mode set",
		},
		// Both rank modes set is no longer an error: it is the hybrid path, covered
		// by hybrid_test.go.
		{
			name:    "bm25 with no text field",
			cfg:     NamespaceConfig{Dimension: 4, Metric: "cosine", TextField: ""},
			params:  QueryParams{RankBy: RankBy{Text: "quick"}, TopK: 3},
			wantSub: "no text field",
		},
		{
			name:    "vector dimension mismatch",
			cfg:     testConfig(),
			params:  QueryParams{RankBy: RankBy{Vector: []float32{1, 0}}, TopK: 3},
			wantSub: "dimension 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := setupNS(ctx, t, tt.cfg, nil)
			_, err := RunQuery(ctx, store, testNS, tt.params)
			if err == nil {
				t.Fatalf("RunQuery(%s): got nil err, want error containing %q", tt.name, tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("RunQuery(%s) error = %q, want it to contain %q", tt.name, err.Error(), tt.wantSub)
			}
		})
	}
}

func TestRunQueryDimensionMismatchReportsBothNumbers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := setupNS(ctx, t, testConfig(), nil) // testConfig dimension is 4

	_, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 2, 3}},
		TopK:   3,
	})
	if err == nil {
		t.Fatalf("RunQuery: got nil err, want a dimension-mismatch error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "3") || !strings.Contains(msg, "4") {
		t.Errorf("dimension-mismatch error %q must mention both 3 (query) and 4 (namespace)", msg)
	}
}

func TestRunQueryDefaultTopK(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	// 15 docs, TopK unset (0) must default to defaultTopK (10).
	var ops []Document
	for i := 0; i < 15; i++ {
		id := string(rune('a' + i))
		ops = append(ops, vecDoc(id, []float32{float32(i + 1), 0, 0, 0}, "body "+id))
	}
	store := setupNS(ctx, t, testConfig(), [][]Document{ops})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}},
	})
	if err != nil {
		t.Fatalf("RunQuery: got err %v, want nil", err)
	}
	if len(got) != defaultTopK {
		t.Errorf("default TopK: got %d results, want %d", len(got), defaultTopK)
	}
}
