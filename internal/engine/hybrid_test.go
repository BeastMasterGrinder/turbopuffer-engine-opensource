package engine

import (
	"context"
	"math"
	"testing"
)

// almostEqual reports whether two RRF scores are within a small epsilon, since
// the fused values are sums of reciprocals and exact float equality is brittle.
func almostEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// rrfScoreOf returns the fused $rrf of the result with the given id, or 0 if it
// is absent from the fused list.
func rrfScoreOf(results []QueryResult, id string) float64 {
	for _, r := range results {
		if r.ID == id {
			return r.RRF
		}
	}
	return 0
}

// TestFuseRRFWorkedExample reproduces the worked example from
// docs/extensions/hybrid-fusion.md exactly: with k=60, document B (vector rank 2,
// BM25 rank 2) wins over A (1,4) despite never ranking #1 in either list, and the
// single-list docs C (BM25 #1 only) and D (vector #3 only) trail. This pins both
// the formula 1/(k+rank) and the "consistent in both beats #1 in one" property.
func TestFuseRRFWorkedExample(t *testing.T) {
	t.Parallel()

	// Vector list, best-first (ascending $dist): A, B, D.
	vecResults := []QueryResult{
		{ID: "A", Dist: 0.10},
		{ID: "B", Dist: 0.20},
		{ID: "D", Dist: 0.30},
	}
	// BM25 list, best-first (descending $score): C, B, A.
	// (C rank 1, B rank 2, A rank 3 — but the doc's table puts A at BM25 rank 4;
	// we reproduce the table's ranks below with an explicit padding doc.)
	textResults := []QueryResult{
		{ID: "C", Score: 9.0},
		{ID: "B", Score: 8.0},
		{ID: "X", Score: 7.0}, // padding so A lands at BM25 rank 4 as in the table
		{ID: "A", Score: 6.0},
	}

	got := fuseRRF(vecResults, textResults, 10)

	// Expected fused scores straight from the doc's table (k=60).
	wantA := 1.0/61 + 1.0/64 // vec rank 1, bm25 rank 4
	wantB := 1.0/62 + 1.0/62 // vec rank 2, bm25 rank 2
	wantC := 1.0 / 61        // bm25 rank 1 only
	wantD := 1.0 / 63        // vec rank 3 only

	if s := rrfScoreOf(got, "A"); !almostEqual(s, wantA) {
		t.Errorf("RRF(A) = %.6f, want %.6f", s, wantA)
	}
	if s := rrfScoreOf(got, "B"); !almostEqual(s, wantB) {
		t.Errorf("RRF(B) = %.6f, want %.6f", s, wantB)
	}
	if s := rrfScoreOf(got, "C"); !almostEqual(s, wantC) {
		t.Errorf("RRF(C) = %.6f, want %.6f", s, wantC)
	}
	if s := rrfScoreOf(got, "D"); !almostEqual(s, wantD) {
		t.Errorf("RRF(D) = %.6f, want %.6f", s, wantD)
	}

	// B must win overall — it is consistently near the top of both lists, which is
	// the behavior hybrid search wants (it never ranked #1 in either list).
	if got[0].ID != "B" {
		t.Errorf("fused winner = %q, want B (rank 2 in both lists beats #1-in-one-only)", got[0].ID)
	}
	// And B's fused score must exceed A's (the #1 vector hit) and C's (#1 BM25).
	if rrfScoreOf(got, "B") <= rrfScoreOf(got, "A") {
		t.Errorf("RRF(B) %.6f must beat RRF(A) %.6f", rrfScoreOf(got, "B"), rrfScoreOf(got, "A"))
	}
	if rrfScoreOf(got, "B") <= rrfScoreOf(got, "C") {
		t.Errorf("RRF(B) %.6f must beat RRF(C) %.6f", rrfScoreOf(got, "B"), rrfScoreOf(got, "C"))
	}
}

// TestFuseRRFKeepsRawDistAndScore checks that the fused result carries both the
// raw $dist and $score from whichever legs the doc appeared in, for transparency,
// while a single-list doc keeps only its own signal.
func TestFuseRRFKeepsRawDistAndScore(t *testing.T) {
	t.Parallel()

	vecResults := []QueryResult{{ID: "both", Dist: 0.42}, {ID: "vec-only", Dist: 0.99}}
	textResults := []QueryResult{{ID: "both", Score: 3.14}, {ID: "text-only", Score: 1.0}}

	got := fuseRRF(vecResults, textResults, 10)

	for _, r := range got {
		switch r.ID {
		case "both":
			if r.Dist != 0.42 || r.Score != 3.14 {
				t.Errorf("both: $dist=%v $score=%v, want 0.42 / 3.14", r.Dist, r.Score)
			}
			if r.RRF <= 0 {
				t.Errorf("both: $rrf=%v, want > 0", r.RRF)
			}
		case "vec-only":
			if r.Dist != 0.99 || r.Score != 0 {
				t.Errorf("vec-only: $dist=%v $score=%v, want 0.99 / 0", r.Dist, r.Score)
			}
		case "text-only":
			if r.Score != 1.0 || r.Dist != 0 {
				t.Errorf("text-only: $score=%v $dist=%v, want 1.0 / 0", r.Score, r.Dist)
			}
		}
	}
}

// TestFuseRRFConsistentBeatsStrongInOne is the core property in isolation: a doc
// at rank 2 in BOTH lists outranks a doc at rank 1 in exactly one list, because
// 1/(k+2)+1/(k+2) > 1/(k+1)+0 for k=60.
func TestFuseRRFConsistentBeatsStrongInOne(t *testing.T) {
	t.Parallel()

	vecResults := []QueryResult{
		{ID: "vstar", Dist: 0.1},    // #1 vector only
		{ID: "balanced", Dist: 0.2}, // #2 vector
	}
	textResults := []QueryResult{
		{ID: "tstar", Score: 9},    // #1 bm25 only
		{ID: "balanced", Score: 8}, // #2 bm25
	}

	got := fuseRRF(vecResults, textResults, 10)
	if got[0].ID != "balanced" {
		t.Errorf("winner = %q, want balanced (rank 2 in both beats rank 1 in one)", got[0].ID)
	}
}

// TestFuseRRFKSensitivity documents how k changes the gap between a both-lists
// doc and a single-list doc. The doc's claim is that the METHOD (which doc wins)
// is insensitive to k, while the absolute margin shrinks as k grows. We verify
// both: the winner is stable across k, and the (both − single) score margin is
// strictly smaller at a larger k.
func TestFuseRRFKSensitivity(t *testing.T) {
	t.Parallel()

	// rank 2 in both vs rank 1 in one only, parameterized over k.
	margin := func(k int) float64 {
		both := 1.0/float64(k+2) + 1.0/float64(k+2)
		single := 1.0 / float64(k+1)
		return both - single
	}

	// The default k=60 keeps "both" ahead (positive margin) — the winner is stable.
	if m := margin(rrfK); m <= 0 {
		t.Fatalf("at k=%d the both-lists doc must still win, margin=%.6f", rrfK, m)
	}
	// A much larger k still keeps the same winner (k-insensitivity of the method)...
	if m := margin(500); m <= 0 {
		t.Fatalf("at k=500 the both-lists doc must still win, margin=%.6f", m)
	}
	// ...but the absolute margin shrinks as k grows: larger k flattens rank gaps.
	if margin(500) >= margin(rrfK) {
		t.Errorf("margin at k=500 (%.6g) should be smaller than at k=60 (%.6g)", margin(500), margin(rrfK))
	}
}

// TestFuseRRFEmptyLeg checks that fusing against an empty list degrades to that
// single list's order (a doc missing from a list simply gets no term for it).
func TestFuseRRFEmptyLeg(t *testing.T) {
	t.Parallel()

	vecResults := []QueryResult{{ID: "a", Dist: 0.1}, {ID: "b", Dist: 0.2}}
	got := fuseRRF(vecResults, nil, 10)
	if ids := resultIDs(got); !equalStrings(ids, []string{"a", "b"}) {
		t.Errorf("fusing with an empty text leg: got %v, want [a b]", ids)
	}
}

// TestFuseRRFTruncatesToTopK checks the fused list is cut to topK after sorting.
func TestFuseRRFTruncatesToTopK(t *testing.T) {
	t.Parallel()

	vecResults := []QueryResult{
		{ID: "a", Dist: 0.1}, {ID: "b", Dist: 0.2}, {ID: "c", Dist: 0.3},
	}
	got := fuseRRF(vecResults, nil, 2)
	if len(got) != 2 {
		t.Fatalf("truncation: got %d results, want 2", len(got))
	}
}

// TestRunQueryHybridEndToEnd drives the whole hybrid path over storage.New():
// it plants three documents per the worked-example shape and confirms the engine
// returns the consistent-in-both document first, even though neither the vector
// query nor the BM25 query alone ranks it #1.
//
// Setup (dimension 4, cosine):
//   - balanced : vector close to the query AND body shares one query term →
//     near-top of both lists, top of neither.
//   - vstar    : vector exactly the query, body shares no query term → #1 vector,
//     absent from BM25.
//   - tstar    : vector far from the query, body is the exact query terms → #1
//     BM25, last in vector.
//
// Vector-only therefore returns vstar first, BM25-only returns tstar first, and
// only the hybrid fusion surfaces balanced. This also exercises rule 5 implicitly:
// no index is built, so both legs serve purely from the WAL tail.
func TestRunQueryHybridEndToEnd(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	queryVec := []float32{1, 0, 0, 0}
	queryText := "alpha beta"

	docs := []Document{
		// balanced: vector slightly off the query axis, body has one query term + filler.
		{ID: "balanced", Vector: []float32{0.9, 0.2, 0, 0}, Attributes: map[string]any{"body": "alpha gamma delta"}},
		// vstar: the literal nearest vector, body shares NO query term.
		{ID: "vstar", Vector: []float32{1, 0, 0, 0}, Attributes: map[string]any{"body": "gamma delta epsilon"}},
		// tstar: far in vector space, body is the exact query (BM25 runaway #1).
		{ID: "tstar", Vector: []float32{0, 1, 0, 0}, Attributes: map[string]any{"body": "alpha beta alpha beta"}},
		// vector-only filler that out-ranks tstar on the vector leg (closer to the
		// query axis than tstar, no query terms), pushing tstar's vector rank down
		// so it cannot tie balanced on combined rank. This mirrors the doc's point
		// that a doc strong in only one list trails a doc consistent in both.
		{ID: "vfill1", Vector: []float32{0.7, 0.4, 0, 0}, Attributes: map[string]any{"body": "gamma delta zeta"}},
		{ID: "vfill2", Vector: []float32{0.5, 0.6, 0, 0}, Attributes: map[string]any{"body": "epsilon zeta eta"}},
	}

	store := setupNS(ctx, t, testConfig(), [][]Document{docs})

	// Vector-only: vstar is the exact match, so it must top this list.
	vec, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: queryVec}, TopK: 5,
	})
	if err != nil {
		t.Fatalf("vector RunQuery: got err %v, want nil", err)
	}
	if vec[0].ID != "vstar" {
		t.Fatalf("vector-only winner = %q, want vstar", vec[0].ID)
	}

	// BM25-only: tstar carries both query terms twice, so it must top this list.
	text, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Text: queryText}, TopK: 5,
	})
	if err != nil {
		t.Fatalf("bm25 RunQuery: got err %v, want nil", err)
	}
	if text[0].ID != "tstar" {
		t.Fatalf("bm25-only winner = %q, want tstar", text[0].ID)
	}

	// Hybrid: balanced is near the top of BOTH, so RRF must surface it first.
	hybrid, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: queryVec, Text: queryText}, TopK: 3,
	})
	if err != nil {
		t.Fatalf("hybrid RunQuery: got err %v, want nil", err)
	}
	if hybrid[0].ID != "balanced" {
		t.Fatalf("hybrid winner = %q, want balanced (consistent in both lists)\nfull: %+v", hybrid[0].ID, hybrid)
	}
	// The fused winner must carry a positive $rrf and the legs it appeared in.
	if hybrid[0].RRF <= 0 {
		t.Errorf("hybrid winner $rrf = %v, want > 0", hybrid[0].RRF)
	}
	if hybrid[0].Dist == 0 || hybrid[0].Score == 0 {
		t.Errorf("hybrid winner should keep both raw $dist (%v) and $score (%v)", hybrid[0].Dist, hybrid[0].Score)
	}
}

// TestRunQueryHybridReadsTail proves both legs still overlay the unindexed WAL
// tail in hybrid mode (correctness rule 5): a document upserted AFTER an index
// build — landing in [IndexedUpTo, WALSeq) — is still fusable and returned.
func TestRunQueryHybridReadsTail(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	indexed := []Document{
		vecDoc("old", []float32{0, 1, 0, 0}, "stale text"),
	}
	store := setupNS(ctx, t, testConfig(), [][]Document{indexed})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// fresh lands in the unindexed tail after the index build.
	seedTail(ctx, t, store, [][]Document{{
		vecDoc("fresh", []float32{1, 0, 0, 0}, "quick walrus"),
	}})

	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: []float32{1, 0, 0, 0}, Text: "quick walrus"}, TopK: 5,
	})
	if err != nil {
		t.Fatalf("hybrid RunQuery: got err %v, want nil", err)
	}
	if rrfScoreOf(got, "fresh") <= 0 {
		t.Errorf("freshly upserted tail doc not fused: %+v", got)
	}
	// fresh matches both signals; old matches neither well, so fresh must win.
	if got[0].ID != "fresh" {
		t.Errorf("hybrid winner = %q, want fresh (the tail doc strong on both signals)", got[0].ID)
	}
}
