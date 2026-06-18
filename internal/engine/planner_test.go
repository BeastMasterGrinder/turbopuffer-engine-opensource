package engine

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// attrDoc builds a document with a vector and an arbitrary attribute set, used to
// seed the bitmap attribute index with low-cardinality categorical fields.
func attrDoc(id string, vec []float32, attrs map[string]any) Document {
	return Document{ID: id, Vector: vec, Attributes: attrs}
}

// bruteForceVector is the trivially-correct reference the bitmap planner must
// match: it materializes every live document, computes the exact distance, keeps
// only those passing Filter.Match (the pre-bitmap per-candidate semantics), and
// returns the TopK ids nearest-first with the same id tie-break the engine uses.
// If the planner ever returns a different set, the bitmap path changed an answer.
func bruteForceVector(ctx context.Context, t *testing.T, store *cache.Store, m Manifest, query []float32, filter Filter, topK int) []string {
	t.Helper()
	live, err := MaterializeLive(ctx, store, testNS, 0, m.WALSeq)
	if err != nil {
		t.Fatalf("brute-force materialize: got err %v, want nil", err)
	}
	type hit struct {
		id   string
		dist float64
	}
	var hits []hit
	for id, d := range live {
		if d.Vector == nil || !filter.Match(d.Attributes) {
			continue
		}
		hits = append(hits, hit{id: id, dist: Distance(m.Metric, query, d.Vector)})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dist != hits[j].dist {
			return hits[i].dist < hits[j].dist
		}
		return hits[i].id < hits[j].id
	})
	if len(hits) > topK {
		hits = hits[:topK]
	}
	ids := make([]string, len(hits))
	for i, h := range hits {
		ids[i] = h.id
	}
	return ids
}

func TestBuildAttributeIndexCorrectness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// Three langs across five docs; "body" is the text field and must NOT be
	// bitmap-indexed. rank is numeric to exercise the numeric value key.
	seedWAL(ctx, t, store, testNS, [][]Document{{
		attrDoc("a", []float32{1, 0, 0, 0}, map[string]any{"lang": "en", "rank": 1, "body": "alpha"}),
		attrDoc("b", []float32{0, 1, 0, 0}, map[string]any{"lang": "en", "rank": 2, "body": "beta"}),
		attrDoc("c", []float32{0, 0, 1, 0}, map[string]any{"lang": "fr", "rank": 1, "body": "gamma"}),
		attrDoc("d", []float32{0, 0, 0, 1}, map[string]any{"lang": "de", "rank": 2, "body": "delta"}),
		attrDoc("e", []float32{1, 1, 0, 0}, map[string]any{"lang": "en", "rank": 2, "body": "epsilon"}),
	}})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	af, err := loadAttrs(ctx, store, testNS, 1)
	if err != nil {
		t.Fatalf("loadAttrs: got err %v, want nil", err)
	}
	if af == nil {
		t.Fatal("bitmaps.json must be written by BuildIndex")
	}

	// Ordinals cover every live document exactly once.
	if len(af.Ords) != 5 || len(af.ClusterOf) != 5 {
		t.Fatalf("ordinal arrays: got Ords=%d ClusterOf=%d, want 5 each", len(af.Ords), len(af.ClusterOf))
	}

	// The text field is excluded; lang and rank are indexed.
	if _, ok := af.Values["body"]; ok {
		t.Errorf("text field %q must not be bitmap-indexed", "body")
	}
	for _, field := range []string{"lang", "rank"} {
		if _, ok := af.Values[field]; !ok {
			t.Errorf("field %q must be bitmap-indexed", field)
		}
	}

	// Resolve a (field,value) bitmap back to the concrete ids it covers and check
	// it against the seed data.
	idsFor := func(field, key string) []string {
		bm := bitmapFromSorted(af.Values[field][key])
		var out []string
		bm.each(func(ord uint32) { out = append(out, af.Ords[ord]) })
		sort.Strings(out)
		return out
	}
	if got, want := idsFor("lang", "s:en"), []string{"a", "b", "e"}; !equalStrings(got, want) {
		t.Errorf("lang=en bitmap: got %v, want %v", got, want)
	}
	if got, want := idsFor("lang", "s:fr"), []string{"c"}; !equalStrings(got, want) {
		t.Errorf("lang=fr bitmap: got %v, want %v", got, want)
	}
	// 1 keyed through float64 formatting, matching valueKey/Filter.Match coercion.
	if got, want := idsFor("rank", "n:1"), []string{"a", "c"}; !equalStrings(got, want) {
		t.Errorf("rank=1 bitmap: got %v, want %v", got, want)
	}
}

func TestBuildAttributeIndexSkipsHighCardinality(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, NamespaceConfig{Dimension: 4, Metric: "cosine"}); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// 200 docs: "tier" is a 2-value categorical field (indexed); "uid" is unique
	// per document (200 distinct values ⇒ above both the floor and the ratio, so it
	// must be skipped as pure overhead with no pruning value).
	rng := rand.New(rand.NewSource(5))
	var ops []Document
	for i := 0; i < 200; i++ {
		attrs := map[string]any{
			"tier": []string{"gold", "silver"}[i%2],
			"uid":  fmt.Sprintf("u-%d", i),
		}
		ops = append(ops, attrDoc(fmt.Sprintf("d%d", i), randVec(rng, 4), attrs))
	}
	seedWAL(ctx, t, store, testNS, [][]Document{ops})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	af, err := loadAttrs(ctx, store, testNS, 1)
	if err != nil || af == nil {
		t.Fatalf("loadAttrs: af=%v err=%v", af, err)
	}
	if _, ok := af.Values["tier"]; !ok {
		t.Errorf("low-cardinality field %q must be indexed", "tier")
	}
	if _, ok := af.Values["uid"]; ok {
		t.Errorf("high-cardinality field %q must be skipped", "uid")
	}

	// A query filtering on the skipped field must still return correct results via
	// the per-candidate Filter.Match fallback (the plan is just unusable for it).
	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	plan, err := planVectorQuery(ctx, store, testNS, m.IndexEpoch, Filter{Op: "eq", Field: "uid", Value: "u-7"})
	if err != nil {
		t.Fatalf("planVectorQuery: got err %v, want nil", err)
	}
	if plan.usable {
		t.Errorf("plan on unindexed field: got usable=true, want false (fall back to Filter.Match)")
	}
	// The plan is unusable, so the query gathers candidates exactly as the
	// pre-bitmap engine did and Filter.Match decides membership. We cannot assert a
	// specific recall (the unindexed field falls into the same shortlist-limited
	// approximation as before), but every returned document MUST satisfy the filter
	// — the Filter.Match fallback is still authoritative.
	got, err := RunQuery(ctx, store, testNS, QueryParams{
		RankBy: RankBy{Vector: randVec(rand.New(rand.NewSource(1)), 4)},
		Filter: Filter{Op: "eq", Field: "uid", Value: "u-7"},
		TopK:   10,
		NProbe: 1000,
	})
	if err != nil {
		t.Fatalf("RunQuery on skipped field: got err %v, want nil", err)
	}
	for _, r := range got {
		if r.Attributes["uid"] != "u-7" {
			t.Errorf("result %q has uid=%v, want u-7 (Filter.Match fallback must hold on the unindexed field)", r.ID, r.Attributes["uid"])
		}
	}
}

func TestCompileFilterMatchesFilterMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}
	seedWAL(ctx, t, store, testNS, [][]Document{{
		attrDoc("a", []float32{1, 0, 0, 0}, map[string]any{"lang": "en", "tier": "gold"}),
		attrDoc("b", []float32{0, 1, 0, 0}, map[string]any{"lang": "en", "tier": "silver"}),
		attrDoc("c", []float32{0, 0, 1, 0}, map[string]any{"lang": "fr", "tier": "gold"}),
		attrDoc("d", []float32{0, 0, 0, 1}, map[string]any{"lang": "de", "tier": "silver"}),
	}})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}
	af, err := loadAttrs(ctx, store, testNS, 1)
	if err != nil || af == nil {
		t.Fatalf("loadAttrs: af=%v err=%v", af, err)
	}

	filters := []Filter{
		{Op: "eq", Field: "lang", Value: "en"},
		{Op: "eq", Field: "tier", Value: "gold"},
		{Op: "and", Sub: []Filter{{Op: "eq", Field: "lang", Value: "en"}, {Op: "eq", Field: "tier", Value: "gold"}}},
		{Op: "or", Sub: []Filter{{Op: "eq", Field: "lang", Value: "fr"}, {Op: "eq", Field: "tier", Value: "silver"}}},
		{Op: "eq", Field: "lang", Value: "missing"}, // empty match set
	}
	live, _ := MaterializeLive(ctx, store, testNS, 0, 1)
	for _, f := range filters {
		bm, ok := compileFilter(f, af)
		if !ok {
			t.Fatalf("compileFilter(%+v): ok=false, want true (all fields indexed)", f)
		}
		got := map[string]bool{}
		bm.each(func(ord uint32) { got[af.Ords[ord]] = true })
		for id, d := range live {
			want := f.Match(d.Attributes)
			if got[id] != want {
				t.Errorf("filter %+v on %q: bitmap=%v, Filter.Match=%v", f, id, got[id], want)
			}
		}
	}

	// A filter touching an unindexed field cannot be compiled, so the planner must
	// fall back to the exact path.
	if _, ok := compileFilter(Filter{Op: "eq", Field: "nope", Value: "x"}, af); ok {
		t.Errorf("compileFilter on unindexed field: ok=true, want false (fall back)")
	}
}

func TestPlannerPicksFilterFirstWhenSelective(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, NamespaceConfig{Dimension: 4, Metric: "cosine"}); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// 200 docs: exactly one carries tier=rare (0.5% selectivity, below the 5%
	// threshold), the rest are tier=common.
	rng := rand.New(rand.NewSource(3))
	var ops []Document
	for i := 0; i < 200; i++ {
		tier := "common"
		if i == 137 {
			tier = "rare"
		}
		ops = append(ops, attrDoc(fmt.Sprintf("d%d", i), randVec(rng, 4), map[string]any{"tier": tier}))
	}
	seedWAL(ctx, t, store, testNS, [][]Document{ops})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}
	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}

	// Selective predicate → filter-first.
	rarePlan, err := planVectorQuery(ctx, store, testNS, m.IndexEpoch, Filter{Op: "eq", Field: "tier", Value: "rare"})
	if err != nil {
		t.Fatalf("planVectorQuery(rare): got err %v, want nil", err)
	}
	if !rarePlan.usable || !rarePlan.filterFirst {
		t.Errorf("rare filter: got usable=%v filterFirst=%v, want true/true (selective ⇒ filter-first)", rarePlan.usable, rarePlan.filterFirst)
	}
	if rarePlan.cands.len() != 1 {
		t.Errorf("rare candidate count: got %d, want 1", rarePlan.cands.len())
	}

	// Non-selective predicate → search-first (prune only).
	commonPlan, err := planVectorQuery(ctx, store, testNS, m.IndexEpoch, Filter{Op: "eq", Field: "tier", Value: "common"})
	if err != nil {
		t.Fatalf("planVectorQuery(common): got err %v, want nil", err)
	}
	if !commonPlan.usable || commonPlan.filterFirst {
		t.Errorf("common filter: got usable=%v filterFirst=%v, want true/false (non-selective ⇒ search-first)", commonPlan.usable, commonPlan.filterFirst)
	}

	// An empty filter is a pass-through plan (no prune, no filter-first).
	emptyPlan, err := planVectorQuery(ctx, store, testNS, m.IndexEpoch, Filter{})
	if err != nil {
		t.Fatalf("planVectorQuery(empty): got err %v, want nil", err)
	}
	if emptyPlan.usable {
		t.Errorf("empty filter: got usable=true, want false (pass-through)")
	}
}

// TestVectorPlannerResultsIdenticalAcrossSelectivities is THE key invariant: the
// bitmap planner — filter-first when selective, search-first-with-prune otherwise —
// must return exactly the same TopK ids as the brute-force per-candidate path, for
// every selectivity. Same answers, just faster.
//
// To make brute-force the exact ground truth for BOTH plans, the namespace is
// sized so the IVF index introduces no approximation here: NProbe is set far above
// the cluster count (so search-first probes every cluster) and N stays below the
// RaBitQ shortlist limit topK*shortlistMultiplier (so the prefilter never truncates).
// Under those conditions the pre-bitmap path was already exact, so "identical to the
// brute force" is exactly "identical to today's per-candidate path" — the win is
// purely the work skipped, never a changed answer.
func TestVectorPlannerResultsIdenticalAcrossSelectivities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, NamespaceConfig{Dimension: 8, Metric: "cosine"}); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// 36 docs (< topK*shortlistMultiplier = 40, so no shortlist truncation) across
	// categories of widely varying selectivity:
	//   tier:  rare (1 doc ⇒ <5% ⇒ filter-first) | mid (~11%) | common (~86%)
	//   color: 5 evenly-split values
	// so eq/AND/OR over the two span ultra-selective to broad, exercising both plans.
	const n = 36
	rng := rand.New(rand.NewSource(11))
	colors := []string{"red", "green", "blue", "yellow", "purple"}
	var ops []Document
	for i := 0; i < n; i++ {
		tier := "common"
		switch {
		case i == 0:
			tier = "rare"
		case i%9 == 0:
			tier = "mid"
		}
		attrs := map[string]any{"tier": tier, "color": colors[i%len(colors)]}
		ops = append(ops, attrDoc(fmt.Sprintf("d%02d", i), randVec(rng, 8), attrs))
	}
	seedWAL(ctx, t, store, testNS, [][]Document{ops})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}
	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}

	filters := map[string]Filter{
		"none":         {},
		"rare":         {Op: "eq", Field: "tier", Value: "rare"},
		"mid":          {Op: "eq", Field: "tier", Value: "mid"},
		"common":       {Op: "eq", Field: "tier", Value: "common"},
		"color":        {Op: "eq", Field: "color", Value: "blue"},
		"and":          {Op: "and", Sub: []Filter{{Op: "eq", Field: "tier", Value: "common"}, {Op: "eq", Field: "color", Value: "red"}}},
		"or":           {Op: "or", Sub: []Filter{{Op: "eq", Field: "tier", Value: "rare"}, {Op: "eq", Field: "color", Value: "green"}}},
		"empty-result": {Op: "eq", Field: "tier", Value: "ghost"},
	}

	// nProbe far above the cluster count forces search-first to probe every cluster,
	// so the only approximation that could differ from brute force is removed.
	const fullProbe = 1000

	qrng := rand.New(rand.NewSource(99))
	for q := 0; q < 25; q++ {
		query := randVec(qrng, 8)
		for name, f := range filters {
			want := bruteForceVector(ctx, t, store, m, query, f, 10)
			got, err := RunQuery(ctx, store, testNS, QueryParams{
				RankBy: RankBy{Vector: query},
				Filter: f,
				TopK:   10,
				NProbe: fullProbe,
			})
			if err != nil {
				t.Fatalf("RunQuery[%s]: got err %v, want nil", name, err)
			}
			if ids := resultIDs(got); !equalStrings(ids, want) {
				t.Errorf("query %d filter %q: bitmap path ids=%v, brute-force ids=%v", q, name, ids, want)
			}
		}
	}
}

// TestFilterFirstFetchesFewerClusters demonstrates the win concretely: on a
// selective filter the filter-first plan fetches ONLY the clusters its handful of
// candidates live in, so a cold-cache query touches far fewer cluster objects than
// an unfiltered search-first query over the same data. The cache's cold-miss
// counter is the proof — fewer index-object fetches is exactly the work the plan
// skips.
func TestFilterFirstFetchesFewerClusters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := storage.New()

	// 400 docs spread across many clusters; exactly two carry tier=rare.
	rng := rand.New(rand.NewSource(21))
	var ops []Document
	for i := 0; i < 400; i++ {
		tier := "common"
		if i == 50 || i == 333 {
			tier = "rare"
		}
		ops = append(ops, attrDoc(fmt.Sprintf("d%d", i), randVec(rng, 8), map[string]any{"tier": tier}))
	}
	seedStore := cache.New(backend)
	if err := CreateManifest(ctx, seedStore, testNS, NamespaceConfig{Dimension: 8, Metric: "cosine"}); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}
	seedWAL(ctx, t, seedStore, testNS, [][]Document{ops})
	if err := BuildIndex(ctx, seedStore, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// Cold cache per measurement so the miss counter reflects exactly the index
	// objects each plan touched. The unfiltered query probes the default nProbe
	// nearest clusters; the rare-filter query is filter-first and fetches only the
	// (≤2) clusters holding the rare docs.
	query := randVec(rand.New(rand.NewSource(7)), 8)
	run := func(f Filter) uint64 {
		cold := cache.New(backend)
		before := cold.Stats()
		if _, err := RunQuery(ctx, cold, testNS, QueryParams{RankBy: RankBy{Vector: query}, Filter: f, TopK: 10, NProbe: 3}); err != nil {
			t.Fatalf("RunQuery: got err %v, want nil", err)
		}
		return cold.Stats().Sub(before).Misses
	}

	unfiltered := run(Filter{})
	rare := run(Filter{Op: "eq", Field: "tier", Value: "rare"})
	if rare >= unfiltered {
		t.Errorf("filter-first cold misses: got %d (rare) vs %d (unfiltered); filter-first should fetch fewer objects", rare, unfiltered)
	}
}

// randVec returns a random dim-length vector in [0,1).
func randVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}
