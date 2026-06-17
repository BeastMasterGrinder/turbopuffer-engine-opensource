package engine

import (
	"math"
	"reflect"
	"sort"
	"testing"
)

func TestTokenize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "empty string", in: "", want: nil},
		{name: "whitespace only", in: "   \t\n ", want: nil},
		{name: "punctuation only", in: "!?.,;:", want: nil},
		{name: "simple words", in: "quick brown fox", want: []string{"quick", "brown", "fox"}},
		{name: "mixed case lowercased", in: "Quick BROWN Fox", want: []string{"quick", "brown", "fox"}},
		{name: "punctuation separators", in: "Quick, brown. FOX!", want: []string{"quick", "brown", "fox"}},
		{name: "collapses repeated separators", in: "a---b   c", want: []string{"a", "b", "c"}},
		{name: "leading and trailing separators", in: ".hello.", want: []string{"hello"}},
		{name: "alphanumeric kept together", in: "user42 v2 go1.26", want: []string{"user42", "v2", "go1", "26"}},
		{name: "digits are terms", in: "the 100 walruses", want: []string{"the", "100", "walruses"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Tokenize(tt.in)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tokenize(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildBM25Stats(t *testing.T) {
	t.Parallel()

	docs := map[string]string{
		"a": "the quick brown fox",     // 4 terms
		"b": "the lazy dog",            // 3 terms
		"c": "quick quick brown brown", // 4 terms
	}

	idx := BuildBM25(docs)

	if got, want := idx.N, 3; got != want {
		t.Errorf("BuildBM25 N = %d, want %d", got, want)
	}

	// AvgDL = (4 + 3 + 4) / 3.
	wantAvg := 11.0 / 3.0
	if math.Abs(idx.AvgDL-wantAvg) > 1e-9 {
		t.Errorf("BuildBM25 AvgDL = %v, want %v", idx.AvgDL, wantAvg)
	}

	wantDocLen := map[string]int{"a": 4, "b": 3, "c": 4}
	if !reflect.DeepEqual(idx.DocLen, wantDocLen) {
		t.Errorf("BuildBM25 DocLen = %v, want %v", idx.DocLen, wantDocLen)
	}

	// Document frequency per term = number of postings for that term.
	wantDF := map[string]int{
		"the":   2,
		"quick": 2,
		"brown": 2,
		"fox":   1,
		"lazy":  1,
		"dog":   1,
	}
	for term, df := range wantDF {
		if got := len(idx.Index[term]); got != df {
			t.Errorf("df(%q) = %d, want %d", term, got, df)
		}
	}

	// Within-document term frequency is collapsed into one posting per (term, doc).
	postings := idx.Index["quick"]
	wantPostings := []Posting{{ID: "a", TF: 1}, {ID: "c", TF: 2}}
	if !reflect.DeepEqual(postings, wantPostings) {
		t.Errorf("postings(quick) = %v, want %v", postings, wantPostings)
	}
}

func TestBuildBM25Empty(t *testing.T) {
	t.Parallel()

	idx := BuildBM25(map[string]string{})
	if idx.N != 0 {
		t.Errorf("empty BuildBM25 N = %d, want 0", idx.N)
	}
	if idx.AvgDL != 0 {
		t.Errorf("empty BuildBM25 AvgDL = %v, want 0", idx.AvgDL)
	}
	if len(idx.Index) != 0 {
		t.Errorf("empty BuildBM25 Index has %d terms, want 0", len(idx.Index))
	}
	// Scoring against an empty index must not panic and yields no hits.
	if got := Score(idx, []string{"anything"}); len(got) != 0 {
		t.Errorf("Score on empty index = %v, want empty", got)
	}
}

func TestScoreRanking(t *testing.T) {
	t.Parallel()

	docs := map[string]string{
		"a": "walrus walrus walrus tusks",  // many "walrus"
		"b": "the walrus swims in the sea", // one "walrus", longer, common terms
		"c": "the quick brown fox",         // no "walrus"
	}
	idx := BuildBM25(docs)

	scores := Score(idx, []string{"walrus"})

	if _, ok := scores["c"]; ok {
		t.Errorf("doc c matched no query term but got a score: %v", scores)
	}
	if _, ok := scores["a"]; !ok {
		t.Fatalf("doc a should match 'walrus' but is absent: %v", scores)
	}
	if _, ok := scores["b"]; !ok {
		t.Fatalf("doc b should match 'walrus' but is absent: %v", scores)
	}

	// Doc a has higher term frequency and is shorter, so it must outrank b.
	if scores["a"] <= scores["b"] {
		t.Errorf("Score ranking: a=%v should outrank b=%v", scores["a"], scores["b"])
	}

	// Ranking order sanity: descending by score, a before b, c absent.
	ranked := rankIDs(scores)
	want := []string{"a", "b"}
	if !reflect.DeepEqual(ranked, want) {
		t.Errorf("ranked ids = %v, want %v", ranked, want)
	}
}

func TestScoreCommonTermDownweighted(t *testing.T) {
	t.Parallel()

	// "the" appears in every doc (IDF ~0); "rare" appears in one.
	docs := map[string]string{
		"a": "the rare bird",
		"b": "the common thing",
		"c": "the common thing",
	}
	idx := BuildBM25(docs)

	rare := Score(idx, []string{"rare"})
	common := Score(idx, []string{"the"})

	if rare["a"] <= 0 {
		t.Fatalf("rare term should give positive score, got %v", rare["a"])
	}
	// A term in every document carries near-zero information; its score must be
	// far below a term that appears in a single document.
	if common["a"] >= rare["a"] {
		t.Errorf("common-term score a=%v should be below rare-term score a=%v", common["a"], rare["a"])
	}
}

func TestScoreDocConsistentWithScore(t *testing.T) {
	t.Parallel()

	docs := map[string]string{
		"a": "the quick brown fox jumps",
		"b": "a lazy quick dog",
		"c": "quick quick quick",
	}
	idx := BuildBM25(docs)
	query := []string{"quick", "fox"}

	indexed := Score(idx, query)

	// ScoreDoc, given the same global stats and a doc's own text, must produce
	// the identical score the inverted-index path produced for that indexed doc.
	for id, text := range docs {
		got := ScoreDoc(idx, text, query)
		want := indexed[id] // zero if the doc matched no query term
		if math.Abs(got-want) > 1e-9 {
			t.Errorf("ScoreDoc(%q) = %v, Score()[%q] = %v; want equal", id, got, id, want)
		}
	}
}

func TestScoreDocNoMatch(t *testing.T) {
	t.Parallel()

	idx := BuildBM25(map[string]string{"a": "quick brown fox"})
	if got := ScoreDoc(idx, "totally unrelated text", []string{"quick"}); got != 0 {
		t.Errorf("ScoreDoc with no matching term = %v, want 0", got)
	}
}

// rankIDs returns the ids of a score map ordered by descending score, breaking
// ties by id so the order is deterministic for assertions.
func rankIDs(scores map[string]float64) []string {
	ids := make([]string, 0, len(scores))
	for id := range scores {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		if scores[ids[i]] != scores[ids[j]] {
			return scores[ids[i]] > scores[ids[j]]
		}
		return ids[i] < ids[j]
	})
	return ids
}
