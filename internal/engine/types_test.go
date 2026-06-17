package engine

import "testing"

func TestFilterMatch(t *testing.T) {
	attrs := map[string]any{
		"lang":    "en",
		"rank":    float64(5), // JSON decodes numbers to float64
		"score":   float64(2.5),
		"active":  true,
		"missing": nil,
	}

	tests := []struct {
		name   string
		filter Filter
		want   bool
	}{
		{
			name:   "match-all empty op",
			filter: Filter{},
			want:   true,
		},
		{
			name:   "eq string hit",
			filter: Filter{Op: "eq", Field: "lang", Value: "en"},
			want:   true,
		},
		{
			name:   "eq string miss",
			filter: Filter{Op: "eq", Field: "lang", Value: "de"},
			want:   false,
		},
		{
			name:   "eq numeric coercion int literal vs float64 attr",
			filter: Filter{Op: "eq", Field: "rank", Value: 5}, // int vs float64(5)
			want:   true,
		},
		{
			name:   "eq numeric coercion float literal",
			filter: Filter{Op: "eq", Field: "score", Value: 2.5},
			want:   true,
		},
		{
			name:   "eq numeric miss",
			filter: Filter{Op: "eq", Field: "rank", Value: 6},
			want:   false,
		},
		{
			name:   "eq bool hit",
			filter: Filter{Op: "eq", Field: "active", Value: true},
			want:   true,
		},
		{
			name:   "eq numeric value against string attr is false",
			filter: Filter{Op: "eq", Field: "lang", Value: 5},
			want:   false,
		},
		{
			name:   "eq on missing attribute is false",
			filter: Filter{Op: "eq", Field: "nope", Value: "x"},
			want:   false,
		},
		{
			name: "and all true",
			filter: Filter{Op: "and", Sub: []Filter{
				{Op: "eq", Field: "lang", Value: "en"},
				{Op: "eq", Field: "rank", Value: 5},
			}},
			want: true,
		},
		{
			name: "and one false",
			filter: Filter{Op: "and", Sub: []Filter{
				{Op: "eq", Field: "lang", Value: "en"},
				{Op: "eq", Field: "rank", Value: 99},
			}},
			want: false,
		},
		{
			name:   "and empty sub is vacuously true",
			filter: Filter{Op: "and"},
			want:   true,
		},
		{
			name: "or one true",
			filter: Filter{Op: "or", Sub: []Filter{
				{Op: "eq", Field: "lang", Value: "de"},
				{Op: "eq", Field: "rank", Value: 5},
			}},
			want: true,
		},
		{
			name: "or all false",
			filter: Filter{Op: "or", Sub: []Filter{
				{Op: "eq", Field: "lang", Value: "de"},
				{Op: "eq", Field: "rank", Value: 99},
			}},
			want: false,
		},
		{
			name:   "or empty sub is false",
			filter: Filter{Op: "or"},
			want:   false,
		},
		{
			name: "nested and within or",
			filter: Filter{Op: "or", Sub: []Filter{
				{Op: "and", Sub: []Filter{
					{Op: "eq", Field: "lang", Value: "de"},
					{Op: "eq", Field: "active", Value: true},
				}},
				{Op: "and", Sub: []Filter{
					{Op: "eq", Field: "lang", Value: "en"},
					{Op: "eq", Field: "rank", Value: 5},
				}},
			}},
			want: true,
		},
		{
			name: "nested and within and missing attr false",
			filter: Filter{Op: "and", Sub: []Filter{
				{Op: "eq", Field: "lang", Value: "en"},
				{Op: "eq", Field: "absent", Value: "x"},
			}},
			want: false,
		},
		{
			name:   "unknown op matches nothing",
			filter: Filter{Op: "gte", Field: "rank", Value: 1},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.filter.Match(attrs)
			if got != tt.want {
				t.Errorf("Filter.Match(%+v) = %v, want %v", tt.filter, got, tt.want)
			}
		})
	}
}

func TestFilterMatchNilAttrs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		filter Filter
		want   bool
	}{
		{"match-all on nil attrs", Filter{}, true},
		{"eq on nil attrs is false", Filter{Op: "eq", Field: "x", Value: 1}, false},
		{"and on nil attrs", Filter{Op: "and", Sub: []Filter{{Op: "eq", Field: "x", Value: 1}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.filter.Match(nil)
			if got != tt.want {
				t.Errorf("Filter.Match(nil) op=%q = %v, want %v", tt.filter.Op, got, tt.want)
			}
		})
	}
}

func TestRankByMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		rankBy     RankBy
		wantVector bool
		wantText   bool
	}{
		{"vector set", RankBy{Vector: []float32{0.1, 0.2}}, true, false},
		{"text set", RankBy{Text: "quick walrus"}, false, true},
		{"empty vector slice is vector mode", RankBy{Vector: []float32{}}, true, false},
		{"neither set", RankBy{}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.rankBy.IsVector(); got != tt.wantVector {
				t.Errorf("RankBy.IsVector() = %v, want %v", got, tt.wantVector)
			}
			if got := tt.rankBy.IsText(); got != tt.wantText {
				t.Errorf("RankBy.IsText() = %v, want %v", got, tt.wantText)
			}
		})
	}
}
