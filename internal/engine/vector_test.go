package engine

import (
	"math"
	"testing"
)

const distEps = 1e-6

func TestCosineDistance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float64
	}{
		{"identical", []float32{1, 0, 0}, []float32{1, 0, 0}, 0},
		{"same direction different magnitude", []float32{1, 2, 3}, []float32{2, 4, 6}, 0},
		{"orthogonal", []float32{1, 0}, []float32{0, 1}, 1},
		{"opposite", []float32{1, 0}, []float32{-1, 0}, 2},
		{"zero norm a", []float32{0, 0, 0}, []float32{1, 2, 3}, 1},
		{"zero norm b", []float32{1, 2, 3}, []float32{0, 0, 0}, 1},
		{"both zero norm", []float32{0, 0}, []float32{0, 0}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CosineDistance(tt.a, tt.b)
			if math.IsNaN(got) {
				t.Fatalf("CosineDistance(%v, %v) = NaN; want %v", tt.a, tt.b, tt.want)
			}
			if math.Abs(got-tt.want) > distEps {
				t.Errorf("CosineDistance(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestEuclideanDistance(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []float32
		want float64
	}{
		{"identical", []float32{1, 2, 3}, []float32{1, 2, 3}, 0},
		{"3-4-5 triangle", []float32{0, 0}, []float32{3, 4}, 5},
		{"unit step", []float32{0}, []float32{1}, 1},
		{"negative coords", []float32{-1, -1}, []float32{1, 1}, math.Sqrt(8)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := EuclideanDistance(tt.a, tt.b)
			if math.Abs(got-tt.want) > distEps {
				t.Errorf("EuclideanDistance(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestDistanceDispatch(t *testing.T) {
	t.Parallel()
	a := []float32{1, 0}
	b := []float32{0, 1}
	tests := []struct {
		name   string
		metric string
		want   float64
	}{
		{"cosine", MetricCosine, CosineDistance(a, b)},
		{"euclidean", MetricEuclidean, EuclideanDistance(a, b)},
		{"unknown falls back to euclidean", "manhattan", EuclideanDistance(a, b)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Distance(tt.metric, a, b)
			if math.Abs(got-tt.want) > distEps {
				t.Errorf("Distance(%q, %v, %v) = %v, want %v", tt.metric, a, b, got, tt.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   []float32
		want []float32
	}{
		{"unit x stays unit", []float32{1, 0, 0}, []float32{1, 0, 0}},
		{"3-4 scales to 0.6-0.8", []float32{3, 4}, []float32{0.6, 0.8}},
		{"zero vector stays zero", []float32{0, 0, 0}, []float32{0, 0, 0}},
		{"empty", []float32{}, []float32{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Normalize(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("Normalize(%v) length = %d, want %d", tt.in, len(got), len(tt.want))
			}
			for i := range got {
				if math.IsNaN(float64(got[i])) {
					t.Fatalf("Normalize(%v)[%d] = NaN", tt.in, i)
				}
				if math.Abs(float64(got[i]-tt.want[i])) > distEps {
					t.Errorf("Normalize(%v)[%d] = %v, want %v", tt.in, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestNormalizeDoesNotMutateInput(t *testing.T) {
	t.Parallel()
	in := []float32{3, 4}
	_ = Normalize(in)
	if in[0] != 3 || in[1] != 4 {
		t.Errorf("Normalize mutated its input: got %v, want [3 4]", in)
	}
}

func TestChooseK(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"zero", 0, 1},
		{"negative", -5, 1},
		{"one", 1, 1},
		{"four", 4, 2},
		{"nine", 9, 3},
		{"hundred", 100, 10},
		{"ten rounds to three", 10, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := ChooseK(tt.n); got != tt.want {
				t.Errorf("ChooseK(%d) = %d, want %d", tt.n, got, tt.want)
			}
		})
	}
}

func TestKMeansEmpty(t *testing.T) {
	t.Parallel()
	centroids, assign := KMeans(nil, 3, MetricEuclidean, 10)
	if centroids != nil || assign != nil {
		t.Errorf("KMeans(nil) = (%v, %v), want (nil, nil)", centroids, assign)
	}
}

func TestKMeansSinglePoint(t *testing.T) {
	t.Parallel()
	points := [][]float32{{1, 2, 3}}
	centroids, assign := KMeans(points, 1, MetricEuclidean, 10)
	if len(centroids) != 1 {
		t.Fatalf("KMeans(N=1, k=1) produced %d centroids, want 1", len(centroids))
	}
	if len(assign) != 1 || assign[0] != 0 {
		t.Fatalf("KMeans(N=1) assign = %v, want [0]", assign)
	}
	for d := range points[0] {
		if math.Abs(float64(centroids[0][d]-points[0][d])) > distEps {
			t.Errorf("centroid[%d] = %v, want %v", d, centroids[0][d], points[0][d])
		}
	}
}

func TestKMeansK1IsMean(t *testing.T) {
	t.Parallel()
	points := [][]float32{{0, 0}, {2, 0}, {0, 2}, {2, 2}}
	centroids, assign := KMeans(points, 1, MetricEuclidean, 10)
	if len(centroids) != 1 {
		t.Fatalf("k=1 produced %d centroids, want 1", len(centroids))
	}
	want := []float32{1, 1} // mean of the four corners
	for d := range want {
		if math.Abs(float64(centroids[0][d]-want[d])) > distEps {
			t.Errorf("centroid[%d] = %v, want %v", d, centroids[0][d], want[d])
		}
	}
	for i, c := range assign {
		if c != 0 {
			t.Errorf("assign[%d] = %d, want 0 (only one cluster)", i, c)
		}
	}
}

func TestKMeansSeparatesTwoClusters(t *testing.T) {
	t.Parallel()
	// Two tight groups far apart; k=2 must split them cleanly.
	points := [][]float32{
		{0, 0}, {0.1, 0}, {0, 0.1},
		{10, 10}, {10.1, 10}, {10, 10.1},
	}
	_, assign := KMeans(points, 2, MetricEuclidean, 25)
	// The first three and the last three must each share one cluster, and the
	// two clusters must differ.
	if assign[0] != assign[1] || assign[1] != assign[2] {
		t.Errorf("low group split across clusters: assign[:3] = %v", assign[:3])
	}
	if assign[3] != assign[4] || assign[4] != assign[5] {
		t.Errorf("high group split across clusters: assign[3:] = %v", assign[3:])
	}
	if assign[0] == assign[3] {
		t.Errorf("the two groups landed in the same cluster: %v", assign)
	}
}

func TestKMeansReseedsEmptyCluster(t *testing.T) {
	t.Parallel()
	// Two distinct points but k=3. With initial centroids = first 3 points, the
	// duplicates at index 0/1 collapse and a cluster would go empty unless
	// reseeding kicks in. We assert k non-empty clusters and full coverage.
	points := [][]float32{
		{0, 0}, {0, 0}, {0, 0},
		{5, 5}, {5, 5},
		{9, 9},
	}
	centroids, assign := KMeans(points, 3, MetricEuclidean, 25)
	if len(centroids) != 3 {
		t.Fatalf("KMeans produced %d centroids, want 3", len(centroids))
	}
	counts := make([]int, len(centroids))
	for i, c := range assign {
		if c < 0 || c >= len(centroids) {
			t.Fatalf("assign[%d] = %d out of range [0,%d)", i, c, len(centroids))
		}
		counts[c]++
	}
	for c, n := range counts {
		if n == 0 {
			t.Errorf("cluster %d is empty after reseeding; counts = %v", c, counts)
		}
	}
	// Every centroid must be finite (no NaN from dividing an empty cluster).
	for c, centroid := range centroids {
		for d, x := range centroid {
			if math.IsNaN(float64(x)) {
				t.Errorf("centroid[%d][%d] = NaN", c, d)
			}
		}
	}
}

func TestKMeansClampsKAboveN(t *testing.T) {
	t.Parallel()
	points := [][]float32{{1}, {2}}
	centroids, assign := KMeans(points, 10, MetricEuclidean, 5)
	if len(centroids) != 2 {
		t.Errorf("k clamped to N: got %d centroids, want 2", len(centroids))
	}
	if len(assign) != 2 {
		t.Errorf("assign length = %d, want 2", len(assign))
	}
}

func TestResidualCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		v        []float32
		centroid []float32
		want     []uint64
	}{
		{
			name:     "all non-negative residuals set all bits",
			v:        []float32{1, 1, 1},
			centroid: []float32{0, 0, 0},
			want:     []uint64{0b111},
		},
		{
			name:     "all negative residuals clear all bits",
			v:        []float32{-1, -1, -1},
			centroid: []float32{0, 0, 0},
			want:     []uint64{0b000},
		},
		{
			name:     "zero residual counts as set (>= 0)",
			v:        []float32{0, 0},
			centroid: []float32{0, 0},
			want:     []uint64{0b11},
		},
		{
			name:     "mixed signs pack little-endian",
			v:        []float32{2, 0, -3, 4},
			centroid: []float32{1, 5, 1, 0}, // residual signs: + - - +
			want:     []uint64{0b1001},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ResidualCode(tt.v, tt.centroid)
			if len(got) != len(tt.want) {
				t.Fatalf("ResidualCode words = %d, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("ResidualCode word %d = %b, want %b", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestResidualCodeWordPadding(t *testing.T) {
	t.Parallel()
	// 65 dims must pack into two uint64 words; the spillover bit lands in word 1.
	v := make([]float32, 65)
	centroid := make([]float32, 65)
	v[64] = 1 // only the last dimension is non-negative-and-positive
	for i := 0; i < 64; i++ {
		v[i] = -1
	}
	code := ResidualCode(v, centroid)
	if len(code) != 2 {
		t.Fatalf("ResidualCode for 65 dims = %d words, want 2", len(code))
	}
	if code[0] != 0 {
		t.Errorf("word 0 = %b, want 0 (all residuals negative)", code[0])
	}
	if code[1] != 1 {
		t.Errorf("word 1 = %b, want 1 (dimension 64 set)", code[1])
	}
}

func TestHammingAndAgreement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		a, b        []uint64
		dim         int
		wantHamming int
		wantAgree   int
	}{
		{"identical", []uint64{0b1010}, []uint64{0b1010}, 4, 0, 4},
		{"all differ in dim", []uint64{0b1111}, []uint64{0b0000}, 4, 4, 0},
		{"one differs", []uint64{0b1011}, []uint64{0b1001}, 4, 1, 3},
		{"multi-word", []uint64{0xFFFFFFFFFFFFFFFF, 0b1}, []uint64{0, 0b1}, 65, 64, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := Hamming(tt.a, tt.b); got != tt.wantHamming {
				t.Errorf("Hamming(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.wantHamming)
			}
			if got := Agreement(tt.a, tt.b, tt.dim); got != tt.wantAgree {
				t.Errorf("Agreement(%v, %v, %d) = %d, want %d", tt.a, tt.b, tt.dim, got, tt.wantAgree)
			}
		})
	}
}

func TestResidualCodeAgreementRoundTrip(t *testing.T) {
	t.Parallel()
	centroid := []float32{0, 0, 0, 0}
	// A vector compared against itself must agree on every dimension.
	v := []float32{1, -2, 3, -4}
	code := ResidualCode(v, centroid)
	if got := Agreement(code, code, len(v)); got != len(v) {
		t.Errorf("self-agreement = %d, want %d", got, len(v))
	}
	// A vector and its negation flip every sign bit: zero agreement.
	neg := []float32{-1, 2, -3, 4}
	negCode := ResidualCode(neg, centroid)
	if got := Agreement(code, negCode, len(v)); got != 0 {
		t.Errorf("opposite-direction agreement = %d, want 0", got)
	}
	// A vector sharing the centroid's direction in 3 of 4 dims agrees on 3.
	near := []float32{1, -2, 3, 4} // last sign flips relative to v
	nearCode := ResidualCode(near, centroid)
	if got := Agreement(code, nearCode, len(v)); got != 3 {
		t.Errorf("near-direction agreement = %d, want 3", got)
	}
}
