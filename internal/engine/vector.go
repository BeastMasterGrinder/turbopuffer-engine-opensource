package engine

import (
	"math"
	"math/bits"
	"math/rand"
)

// Metric strings recognized by Distance and the index. These mirror the values
// stored in a namespace's Manifest.Metric.
const (
	MetricCosine    = "cosine"
	MetricEuclidean = "euclidean"
)

// CosineDistance returns the cosine distance 1 - cos(θ) between a and b, a value
// in [0, 2] where 0 means identical direction. If either vector has zero norm
// the angle is undefined, so we return 1.0 (orthogonal) rather than NaN. Callers
// must pass equal-length vectors.
func CosineDistance(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 1.0
	}
	sim := dot / (math.Sqrt(na) * math.Sqrt(nb))
	// Clamp to guard against floating-point drift outside [-1, 1].
	if sim > 1 {
		sim = 1
	} else if sim < -1 {
		sim = -1
	}
	return 1 - sim
}

// EuclideanDistance returns the L2 distance between a and b. Callers must pass
// equal-length vectors.
func EuclideanDistance(a, b []float32) float64 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return math.Sqrt(sum)
}

// Distance dispatches to the distance function named by metric. An unknown
// metric falls back to Euclidean, which is always well-defined.
func Distance(metric string, a, b []float32) float64 {
	switch metric {
	case MetricCosine:
		return CosineDistance(a, b)
	default:
		return EuclideanDistance(a, b)
	}
}

// Normalize returns a unit-length copy of v. A zero-norm vector cannot be scaled
// to unit length, so its copy is returned unchanged (all zeros) rather than
// producing NaNs. The input is never mutated.
func Normalize(v []float32) []float32 {
	var n float64
	for _, x := range v {
		n += float64(x) * float64(x)
	}
	out := make([]float32, len(v))
	if n == 0 {
		copy(out, v)
		return out
	}
	inv := 1.0 / math.Sqrt(n)
	for i, x := range v {
		out[i] = float32(float64(x) * inv)
	}
	return out
}

// ChooseK returns the number of IVF clusters for n vectors, the small-scale
// heuristic K ≈ round(√n) from docs/02, clamped to at least 1.
func ChooseK(n int) int {
	if n <= 0 {
		return 1
	}
	k := int(math.Round(math.Sqrt(float64(n))))
	if k < 1 {
		return 1
	}
	return k
}

// KMeans runs Lloyd's algorithm to partition points into k clusters under the
// given metric, returning the final centroids and a per-point assignment. It
// runs up to iters refinement passes, stopping early once assignments stabilize.
// Empty clusters are reseeded to the point farthest from its own centroid so k
// clusters are always produced (mirroring the IVF build in docs/02). The indexer
// passes the namespace's configured metric so cluster membership matches
// query-time distances.
//
// k is clamped to [1, len(points)]. With no points it returns nil, nil. The
// algorithm is deterministic: initial centroids are the first k points, so the
// same input always yields the same output (important for reproducible tests).
func KMeans(points [][]float32, k int, metric string, iters int) (centroids [][]float32, assign []int) {
	return kMeans(points, k, metric, iters, rand.New(rand.NewSource(1)))
}

func kMeans(points [][]float32, k int, metric string, iters int, rng *rand.Rand) (centroids [][]float32, assign []int) {
	n := len(points)
	if n == 0 {
		return nil, nil
	}
	if k < 1 {
		k = 1
	}
	if k > n {
		k = n
	}

	dim := len(points[0])
	centroids = make([][]float32, k)
	for i := range centroids {
		centroids[i] = cloneVec(points[i])
	}
	assign = make([]int, n)

	if iters < 1 {
		iters = 1
	}
	for iter := 0; iter < iters; iter++ {
		changed := assignPoints(points, centroids, metric, assign)
		recomputeCentroids(points, assign, centroids, dim)
		reseedEmpty(points, assign, centroids, metric, rng)
		// After the first pass, stop once no point moved between clusters.
		if !changed && iter > 0 {
			break
		}
	}
	return centroids, assign
}

// assignPoints assigns each point to its nearest centroid, returning whether any
// assignment changed from the prior pass.
func assignPoints(points, centroids [][]float32, metric string, assign []int) bool {
	changed := false
	for i, p := range points {
		best, bestDist := 0, math.Inf(1)
		for c, centroid := range centroids {
			d := Distance(metric, p, centroid)
			if d < bestDist {
				best, bestDist = c, d
			}
		}
		if assign[i] != best {
			assign[i] = best
			changed = true
		}
	}
	return changed
}

// recomputeCentroids moves each centroid to the mean of its assigned points.
// Empty clusters are left untouched here and handled by reseedEmpty.
func recomputeCentroids(points [][]float32, assign []int, centroids [][]float32, dim int) {
	sums := make([][]float64, len(centroids))
	counts := make([]int, len(centroids))
	for c := range sums {
		sums[c] = make([]float64, dim)
	}
	for i, p := range points {
		c := assign[i]
		counts[c]++
		for d, x := range p {
			sums[c][d] += float64(x)
		}
	}
	for c := range centroids {
		if counts[c] == 0 {
			continue
		}
		mean := make([]float32, dim)
		for d := 0; d < dim; d++ {
			mean[d] = float32(sums[c][d] / float64(counts[c]))
		}
		centroids[c] = mean
	}
}

// reseedEmpty gives every empty cluster a fresh centroid: the point currently
// farthest from its own assigned centroid. This guarantees k non-empty clusters
// when n >= k and stops a degenerate centroid from sticking forever. A random
// fallback breaks ties when no point stands out.
func reseedEmpty(points [][]float32, assign []int, centroids [][]float32, metric string, rng *rand.Rand) {
	counts := make([]int, len(centroids))
	for _, c := range assign {
		counts[c]++
	}
	for c := range centroids {
		if counts[c] > 0 {
			continue
		}
		far, farDist := -1, -1.0
		for i, p := range points {
			// Only steal points from clusters that still have spares, so we
			// never empty another cluster to fill this one.
			if counts[assign[i]] <= 1 {
				continue
			}
			d := Distance(metric, p, centroids[assign[i]])
			if d > farDist {
				far, farDist = i, d
			}
		}
		if far < 0 {
			far = rng.Intn(len(points))
		}
		centroids[c] = cloneVec(points[far])
		counts[assign[far]]--
		assign[far] = c
		counts[c]++
	}
}

func cloneVec(v []float32) []float32 {
	out := make([]float32, len(v))
	copy(out, v)
	return out
}

// ResidualCode packs the sign bits of the residual (v - centroid) into a
// []uint64, one bit per dimension: bit set when the residual is >= 0. This is
// the RaBitQ-lite code from docs/03 — a 1-bit-per-dimension fingerprint of the
// vector's direction relative to its cluster centroid. Words are little-endian
// within each uint64 (dimension d occupies bit d%64 of word d/64); the final
// word is zero-padded. v and centroid must be the same length.
func ResidualCode(v, centroid []float32) []uint64 {
	words := (len(v) + 63) / 64
	code := make([]uint64, words)
	for i := range v {
		if v[i]-centroid[i] >= 0 {
			code[i/64] |= 1 << uint(i%64)
		}
	}
	return code
}

// Hamming returns the number of differing bits between two equal-length codes —
// the count of dimensions whose residual sign disagrees.
func Hamming(a, b []uint64) int {
	var d int
	for i := range a {
		d += bits.OnesCount64(a[i] ^ b[i])
	}
	return d
}

// Agreement returns the number of matching bits across dim dimensions:
// dim - Hamming(a, b). A higher agreement means the two vectors point in a more
// similar direction relative to their centroid, so it is the RaBitQ-lite
// prefilter score (docs/03): keep the top-M candidates by agreement, then rerank
// at full precision. dim is passed explicitly because the packed code is padded
// to a multiple of 64 and the padding bits must not count.
func Agreement(a, b []uint64, dim int) int {
	return dim - Hamming(a, b)
}
