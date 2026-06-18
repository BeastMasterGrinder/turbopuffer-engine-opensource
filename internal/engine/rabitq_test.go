package engine

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// orthoEps bounds the floating-point slack we tolerate when checking that the
// sampled rotation is orthonormal (PᵀP = I) and that ApplyInverse undoes Apply.
const orthoEps = 1e-9

// TestRotationOrthonormal verifies NewRotation produces a true orthogonal matrix:
// every row is unit length and any two distinct rows are orthogonal, so PᵀP = I.
// Without this, the rotation is not distance-preserving and the estimator's
// reduction ⟨o, P·x̄⟩ = ⟨P⁻¹·o, x̄⟩ (which assumes P⁻¹ = Pᵀ) would be wrong.
func TestRotationOrthonormal(t *testing.T) {
	t.Parallel()
	for _, dim := range []int{1, 2, 4, 8, 16, 32} {
		rot := NewRotation(dim, 42)
		for i := 0; i < dim; i++ {
			for j := 0; j < dim; j++ {
				got := dot64(rot.Rows[i], rot.Rows[j])
				want := 0.0
				if i == j {
					want = 1.0
				}
				if math.Abs(got-want) > orthoEps {
					t.Errorf("dim=%d: <row %d, row %d> = %v, want %v", dim, i, j, got, want)
				}
			}
		}
	}
}

// TestRotationInverseRoundTrip checks ApplyInverse(Apply(v)) == v (and the
// reverse), the property the estimator relies on: encoding inverse-rotates the
// residual and querying does too, so the two frames must agree exactly.
func TestRotationInverseRoundTrip(t *testing.T) {
	t.Parallel()
	dim := 16
	rot := NewRotation(dim, 7)
	rng := rand.New(rand.NewSource(99))
	v := make([]float64, dim)
	for i := range v {
		v[i] = rng.NormFloat64()
	}
	back := rot.ApplyInverse(rot.Apply(v))
	for i := range v {
		if math.Abs(back[i]-v[i]) > orthoEps {
			t.Errorf("round-trip[%d] = %v, want %v", i, back[i], v[i])
		}
	}
	// A rotation preserves length: ‖P·v‖ == ‖v‖.
	var nv, nr float64
	rv := rot.Apply(v)
	for i := range v {
		nv += v[i] * v[i]
		nr += rv[i] * rv[i]
	}
	if math.Abs(math.Sqrt(nv)-math.Sqrt(nr)) > orthoEps {
		t.Errorf("rotation changed norm: ‖v‖=%v ‖P·v‖=%v", math.Sqrt(nv), math.Sqrt(nr))
	}
}

// TestRotationDeterministic locks in reproducibility: the same dimension and seed
// must yield the identical matrix (so an epoch rebuilt from the same data has the
// same rotation and codes), while a different seed must yield a different one.
func TestRotationDeterministic(t *testing.T) {
	t.Parallel()
	a := NewRotation(8, 123)
	b := NewRotation(8, 123)
	for i := range a.Rows {
		for j := range a.Rows[i] {
			if a.Rows[i][j] != b.Rows[i][j] {
				t.Fatalf("same seed produced different rotations at [%d][%d]: %v vs %v", i, j, a.Rows[i][j], b.Rows[i][j])
			}
		}
	}
	c := NewRotation(8, 124)
	same := true
	for i := range a.Rows {
		for j := range a.Rows[i] {
			if a.Rows[i][j] != c.Rows[i][j] {
				same = false
			}
		}
	}
	if same {
		t.Errorf("different seeds produced identical rotations; expected them to differ")
	}
}

// TestEstimatorUnbiasedAndBounded is the core correctness test for True RaBitQ:
// over a synthetic set it checks the unbiased inner-product estimator against the
// brute-force exact ⟨q, o⟩.
//
//   - Unbiasedness (Theorem 3.2): the MEAN signed error over many random data
//     vectors must be ~0 — far smaller than any individual error.
//   - Error bound (Eq. 15): each estimate must land within an O(1/√D) envelope of
//     the truth. We use a generous constant on 1/√D as the high-probability
//     ceiling (the paper fixes ε₀=1.9; we test the bound holds, not the constant).
func TestEstimatorUnbiasedAndBounded(t *testing.T) {
	t.Parallel()
	dim := 128
	rot := NewRotation(dim, 1)
	rng := rand.New(rand.NewSource(2024))

	// A fixed centroid and a fixed query; we vary the data vector.
	centroid := randGaussVec(rng, dim)
	queryVec := randGaussVec(rng, dim)

	// The exact target is ⟨q, o⟩ of the UNIT residuals (the only term the
	// estimator approximates). Build the query code once.
	qResidual, qNorm := unitResidual(queryVec, centroid)
	_ = qNorm
	qc := EncodeQuery(queryVec, centroid, rot, rand.New(rand.NewSource(5)))

	const trials = 400
	// errBound is the high-probability envelope; with D=128, 1/√D ≈ 0.088, so a
	// constant of 5 gives ~0.44 — comfortably above the estimator's spread while
	// still being far tighter than the [-1,1] range of an inner product.
	errBound := 5.0 / math.Sqrt(float64(dim))

	var sumErr, maxAbsErr float64
	for i := 0; i < trials; i++ {
		dataVec := randGaussVec(rng, dim)
		oResidual, _ := unitResidual(dataVec, centroid)
		exact := dot64(qResidual, oResidual)

		code := EncodeRaBitQ(dataVec, centroid, rot)
		est := EstimateInnerProduct(code, qc)

		err := est - exact
		sumErr += err
		if a := math.Abs(err); a > maxAbsErr {
			maxAbsErr = a
		}
		if math.Abs(err) > errBound {
			t.Errorf("trial %d: |est-exact| = %v exceeds O(1/√D) bound %v (est=%v exact=%v)", i, math.Abs(err), errBound, est, exact)
		}
	}
	meanErr := sumErr / trials
	// Unbiasedness: the mean error must be an order of magnitude below the typical
	// per-estimate error, confirming errors cancel rather than systematically skew.
	if math.Abs(meanErr) > maxAbsErr/5 {
		t.Errorf("estimator looks biased: mean error %v is not << max abs error %v", meanErr, maxAbsErr)
	}
	t.Logf("D=%d trials=%d: meanErr=%.5f maxAbsErr=%.5f bound=%.5f", dim, trials, meanErr, maxAbsErr, errBound)
}

// TestEstimateRanksBetterThanLite confirms the property the prefilter relies on:
// ordering candidates by the True RaBitQ estimated distance agrees with the exact
// ordering MORE OFTEN than ordering by the lite sign-bit agreement does. The
// prefilter never returns the estimate as a result — it only uses it to pick which
// candidates to rerank — so what matters is rank fidelity relative to the heuristic
// it replaces. We measure pairwise concordance (the fraction of candidate pairs
// whose predicted ordering matches the exact ordering, Kendall-style) for both
// scorers over the same candidates; True RaBitQ must win, decisively.
func TestEstimateRanksBetterThanLite(t *testing.T) {
	t.Parallel()
	dim := 64
	rot := NewRotation(dim, 3)
	rng := rand.New(rand.NewSource(11))
	centroid := randGaussVec(rng, dim)
	queryVec := randGaussVec(rng, dim)

	for _, metric := range []string{MetricEuclidean, MetricCosine} {
		qc := EncodeQuery(queryVec, centroid, rot, rand.New(rand.NewSource(8)))
		liteQ := ResidualCode(queryVec, centroid)

		const n = 150
		type pt struct {
			estScore  float64 // True RaBitQ estimated distance (lower = nearer)
			liteScore float64 // negated lite agreement (lower = nearer)
			exact     float64
		}
		pts := make([]pt, n)
		for i := 0; i < n; i++ {
			dataVec := randGaussVec(rng, dim)
			code := EncodeRaBitQ(dataVec, centroid, rot)
			lite := ResidualCode(dataVec, centroid)
			pts[i] = pt{
				estScore:  EstimateDistance(metric, code, qc),
				liteScore: -float64(Agreement(liteQ, lite, dim)),
				exact:     Distance(metric, queryVec, dataVec),
			}
		}

		concordance := func(score func(pt) float64) float64 {
			c, total := 0, 0
			for i := 0; i < n; i++ {
				for j := i + 1; j < n; j++ {
					total++
					if (score(pts[i]) < score(pts[j])) == (pts[i].exact < pts[j].exact) {
						c++
					}
				}
			}
			return float64(c) / float64(total)
		}
		estAgree := concordance(func(p pt) float64 { return p.estScore })
		liteAgree := concordance(func(p pt) float64 { return p.liteScore })
		t.Logf("metric=%s rank agreement: True RaBitQ=%.4f  lite=%.4f", metric, estAgree, liteAgree)

		if estAgree <= liteAgree {
			t.Errorf("metric=%s: True RaBitQ rank agreement %.3f should exceed lite %.3f", metric, estAgree, liteAgree)
		}
		// The estimator should also be well above a coin flip (0.5) in absolute
		// terms. Cosine over raw vectors differs from the residual-direction the
		// estimator scores, so its concordance sits a little below euclidean's; both
		// stay comfortably above chance.
		if estAgree < 0.6 {
			t.Errorf("metric=%s: True RaBitQ rank agreement %.3f too low; expected >=0.60", metric, estAgree)
		}
	}
}

// TestEncodeRaBitQZeroResidual covers the degenerate v == centroid case: no
// direction, so the scalars are zero and the estimate is zero (the point sits at
// the centroid). This must not panic or NaN.
func TestEncodeRaBitQZeroResidual(t *testing.T) {
	t.Parallel()
	dim := 8
	rot := NewRotation(dim, 1)
	v := make([]float32, dim)
	code := EncodeRaBitQ(v, v, rot)
	if code.ResidualNorm != 0 || code.OOAlign != 0 {
		t.Errorf("zero residual: ResidualNorm=%v OOAlign=%v, want both 0", code.ResidualNorm, code.OOAlign)
	}
	qc := EncodeQuery(randGaussVec(rand.New(rand.NewSource(1)), dim), v, rot, rand.New(rand.NewSource(1)))
	if got := EstimateInnerProduct(code, qc); got != 0 {
		t.Errorf("estimate for centroid point = %v, want 0", got)
	}
}

// TestRaBitQRecallVsLite is the end-to-end win: over a real namespace built and
// queried through storage.New(), True RaBitQ achieves at least the same recall@k
// as the lite sign-bit prefilter while keeping a SMALLER shortlist. We measure
// recall as "did the exact nearest neighbor survive the binary prefilter into the
// reranked shortlist", at a tight shortlist size where the lite heuristic starts
// to drop true neighbors.
func TestRaBitQRecallVsLite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dim := 64
	nDocs := 600
	rng := rand.New(rand.NewSource(2025))

	// Build the corpus once and a set of queries.
	docs := make([]Document, nDocs)
	vecs := make([][]float32, nDocs)
	for i := 0; i < nDocs; i++ {
		v := randGaussVec(rng, dim)
		vecs[i] = v
		docs[i] = Document{ID: docID(i), Vector: v}
	}
	queries := make([][]float32, 40)
	for i := range queries {
		queries[i] = randGaussVec(rng, dim)
	}

	// Ground truth: brute-force exact nearest neighbor per query.
	metric := MetricEuclidean
	truth := make([]string, len(queries))
	for qi, q := range queries {
		best, bestD := "", math.Inf(1)
		for i, v := range vecs {
			if d := Distance(metric, q, v); d < bestD {
				best, bestD = docID(i), d
			}
		}
		truth[qi] = best
	}

	// Index the namespace (this stores the rotation + True RaBitQ codes).
	store := setupNS(ctx, t, NamespaceConfig{Dimension: dim, Metric: metric}, nil)
	for start := 0; start < nDocs; start += 100 {
		end := start + 100
		if end > nDocs {
			end = nDocs
		}
		seedTail(ctx, t, store, [][]Document{docs[start:end]})
	}
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: %v", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}

	// We measure the PREFILTER's recall: of the queries whose exact nearest
	// neighbor is reachable (lands in one of the nProbe probed clusters), how many
	// keep it in the top-`shortlist` after the binary scan. Restricting to
	// reachable queries isolates the prefilter's ranking quality from the orthogonal
	// IVF probe-coverage factor, so the number reflects exactly what True RaBitQ
	// changes. nProbe is large so the reachable set is most queries.
	nProbe := 12
	reachable := 0
	for qi, q := range queries {
		// A neighbor is reachable iff a full (shortlist = all members) scan of the
		// probed clusters contains it — i.e. it is in a probed cluster at all.
		if shortlistByRaBitQ(ctx, t, store, m, q, nProbe, nDocs, true)[truth[qi]] {
			reachable++
		}
	}

	// Sweep shortlist sizes. The win: True RaBitQ holds high recall down to a far
	// SMALLER shortlist than lite, and converges to ~1.0 (no-bug check) as the
	// shortlist grows — confirming the only thing it changes is how few candidates
	// you must rerank to keep the true neighbor.
	type row struct {
		shortlist          int
		rabitqRec, liteRec float64
	}
	var rows []row
	for _, shortlist := range []int{3, 5, 10, 25} {
		rabitqHits, liteHits := 0, 0
		for qi, q := range queries {
			if shortlistByRaBitQ(ctx, t, store, m, q, nProbe, shortlist, true)[truth[qi]] {
				rabitqHits++
			}
			if shortlistByRaBitQ(ctx, t, store, m, q, nProbe, shortlist, false)[truth[qi]] {
				liteHits++
			}
		}
		denom := float64(reachable)
		if denom == 0 {
			t.Fatal("no reachable queries; IVF probe coverage is zero (test setup bug)")
		}
		rows = append(rows, row{shortlist, float64(rabitqHits) / denom, float64(liteHits) / denom})
	}

	t.Logf("reachable queries=%d/%d nProbe=%d", reachable, len(queries), nProbe)
	for _, r := range rows {
		t.Logf("  shortlist=%2d: True RaBitQ recall=%.2f  lite recall=%.2f", r.shortlist, r.rabitqRec, r.liteRec)
		// At every shortlist size the estimator must rank at least as well as the
		// lite sign-bit heuristic.
		if r.rabitqRec < r.liteRec-1e-9 {
			t.Errorf("shortlist=%d: True RaBitQ recall %.2f BELOW lite recall %.2f; estimator should never rank worse", r.shortlist, r.rabitqRec, r.liteRec)
		}
	}

	// Headline win #1: at the tightest shortlist the estimator must beat lite by a
	// clear margin — the whole point is that True RaBitQ keeps the true neighbor in
	// a far SMALLER shortlist, so at fixed recall it needs to rerank fewer vectors.
	tight := rows[0]
	if tight.rabitqRec < tight.liteRec+0.15 {
		t.Errorf("shortlist=%d: expected True RaBitQ recall (%.2f) to beat lite (%.2f) by >=0.15", tight.shortlist, tight.rabitqRec, tight.liteRec)
	}

	// Headline win #2 (the shortlist-at-fixed-recall claim, stated directly): True
	// RaBitQ at a MID shortlist already matches-or-beats lite at the WIDEST shortlist.
	// In other words True RaBitQ reaches the same recall while reranking fewer than
	// half as many candidates — the exact "smaller shortlist for the same recall"
	// win. Comparing a fixed mid size to a fixed wide size (rather than searching for
	// a crossover) keeps the assertion robust to the small run-to-run recall jitter
	// from map-iteration order in the IVF build, since True RaBitQ dominates lite at
	// every size within any single run.
	mid := rows[2]  // shortlist 10
	wide := rows[3] // shortlist 25
	t.Logf("True RaBitQ@%d recall=%.2f vs lite@%d recall=%.2f (smaller shortlist, same-or-better recall)", mid.shortlist, mid.rabitqRec, wide.shortlist, wide.liteRec)
	if mid.rabitqRec < wide.liteRec {
		t.Errorf("True RaBitQ@%d recall %.2f should match-or-beat lite@%d recall %.2f at <half the shortlist", mid.shortlist, mid.rabitqRec, wide.shortlist, wide.liteRec)
	}

	// Recall must also climb monotonically with shortlist size for True RaBitQ —
	// confirming the ordering is sound (a broken scorer would not improve as you
	// admit more of its top-ranked candidates).
	for i := 1; i < len(rows); i++ {
		if rows[i].rabitqRec < rows[i-1].rabitqRec-1e-9 {
			t.Errorf("True RaBitQ recall dropped as shortlist grew: %.2f at %d then %.2f at %d", rows[i-1].rabitqRec, rows[i-1].shortlist, rows[i].rabitqRec, rows[i].shortlist)
		}
	}
}

// shortlistByRaBitQ replays the prefilter over the live epoch for one query and
// returns the set of ids that survive into the top-`limit` shortlist, scored
// either by the True RaBitQ estimator (rabitq=true) or the lite Hamming agreement
// (rabitq=false). It deliberately mirrors vectorCandidatesFromIndex's scoring so
// the test exercises the real codes, not a reimplementation of the math.
func shortlistByRaBitQ(ctx context.Context, t *testing.T, store *cache.Store, m Manifest, query []float32, nProbe, limit int, rabitq bool) map[string]bool {
	t.Helper()
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, m.IndexEpoch), &cf)
	probes := nearestClusters(m.Metric, query, cf.Centroids, nProbe)

	type scored struct {
		id    string
		score float64 // lower = better
	}
	var all []scored
	qrng := rand.New(rand.NewSource(rotationSeed))
	for _, c := range probes {
		var clf ClusterFile
		loadJSON(ctx, t, store, clusterKey(testNS, m.IndexEpoch, c), &clf)
		if rabitq {
			qc := EncodeQuery(query, clf.Centroid, cf.Rotation, qrng)
			for _, mem := range clf.Members {
				all = append(all, scored{id: mem.ID, score: EstimateDistance(m.Metric, *mem.RaBitQ, qc)})
			}
		} else {
			qCode := ResidualCode(query, clf.Centroid)
			for _, mem := range clf.Members {
				all = append(all, scored{id: mem.ID, score: -float64(Agreement(qCode, mem.Code, cf.Dimension))})
			}
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].score != all[j].score {
			return all[i].score < all[j].score
		}
		return all[i].id < all[j].id
	})
	if len(all) > limit {
		all = all[:limit]
	}
	out := make(map[string]bool, len(all))
	for _, s := range all {
		out[s.id] = true
	}
	return out
}

// ── small test helpers ──

// randGaussVec draws a vector of i.i.d. standard Gaussians, so directions spread
// evenly over the sphere — the regime the estimator's error bound is stated for
// (unlike the uniform-[0,1) randVec in planner_test.go, which clusters in one
// orthant).
func randGaussVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

// unitResidual returns the unit-normalized residual (v-c)/‖v-c‖ as float64 and
// its norm, the same quantity EncodeRaBitQ/EncodeQuery normalize to.
func unitResidual(v, c []float32) ([]float64, float64) {
	dim := len(v)
	r := make([]float64, dim)
	var n2 float64
	for i := 0; i < dim; i++ {
		d := float64(v[i]) - float64(c[i])
		r[i] = d
		n2 += d * d
	}
	if n2 == 0 {
		return r, 0
	}
	inv := 1.0 / math.Sqrt(n2)
	for i := range r {
		r[i] *= inv
	}
	return r, math.Sqrt(n2)
}

func docID(i int) string {
	return "doc-" + padInt(i)
}

func padInt(i int) string {
	s := []byte{'0', '0', '0', '0'}
	n := i
	pos := len(s) - 1
	for n > 0 && pos >= 0 {
		s[pos] = byte('0' + n%10)
		n /= 10
		pos--
	}
	return string(s)
}
