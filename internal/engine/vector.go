package engine

import (
	"math"
	"math/bits"
	"math/rand"
	"sort"
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

// ───────────────────────── Hierarchical centroid tree (SPANN HBC) ─────────────────────────
//
// Everything above builds ONE flat level of IVF centroids: a query scores the
// query vector against EVERY centroid (O(K)) to pick the nProbe nearest. That is
// exactly right at this clone's scale (K ≈ √N, a handful of dot products). The
// code below adds the multi-level alternative — SPANN's hierarchical balanced
// clustering (HBC, SPANN §3.2.1, Figure 3) plus beam descent — so the SHAPE of
// turbopuffer's "ANN v3" wide-shallow centroid tree is implemented and testable.
//
// HONEST FRAMING (docs/extensions/hierarchical-centroid-tree.md, "What's
// genuinely hard"): a tree only reduces centroid comparisons at LARGE K, where a
// flat O(K) scan stops being cheap and the centroid set no longer fits in fast
// memory. At our hundreds-to-thousands of vectors K is tiny, the tree adds levels
// to descend, and beam descent is *equal or slower* than the flat scan — it buys
// nothing and the tests say so. The point is to show the mechanism, bound the
// fan-out, and prove the routing returns the SAME clusters a flat scan would, not
// to speed anything up at demo scale.

// CentroidNode is one node of the hierarchical centroid tree stored on the
// epoch's CentroidsFile. Internal nodes hold a representative Centroid (the mean
// of their subtree's vectors) and Children sub-nodes; a leaf holds no children
// and instead names the flat IVF cluster it routes to via LeafCluster.
//
// CRITICAL COMPATIBILITY INVARIANT: the leaves of this tree are exactly the flat
// clusters the indexer already writes (cluster-{i}.json). A leaf's LeafCluster is
// a flat cluster id, so the tree is purely an alternate ROUTER to the same
// posting lists — it does not re-bucket documents, re-encode F5 rotated RaBitQ
// codes, or change the F4 bitmap ClusterOf assignment. Build the flat index
// first, then build the tree OVER the flat centroids; query-time beam descent
// then selects a set of flat cluster ids identical (for a wide-enough beam) to
// what nearestClusters would pick, so top-K is unchanged.
type CentroidNode struct {
	Centroid    []float32      `json:"centroid"`
	Children    []CentroidNode `json:"children,omitempty"`
	LeafCluster int            `json:"leafCluster,omitempty"` // valid only when Children == nil
}

// IsLeaf reports whether this node routes directly to a flat IVF cluster.
func (n *CentroidNode) IsLeaf() bool { return len(n.Children) == 0 }

// treeItem pairs a representative vector with the flat cluster id it routes to as
// the tree recurses, so a leaf can name its cluster without a second lookup.
type treeItem struct {
	point   []float32
	cluster int
}

// buildCentroidTree is the recursive HBC builder (SPANN's hierarchical balanced
// clustering, §3.2.1, Figure 3): split a bucket with one balanced k-means pass
// and recurse on each child until a bucket holds at most capacity items, which is
// then sealed as a leaf.
//
// fanout is the per-level branching constant F. SPANN recurses with a small k per
// split, which "reduce[s] the clustering time complexity from O(|X|·m·N) to
// O(|X|·m·k·logₖ(N))"; turbopuffer's ANN v3 uses ≈100 children per node. capacity
// is the leaf cap — the analogue of SPANN's posting-length limit (§4.2): a bucket
// at or under it stops splitting. A single-item bucket, or any split that fails to
// divide the bucket into ≥2 non-trivial children, is sealed as a leaf so recursion
// always terminates. The split reuses the deterministic kMeans the flat build uses
// (threaded rng), so the tree is reproducible; metric is the namespace metric so
// internal-node distances match query-time scoring.
func buildCentroidTree(items []treeItem, fanout, capacity int, metric string, rng *rand.Rand) *CentroidNode {
	// A bucket within the leaf cap (or a single item) becomes a leaf. The leaf's
	// representative is the mean of its items so an internal parent above it can be
	// scored, but its identity is the flat cluster it routes to. A multi-item leaf
	// is collapsed to its lowest cluster id — at our scale a leaf cap >= K makes the
	// whole tree a single root leaf, which is the degenerate "flat" case and is
	// exactly the right answer when a tree buys nothing.
	if len(items) <= capacity || len(items) <= 1 {
		return newLeaf(items)
	}

	// One balanced split into <= fanout children (SPANN HBC, §3.2.1). KMeans clamps
	// k to [1, len(points)], so a bucket smaller than fanout still splits cleanly.
	points := make([][]float32, len(items))
	for i, it := range items {
		points[i] = it.point
	}
	k := fanout
	if k > len(points) {
		k = len(points)
	}
	centroids, assign := kMeans(points, k, metric, kmeansIters, rng)

	// Partition items by their assigned child bucket.
	buckets := make([][]treeItem, len(centroids))
	for i, it := range items {
		c := assign[i]
		buckets[c] = append(buckets[c], it)
	}

	// If the split failed to actually divide the bucket (every item landed in one
	// child), recursing would loop forever — seal this bucket as a leaf instead.
	nonEmpty := 0
	for _, b := range buckets {
		if len(b) > 0 {
			nonEmpty++
		}
	}
	if nonEmpty <= 1 {
		return newLeaf(items)
	}

	node := &CentroidNode{Centroid: meanVec(points)}
	for c, b := range buckets {
		if len(b) == 0 {
			continue
		}
		child := buildCentroidTree(b, fanout, capacity, metric, rng)
		// A child's representative is its subtree mean; keep the split centroid as
		// the routing key so descent scores against the partition, not the recursion.
		child.Centroid = centroids[c]
		node.Children = append(node.Children, *child)
	}
	return node
}

// newLeaf seals a bucket into a leaf node routing to the flat cluster of its
// lowest cluster id, with the bucket mean as its representative centroid. The
// lowest id is a deterministic, arbitrary pick: at this clone's scale the leaf cap
// is set so each leaf holds exactly one flat centroid, so the choice never bites;
// it exists only to keep a degenerate multi-centroid leaf well-defined.
func newLeaf(items []treeItem) *CentroidNode {
	points := make([][]float32, len(items))
	cluster := items[0].cluster
	for i, it := range items {
		points[i] = it.point
		if it.cluster < cluster {
			cluster = it.cluster
		}
	}
	return &CentroidNode{Centroid: meanVec(points), LeafCluster: cluster}
}

// meanVec returns the component-wise mean of points (the centroid of a bucket).
// An empty input returns nil; callers never pass one.
func meanVec(points [][]float32) []float32 {
	if len(points) == 0 {
		return nil
	}
	dim := len(points[0])
	sum := make([]float64, dim)
	for _, p := range points {
		for d, x := range p {
			sum[d] += float64(x)
		}
	}
	mean := make([]float32, dim)
	inv := 1.0 / float64(len(points))
	for d := range mean {
		mean[d] = float32(sum[d] * inv)
	}
	return mean
}

// BuildTree is the indexer's entry point: it wraps the flat IVF centroids (one
// treeItem per cluster, point = centroid, cluster = its index) and recurses the
// HBC split into a tree whose leaves ARE the flat clusters. It is deterministic
// (seeded like KMeans) so the published epoch stays byte-for-byte reproducible.
//
// Returns nil when there are no centroids (a text-only epoch) or when the tree
// would be a single root leaf wrapping every cluster (leaf cap >= K) — in that
// degenerate case the tree adds no routing structure over the flat scan, so we
// omit it entirely and the query path keeps doing the plain O(K) scan. That is
// the honest "buys nothing at our scale" outcome made explicit in the data.
func BuildTree(centroids [][]float32, fanout, capacity int, metric string) *CentroidNode {
	if len(centroids) == 0 {
		return nil
	}
	items := make([]treeItem, len(centroids))
	for i, c := range centroids {
		items[i] = treeItem{point: c, cluster: i}
	}
	rng := rand.New(rand.NewSource(1))
	root := buildCentroidTree(items, fanout, capacity, metric, rng)
	if root.IsLeaf() {
		// One flat leaf over all clusters: no hierarchy was created, so storing it
		// would only add an indirection that selects every cluster. Omit it.
		return nil
	}
	return root
}

// BeamDescend traverses the centroid tree top-down keeping the beamWidth nearest
// nodes at each level (a beam, SPANN §3.2.3 dynamic pruning's fixed-width cousin),
// descending into their children until it reaches the leaf level, and returns the
// flat cluster ids those leaves route to, nearest-leaf first. It also returns the
// measured fan-out — the total number of node-distance comparisons performed —
// which a query or bench can compare against the flat scan's K comparisons to SEE
// whether the tree is actually saving work (at our scale it usually is not).
//
// A wider beam selects more leaves and recovers exactly the flat ranking; a beam
// of 1 follows the single best path and may miss a cluster near a split boundary
// (the SPANN "boundary issue", §3.1) — which is precisely why the correctness
// test uses a beam wide enough to reproduce the flat top-K.
func BeamDescend(root *CentroidNode, query []float32, metric string, beamWidth int) (clusters []int, fanout int) {
	if root == nil {
		return nil, 0
	}
	if beamWidth < 1 {
		beamWidth = 1
	}

	// scored pairs a node with its distance to the query so a level's beam can be
	// ranked; idx breaks ties deterministically (mirrors nearestClusters).
	type scored struct {
		node *CentroidNode
		dist float64
	}

	// The frontier starts at the root. We never score the root itself (there is one
	// root, descending it is unconditional); fan-out counts child comparisons, the
	// work a flat scan's K comparisons are measured against.
	frontier := []*CentroidNode{root}
	for {
		// If every node in the frontier is a leaf, we have reached the leaf level.
		allLeaves := true
		for _, n := range frontier {
			if !n.IsLeaf() {
				allLeaves = false
				break
			}
		}
		if allLeaves {
			break
		}

		// Gather the children of every frontier node (a leaf in a mixed level carries
		// itself forward so it is not dropped before the leaf level is reached), score
		// each against the query, and keep the beamWidth nearest.
		var cand []scored
		for _, n := range frontier {
			if n.IsLeaf() {
				cand = append(cand, scored{node: n, dist: Distance(metric, query, n.Centroid)})
				fanout++
				continue
			}
			for i := range n.Children {
				child := &n.Children[i]
				cand = append(cand, scored{node: child, dist: Distance(metric, query, child.Centroid)})
				fanout++
			}
		}
		sort.Slice(cand, func(i, j int) bool {
			if cand[i].dist != cand[j].dist {
				return cand[i].dist < cand[j].dist
			}
			// Tie-break: leaves by cluster id, internal nodes after leaves, so the
			// order is deterministic and reproducible across runs.
			return leafKey(cand[i].node) < leafKey(cand[j].node)
		})
		if len(cand) > beamWidth {
			cand = cand[:beamWidth]
		}
		frontier = frontier[:0]
		for _, s := range cand {
			frontier = append(frontier, s.node)
		}
	}

	// The frontier is now all leaves; rank them by distance and emit their flat
	// cluster ids nearest-first, de-duplicated (distinct leaves may share a cluster
	// only in the degenerate multi-centroid-leaf case).
	leaves := make([]scored, 0, len(frontier))
	for _, n := range frontier {
		leaves = append(leaves, scored{node: n, dist: Distance(metric, query, n.Centroid)})
		fanout++
	}
	sort.Slice(leaves, func(i, j int) bool {
		if leaves[i].dist != leaves[j].dist {
			return leaves[i].dist < leaves[j].dist
		}
		return leaves[i].node.LeafCluster < leaves[j].node.LeafCluster
	})
	seen := make(map[int]bool, len(leaves))
	for _, s := range leaves {
		if seen[s.node.LeafCluster] {
			continue
		}
		seen[s.node.LeafCluster] = true
		clusters = append(clusters, s.node.LeafCluster)
	}
	return clusters, fanout
}

// leafKey returns a sort key that orders leaves by their cluster id and pushes
// internal nodes after leaves at equal distance, so BeamDescend's tie-break is
// total and deterministic.
func leafKey(n *CentroidNode) int {
	if n.IsLeaf() {
		return n.LeafCluster
	}
	return 1 << 30 // internal nodes sort after any real cluster id on a tie.
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

// ───────────────────────── True RaBitQ (Gao & Long, SIGMOD '24) ─────────────────────────
//
// Everything above is "RaBitQ-lite": a sign-bit fingerprint scored by Hamming
// agreement, an ordering heuristic with no error bound (docs/03,
// docs/extensions/true-rabitq.md). The code below implements the real method —
// a seeded random orthogonal rotation plus the paper's *unbiased* inner-product
// estimator with its O(1/√D) error bound — so the binary prefilter ranks
// candidates by an actual estimated distance rather than a sign-agreement count.
// The exact rerank afterward is unchanged; the win is that a far smaller
// shortlist preserves the same recall, because the estimate is provably close to
// the true distance (docs/extensions/true-rabitq.md, §3.2 / Theorem 3.2).
//
// Notation follows the paper's Table 2: oᵣ/qᵣ are the raw data/query vectors, c
// the IVF centroid, o = (oᵣ−c)/‖oᵣ−c‖ and q = (qᵣ−c)/‖qᵣ−c‖ the unit residuals,
// P the random orthogonal matrix, x̄ ∈ {±1/√D}^D the codebook vector, x̄_b its
// stored bit string, and ō = P·x̄ the reconstructed quantized data vector.

// bqBits is B_q, the number of bits the query residual is scalar-quantized to in
// the bitwise scan. The paper fixes B_q = 4 (§5.1) and proves B_q = Θ(log log D)
// keeps the query-quantization error in the same O(1/√D) order as the estimator
// (Theorem 3.3), so 4 is comfortably enough at this clone's dimensions.
const bqBits = 4

// Rotation is a D×D random orthogonal matrix P — the Johnson–Lindenstrauss
// transform RaBitQ applies before quantization (§3.1.2, Eq. 4). It is sampled
// once per index epoch from a fixed seed and stored on the epoch's CentroidsFile,
// so it is deterministic, immutable, and shared by index-time encoding and
// query-time estimation. Rows hold P (Apply computes P·x); because P is
// orthogonal, P⁻¹ = Pᵀ and ApplyInverse computes Pᵀ·x without a matrix inverse.
type Rotation struct {
	Dim  int         `json:"dim"`
	Rows [][]float64 `json:"rows"` // Rows[i] is the i-th row of P (length Dim)
}

// NewRotation samples a uniformly-distributed random orthogonal matrix of the
// given dimension, deterministically from seed. The construction is the standard
// recipe (docs/extensions/true-rabitq.md, "What's genuinely hard"): fill a D×D
// matrix with i.i.d. standard Gaussians and orthonormalize it with the
// (modified) Gram–Schmidt process. The subtle correctness point is uniformity:
// raw Gram–Schmidt biases the result, so we flip the sign of each orthonormal row
// to make the corresponding diagonal of the implicit R positive — the Mezzadri
// sign fix that makes P Haar-uniform over O(D). Seeding from a fixed source keeps
// the rotation reproducible, mirroring how KMeans seeds rand.NewSource(1) so the
// whole vector index stays deterministic across builds.
func NewRotation(dim int, seed int64) *Rotation {
	rng := rand.New(rand.NewSource(seed))
	// a[i] is row i of a Gaussian matrix; we orthonormalize the rows in place.
	a := make([][]float64, dim)
	for i := range a {
		a[i] = make([]float64, dim)
		for j := range a[i] {
			a[i][j] = rng.NormFloat64()
		}
	}

	// Modified Gram–Schmidt over the rows: subtract from each row its projection
	// onto every already-orthonormalized row, then normalize.
	for i := 0; i < dim; i++ {
		for k := 0; k < i; k++ {
			proj := dot64(a[i], a[k])
			for j := 0; j < dim; j++ {
				a[i][j] -= proj * a[k][j]
			}
		}
		norm := math.Sqrt(dot64(a[i], a[i]))
		if norm == 0 {
			// Degenerate (near-dependent Gaussian draw): substitute the i-th
			// standard basis vector so the result stays a valid orthonormal set.
			for j := range a[i] {
				a[i][j] = 0
			}
			a[i][i] = 1
			norm = 1
		}
		inv := 1.0 / norm
		for j := 0; j < dim; j++ {
			a[i][j] *= inv
		}
		// Mezzadri sign fix: force the leading nonzero component positive so the
		// orthonormalization corresponds to a Haar-uniform rotation rather than a
		// sign-biased one.
		if a[i][i] < 0 {
			for j := 0; j < dim; j++ {
				a[i][j] = -a[i][j]
			}
		}
	}
	return &Rotation{Dim: dim, Rows: a}
}

// Apply returns P·v (the forward rotation), used to reconstruct ō = P·x̄ at index
// time. v must have length Dim.
func (r *Rotation) Apply(v []float64) []float64 {
	out := make([]float64, r.Dim)
	for i := 0; i < r.Dim; i++ {
		out[i] = dot64(r.Rows[i], v)
	}
	return out
}

// ApplyInverse returns P⁻¹·v = Pᵀ·v, the inverse rotation that maps a residual
// into the cube's frame (Eq. 7: ⟨o, P·x̄⟩ = ⟨P⁻¹·o, x̄⟩) and inverse-rotates the
// query (Eq. 17, q' = P⁻¹q). Because P is orthogonal the transpose IS the
// inverse, so this is exact, not an approximation. v must have length Dim.
func (r *Rotation) ApplyInverse(v []float64) []float64 {
	out := make([]float64, r.Dim)
	for j := 0; j < r.Dim; j++ {
		var s float64
		for i := 0; i < r.Dim; i++ {
			s += r.Rows[i][j] * v[i]
		}
		out[j] = s
	}
	return out
}

// RaBitQCode is the True RaBitQ code stored per data vector (§3.1.3, Algorithm 1).
// Bits is the D-bit string x̄_b = sign(P⁻¹o), packed one bit per dimension like
// ResidualCode. ResidualNorm is ‖oᵣ−c‖, the per-vector scalar in the Eq. 2
// distance decomposition. OOAlign is ⟨ō, o⟩, the de-bias factor the unbiased
// estimator divides by (Eq. 13). Storing the two floats alongside D bits is the
// whole RaBitQ storage footprint (vs 32D bits raw).
type RaBitQCode struct {
	Bits         []uint64 `json:"bits"`
	ResidualNorm float64  `json:"residualNorm"` // ‖oᵣ − c‖
	OOAlign      float64  `json:"ooAlign"`      // ⟨ō, o⟩, the de-bias scalar
}

// EncodeRaBitQ computes a data vector's True RaBitQ code relative to its cluster
// centroid (§3.1.3). It normalizes the residual onto the unit sphere
// (o = (v−c)/‖v−c‖), inverse-rotates it into the cube frame, records the sign
// bits x̄_b, reconstructs the quantized vector ō = P·x̄ where x̄[i] = ±1/√D, and
// precomputes ‖v−c‖ and the alignment ⟨ō, o⟩ that the estimator needs.
//
// A residual of exactly zero (v == c) has no direction: its code is all bits set
// (sign convention matching ResidualCode's ">= 0") with ResidualNorm 0 and
// OOAlign 0. Such a point reconstructs to distance ‖q−c‖ from any query under the
// Eq. 2 decomposition, which is exactly right — it sits at the centroid.
func EncodeRaBitQ(v, centroid []float32, rot *Rotation) RaBitQCode {
	dim := len(v)
	residual := make([]float64, dim)
	var norm2 float64
	for i := 0; i < dim; i++ {
		d := float64(v[i]) - float64(centroid[i])
		residual[i] = d
		norm2 += d * d
	}
	words := (dim + 63) / 64
	code := RaBitQCode{Bits: make([]uint64, words)}
	if norm2 == 0 {
		// Zero residual: keep the ">= 0 ⇒ bit set" convention but leave the
		// scalars at zero so the estimator contributes no inner-product term.
		for i := 0; i < dim; i++ {
			code.Bits[i/64] |= 1 << uint(i%64)
		}
		return code
	}
	invNorm := 1.0 / math.Sqrt(norm2)
	o := make([]float64, dim)
	for i := 0; i < dim; i++ {
		o[i] = residual[i] * invNorm // unit residual o
	}

	// x̄_b = sign(P⁻¹o): inverse-rotate o into the cube frame and take signs.
	rotated := rot.ApplyInverse(o)
	for i := 0; i < dim; i++ {
		if rotated[i] >= 0 {
			code.Bits[i/64] |= 1 << uint(i%64)
		}
	}

	// Reconstruct x̄ ∈ {±1/√D}^D from the bits, then ō = P·x̄, and precompute the
	// alignment ⟨ō, o⟩ that de-biases the estimator.
	invSqrtD := 1.0 / math.Sqrt(float64(dim))
	xbar := make([]float64, dim)
	for i := 0; i < dim; i++ {
		if code.Bits[i/64]&(1<<uint(i%64)) != 0 {
			xbar[i] = invSqrtD
		} else {
			xbar[i] = -invSqrtD
		}
	}
	obar := rot.Apply(xbar)
	code.ResidualNorm = math.Sqrt(norm2)
	code.OOAlign = dot64(obar, o)
	return code
}

// QueryCode is the query-side companion to RaBitQCode, computed once per probed
// cluster (§3.3.1). The query residual q = (qᵣ−c)/‖qᵣ−c‖ is inverse-rotated to
// q' = P⁻¹q and scalar-quantized to B_q-bit unsigned integers, then split into
// B_q bit-planes so the inner product ⟨x̄_b, q̄_u⟩ becomes a sum of AND+popcounts
// (Eq. 21–22). Planes[j] is the packed bit string of bit j of the quantized
// query across all dimensions. ResidualNorm is ‖qᵣ−c‖, the per-query scalar in
// the Eq. 2 decomposition. Lower/Width recover the dequantized value of each
// query coordinate: q'[i] ≈ Lower + code_i·Width.
type QueryCode struct {
	Planes       [][]uint64 // Planes[j] = packed bit-plane j of the quantized query
	ResidualNorm float64    // ‖qᵣ − c‖
	Lower        float64    // min(q') — the quantizer's lower bound
	Width        float64    // (max(q') − min(q')) / (2^Bq − 1) — quantization step
	SumQuant     int        // Σ_i q̄_u[i], needed to undo the Lower offset
	Dim          int
}

// EncodeQuery builds the QueryCode for a query vector against one cluster centroid
// (§3.3.1). It normalizes and inverse-rotates the query residual, then uniformly
// scalar-quantizes q' into [0, 2^Bq) using randomized rounding (Eq. 18) so the
// quantization is unbiased, and packs the result into B_q bit-planes. The rng is
// threaded in for the randomized rounding; query-time correctness does not depend
// on the seed (the rounding is unbiased either way), but a fixed seed keeps tests
// reproducible.
func EncodeQuery(query, centroid []float32, rot *Rotation, rng *rand.Rand) QueryCode {
	dim := len(query)
	qc := QueryCode{Dim: dim, Planes: make([][]uint64, bqBits)}
	words := (dim + 63) / 64
	for j := range qc.Planes {
		qc.Planes[j] = make([]uint64, words)
	}

	residual := make([]float64, dim)
	var norm2 float64
	for i := 0; i < dim; i++ {
		d := float64(query[i]) - float64(centroid[i])
		residual[i] = d
		norm2 += d * d
	}
	if norm2 == 0 {
		return qc // query sits on the centroid: every estimated inner product is 0.
	}
	invNorm := 1.0 / math.Sqrt(norm2)
	for i := 0; i < dim; i++ {
		residual[i] *= invNorm // unit residual q
	}
	qc.ResidualNorm = math.Sqrt(norm2)

	qprime := rot.ApplyInverse(residual) // q' = P⁻¹q

	lo, hi := qprime[0], qprime[0]
	for _, x := range qprime {
		if x < lo {
			lo = x
		}
		if x > hi {
			hi = x
		}
	}
	levels := float64((uint(1) << bqBits) - 1) // 2^Bq − 1
	width := (hi - lo) / levels
	qc.Lower = lo
	qc.Width = width
	if width == 0 {
		// All coordinates equal (e.g. a 1-dim or constant residual): quantize to 0.
		return qc
	}

	for i := 0; i < dim; i++ {
		// Randomized rounding (Eq. 18): round up with probability equal to the
		// fractional part, keeping E[q̄_u] = (q'−lo)/width and the step unbiased.
		t := (qprime[i] - lo) / width
		f := math.Floor(t)
		level := int(f)
		if rng.Float64() < (t - f) {
			level++
		}
		if level < 0 {
			level = 0
		}
		maxLevel := int(levels)
		if level > maxLevel {
			level = maxLevel
		}
		qc.SumQuant += level
		// Scatter the level's bits across the bit-planes.
		for j := 0; j < bqBits; j++ {
			if level&(1<<uint(j)) != 0 {
				qc.Planes[j][i/64] |= 1 << uint(i%64)
			}
		}
	}
	return qc
}

// EstimateInnerProduct estimates ⟨q, o⟩ — the only unknown scalar in the Eq. 2
// distance decomposition — from a data vector's RaBitQCode and a cluster's
// QueryCode, using the paper's unbiased estimator (Eq. 13, Theorem 3.2):
//
//	est⟨q, o⟩ = ⟨ō, q⟩ / ⟨ō, o⟩
//
// The numerator ⟨ō, q⟩ = ⟨x̄, q'⟩ is recovered from the bitwise scan: with x̄_b the
// stored bits and q̄_u the quantized query, ⟨x̄_b, q̄_u⟩ = Σⱼ 2ʲ·popcount(x̄_b AND
// q̄_u^(j)) (Eq. 22). The dequantization q'[i] ≈ Lower + Width·q̄_u[i] and the
// reconstruction x̄[i] = (2·x̄_b[i]−1)/√D turn that bit count into ⟨x̄, q'⟩, and
// dividing by the precomputed OOAlign = ⟨ō, o⟩ removes the bias. A zero-norm code
// (a point at the centroid) has no direction, so its estimate is 0.
func EstimateInnerProduct(code RaBitQCode, qc QueryCode) float64 {
	if code.OOAlign == 0 {
		return 0
	}
	dim := qc.Dim
	invSqrtD := 1.0 / math.Sqrt(float64(dim))

	// popXBar = popcount(x̄_b) = number of +1/√D coordinates in x̄.
	popXBar := 0
	for _, w := range code.Bits {
		popXBar += bits.OnesCount64(w)
	}

	// ⟨x̄_b, q̄_u⟩ = Σⱼ 2ʲ·popcount(x̄_b AND plane_j) — the integer dot of the bit
	// string against the quantized query (Eq. 22).
	var bitDot int
	for j := 0; j < len(qc.Planes); j++ {
		var pc int
		for w := range code.Bits {
			pc += bits.OnesCount64(code.Bits[w] & qc.Planes[j][w])
		}
		bitDot += pc << uint(j)
	}

	// ⟨x̄, q'⟩ where x̄[i] = (2·x̄_b[i]−1)/√D and q'[i] = Lower + Width·q̄_u[i].
	//   Σ_i x̄[i]·q'[i]
	// = (1/√D)·Σ_i (2·x̄_b[i]−1)·(Lower + Width·q̄_u[i])
	// = (1/√D)·[ 2·Width·⟨x̄_b,q̄_u⟩ + 2·Lower·popcount(x̄_b)
	//            − Width·SumQuant − Lower·D ].
	obarq := invSqrtD * (2*qc.Width*float64(bitDot) +
		2*qc.Lower*float64(popXBar) -
		qc.Width*float64(qc.SumQuant) -
		qc.Lower*float64(dim))

	return obarq / code.OOAlign
}

// EstimateDistance turns the estimated inner product into an estimated distance
// under the namespace metric, completing the Eq. 1–2 decomposition. For
// euclidean it is the squared-distance identity written with the normalized
// residuals:
//
//	‖oᵣ − qᵣ‖² = ‖oᵣ−c‖² + ‖qᵣ−c‖² − 2·‖oᵣ−c‖·‖qᵣ−c‖·⟨q,o⟩
//
// returned as a true L2 distance (sqrt, clamped at 0). For cosine the prefilter
// only needs a *monotonic* ranking score, and a larger ⟨q,o⟩ means a smaller
// cosine distance, so we return 1 − est⟨q,o⟩ as a cosine-distance proxy. Either
// way the value is only used to order the shortlist; the surviving candidates are
// always reranked at full precision, so the estimate never decides a final result
// on its own.
func EstimateDistance(metric string, code RaBitQCode, qc QueryCode) float64 {
	ip := EstimateInnerProduct(code, qc)
	if metric == MetricCosine {
		// Both residuals are unit vectors, so ⟨q,o⟩ is already the cosine of the
		// angle between the residual directions — a monotone stand-in for the
		// full-vector cosine ranking used by the exact rerank.
		return 1 - ip
	}
	on := code.ResidualNorm
	qn := qc.ResidualNorm
	sq := on*on + qn*qn - 2*on*qn*ip
	if sq < 0 {
		sq = 0 // floating-point / estimator slack can push a near-zero negative.
	}
	return math.Sqrt(sq)
}

// dot64 is the float64 dot product of two equal-length vectors, the inner loop of
// the rotation and the alignment scalar.
func dot64(a, b []float64) float64 {
	var s float64
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}
