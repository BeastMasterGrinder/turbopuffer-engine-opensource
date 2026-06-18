package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// Query defaults. NProbe is how many IVF clusters a vector query scans; 3 is the
// small-scale recall/cost tradeoff from docs/02. TopK is the result count. Both
// are filled in when a caller leaves them at zero.
const (
	defaultNProbe = 3
	defaultTopK   = 10
)

// shortlistMultiplier sizes the RaBitQ-lite prefilter shortlist relative to
// TopK: we keep the top (TopK * multiplier) cluster members by sign-bit
// agreement, then rerank just those at full precision. A multiplier > 1 leaves
// headroom so the cheap 1-bit prefilter rarely drops a true nearest neighbor
// before the exact rerank sees it (docs/03).
const shortlistMultiplier = 4

// rrfK is the rank constant in Reciprocal Rank Fusion, score = Σ 1/(k+rank).
// Cormack, Clarke & Büttcher (SIGIR 2009) fixed k=60 during a pilot and never
// retuned it; their Table 1 sweep (k=0..500) barely moves MAP, so the method is
// insensitive to k — Elasticsearch's rrf retriever defaults to 60 for the same
// reason (docs/extensions/hybrid-fusion.md).
const rrfK = 60

// hybridShortlistMultiplier deepens each leg's candidate window before fusion.
// Fusing only the final TopK of each list would drop a doc ranked just past TopK
// by one signal but near the top by the other, losing its contribution from the
// list it fell out of. We fetch TopK * this many from each leg so the two
// orderings overlap enough for RRF to work with (docs/extensions/hybrid-fusion.md,
// "Window depth drives recall"). The cost is a larger scan, not extra S3
// round-trips: both legs share the same cached epoch.
const hybridShortlistMultiplier = 5

// RunQuery answers a query against a namespace using the two-mode planner from
// docs/06. It loads the manifest fresh (never cached), validates the request
// against the namespace config, then dispatches to the vector or BM25 path.
//
// Both paths obey correctness rule 5 (last-writer-wins, end to end): they read
// the live indexed epoch (if any), then overlay the unindexed WAL tail
// [IndexedUpTo, WALSeq) so a freshly upserted document beats its older indexed
// copy, and subtract tombstones written into that tail so a delete hides an id
// the indexed epoch still carries. When IndexEpoch == 0 the query serves purely
// from the tail [0, WALSeq) — the query-before-index headline.
func RunQuery(ctx context.Context, store *cache.Store, ns string, p QueryParams) ([]QueryResult, error) {
	m, _, err := LoadManifest(ctx, store, ns)
	if err != nil {
		return nil, fmt.Errorf("querying %q: %w", ns, err)
	}

	vec, text := p.RankBy.IsVector(), p.RankBy.IsText()
	if !vec && !text {
		return nil, fmt.Errorf("querying %q: no rank mode set, set a vector query, a text query, or both for hybrid", ns)
	}

	topK := p.TopK
	if topK <= 0 {
		topK = defaultTopK
	}

	// Resolve the read plan once: for a root namespace this is a single WAL
	// source over its own prefix and its own epoch (the fast path, unchanged);
	// for a copy-on-write branch it folds in the parent chain's inherited WAL and
	// targets the parent's frozen index epoch until the branch reindexes
	// (docs/extensions/branches-copy-on-write.md). Threading one resolved view
	// into both legs also keeps a hybrid query's two retrievals consistent.
	v, err := resolveReadView(ctx, store, ns, m)
	if err != nil {
		return nil, fmt.Errorf("querying %q: %w", ns, err)
	}

	switch {
	case vec && text:
		// Both signals set: run each retrieval and fuse the two rankings. The
		// same manifest snapshot m is threaded into both legs so a concurrent
		// index swap cannot tear the read between them (docs/extensions/hybrid-fusion.md).
		return runHybridQuery(ctx, store, ns, m, v, p, topK)
	case vec:
		return runVectorQuery(ctx, store, ns, m, v, p, topK)
	default:
		return runBM25Query(ctx, store, ns, m, v, p, topK)
	}
}

// runHybridQuery answers a query that carries both a vector and text by running
// the two existing retrieval paths unchanged and fusing their ranked lists with
// Reciprocal Rank Fusion (RRF). Each leg already overlays the unindexed WAL tail
// and subtracts tombstones, so freshness and last-writer-wins (correctness rule
// 5) hold on both legs for free; fusion is pure post-processing over two reads of
// the same live epoch + tail, touching no storage, manifest, or epoch code.
//
// We fetch a deeper candidate window than the final TopK from each leg so a
// document strong in one signal but just past TopK in the other still appears in
// both lists and keeps both RRF terms (docs/extensions/hybrid-fusion.md).
func runHybridQuery(ctx context.Context, store *cache.Store, ns string, m Manifest, v readView, p QueryParams, topK int) ([]QueryResult, error) {
	window := topK * hybridShortlistMultiplier

	vecResults, err := runVectorQuery(ctx, store, ns, m, v, p, window)
	if err != nil {
		return nil, err
	}
	textResults, err := runBM25Query(ctx, store, ns, m, v, p, window)
	if err != nil {
		return nil, err
	}

	return fuseRRF(vecResults, textResults, topK), nil
}

// fuseRRF combines two already-sorted ranked lists into one ordering by
// Reciprocal Rank Fusion: each document's fused score is Σ 1/(k + rank) summed
// over the lists it appears in, with k = rrfK and rank the document's 1-based
// position in that list. A document missing from a list contributes nothing for
// it (no penalty), so a hit that ranks consistently high in BOTH lists beats one
// that is #1 in only one — the behavior hybrid search wants
// (docs/extensions/hybrid-fusion.md).
//
// The input lists are assumed sorted best-first (vector ascending $dist, BM25
// descending $score) with ties already broken on id by each leg, so the position
// in the slice is a stable rank. We preserve each leg's raw $dist and $score on
// the fused result for transparency, then sort by the fused $rrf descending
// (ties broken on id) and truncate to topK.
func fuseRRF(vecResults, textResults []QueryResult, topK int) []QueryResult {
	fused := make(map[string]*QueryResult)

	get := func(r QueryResult) *QueryResult {
		f, ok := fused[r.ID]
		if !ok {
			f = &QueryResult{ID: r.ID, Attributes: r.Attributes}
			fused[r.ID] = f
		}
		return f
	}

	for i, r := range vecResults {
		f := get(r)
		f.Dist = r.Dist                    // keep the raw distance for transparency
		f.RRF += 1.0 / float64(rrfK+(i+1)) // rank is 1-based
		if f.Attributes == nil {
			f.Attributes = r.Attributes
		}
	}
	for i, r := range textResults {
		f := get(r)
		f.Score = r.Score                  // keep the raw BM25 score for transparency
		f.RRF += 1.0 / float64(rrfK+(i+1)) // rank is 1-based
		if f.Attributes == nil {
			f.Attributes = r.Attributes
		}
	}

	results := make([]QueryResult, 0, len(fused))
	for _, f := range fused {
		results = append(results, *f)
	}

	// Highest fused score first; break ties on id so the order is deterministic
	// and matches the determinism the two legs already guarantee.
	sort.Slice(results, func(i, j int) bool {
		if results[i].RRF != results[j].RRF {
			return results[i].RRF > results[j].RRF
		}
		return results[i].ID < results[j].ID
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results
}

// runVectorQuery ranks documents by distance to the query vector. It gathers
// candidates from the live IVF index (NProbe nearest clusters, RaBitQ-lite
// agreement prefilter, then exact rerank), overlays the full-precision WAL tail
// so newer writes win, subtracts tombstones, applies the filter, and returns the
// TopK nearest (ascending $dist).
func runVectorQuery(ctx context.Context, store *cache.Store, ns string, m Manifest, v readView, p QueryParams, topK int) ([]QueryResult, error) {
	if len(p.RankBy.Vector) != m.Dimension {
		return nil, fmt.Errorf("querying %q: query vector has dimension %d, namespace dimension is %d", ns, len(p.RankBy.Vector), m.Dimension)
	}

	nProbe := p.NProbe
	if nProbe <= 0 {
		nProbe = defaultNProbe
	}

	// candidate distance per id; the tail overlay overwrites indexed entries so
	// the newest copy of an id wins (rule 5).
	dists := make(map[string]float64)
	attrs := make(map[string]map[string]any)

	// indexNS/indexEpoch come from the resolved view: a root namespace reads its
	// own epoch under its own prefix; a branch reads the PARENT's frozen epoch
	// (immutable, so cache-safe) until it builds its own
	// (docs/extensions/branches-copy-on-write.md).
	if v.indexEpoch > 0 {
		// Compile the filter against the epoch's bitmap attribute index, then let
		// the planner choose filter-first vs search-first. When the filter is not
		// fully bitmap-expressible (an unindexed field, or no index at all) the plan
		// is a no-op pass-through and gathering proceeds exactly as before — so the
		// per-candidate Filter.Match below remains the single source of truth and
		// results are identical with or without the bitmaps
		// (docs/extensions/bitmap-attribute-indexes.md).
		plan, err := planVectorQuery(ctx, store, v.indexNS, v.indexEpoch, p.Filter)
		if err != nil {
			return nil, fmt.Errorf("querying %q: %w", ns, err)
		}
		if err := vectorCandidatesFromIndex(ctx, store, v.indexNS, v.indexEpoch, m.Metric, p.RankBy.Vector, nProbe, topK, plan, dists, attrs); err != nil {
			return nil, fmt.Errorf("querying %q: %w", ns, err)
		}
	}

	// Overlay the unindexed tail at full precision, folding the branch chain when
	// this is a branch (MaterializeView reduces to the single-prefix scan for a
	// root). A newer upsert overwrites the indexed distance; a tombstone removes
	// the id entirely — across the fork boundary too.
	live, deleted, err := MaterializeView(ctx, store, v, m.IndexedUpTo, m.WALSeq)
	if err != nil {
		return nil, fmt.Errorf("querying %q: scanning wal tail [%d,%d): %w", ns, m.IndexedUpTo, m.WALSeq, err)
	}
	for id, d := range live {
		if d.Vector == nil {
			// A text-only re-upsert still shadows any indexed vector hit for this
			// id: the newest version of the document has no vector, so it is not a
			// vector result.
			delete(dists, id)
			delete(attrs, id)
			continue
		}
		dists[id] = Distance(m.Metric, p.RankBy.Vector, d.Vector)
		attrs[id] = d.Attributes
	}
	for id := range deleted {
		delete(dists, id)
		delete(attrs, id)
	}

	results := make([]QueryResult, 0, len(dists))
	for id, dist := range dists {
		if !p.Filter.Match(attrs[id]) {
			continue
		}
		results = append(results, QueryResult{ID: id, Dist: dist, Attributes: attrs[id]})
	}

	// Nearest first; break ties on id so identical distances are deterministic.
	sort.Slice(results, func(i, j int) bool {
		if results[i].Dist != results[j].Dist {
			return results[i].Dist < results[j].Dist
		}
		return results[i].ID < results[j].ID
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

// filterFirstThreshold is the selectivity below which the planner flips from
// search-first to filter-first. When the filter matches fewer than this fraction
// of all indexed documents, scanning the few matches directly and scoring them is
// both cheaper and recall-safe — the post-filtering trap (a selective filter
// wiping out a vector shortlist before the filter runs) cannot bite if we never
// shortlist past the filter in the first place. The conservative v1 from the doc:
// always prune in search-first, and only flip to filter-first below a hard
// threshold (docs/extensions/bitmap-attribute-indexes.md).
const filterFirstThreshold = 0.05

// vectorPlan is the planner's decision for one indexed vector query, derived from
// the epoch's bitmap attribute index. A zero vectorPlan (the value when there is
// no bitmap index or the filter is not bitmap-expressible) means "no plan": gather
// candidates exactly as the pre-bitmap engine did. The plan never changes WHICH
// documents pass the filter — runVectorQuery still applies Filter.Match to every
// candidate — it only changes which candidates are gathered, so a search-first
// prune or a filter-first switch returns the identical answer set, faster.
type vectorPlan struct {
	usable      bool    // false ⇒ no usable bitmap plan; fall back to the plain scan
	filterFirst bool    // true ⇒ gather candidates directly from the filter bitmap
	matchAll    bool    // true ⇒ an empty filter: every document qualifies, no prune
	cands       *bitmap // ordinals matching the filter (when usable && !matchAll)
	clusters    map[int]bool
	attrs       *AttrsFile
}

// planVectorQuery loads the epoch's bitmap attribute index and compiles the filter
// into a candidate-ordinal set, then decides the plan from its selectivity. It
// returns an unusable (pass-through) plan — never an error — whenever a bitmap
// plan is impossible: no bitmaps.json (older epoch or empty namespace), or a
// filter touching a field the index does not cover. In every such case the caller
// gathers candidates the original way and the per-candidate Filter.Match still
// decides membership, so the absence of a plan is always safe.
// indexNS/indexEpoch identify the prefix and epoch whose immutable bitmaps.json
// to read — for a branch these point at the parent's frozen epoch, resolved by
// the caller (docs/extensions/branches-copy-on-write.md).
func planVectorQuery(ctx context.Context, store *cache.Store, indexNS string, indexEpoch int64, filter Filter) (vectorPlan, error) {
	// An empty filter matches everything: no prune is possible or needed, and
	// today's behaviour (scan the nearest clusters, keep all) is preserved exactly.
	if filter.Op == "" || filter.Op == "all" {
		return vectorPlan{}, nil
	}

	af, err := loadAttrs(ctx, store, indexNS, indexEpoch)
	if err != nil {
		return vectorPlan{}, err
	}
	if af == nil {
		// No bitmap index under this epoch (built before the feature, or empty).
		return vectorPlan{}, nil
	}

	cands, ok := compileFilter(filter, af)
	if !ok {
		// The filter references a field the bitmap index does not cover; we cannot
		// safely prune, so fall back to the plain scan + Filter.Match.
		return vectorPlan{}, nil
	}

	// Derive the cluster-level set: the clusters that hold at least one matching
	// ordinal. A search-first scan skips any probed cluster absent from this set —
	// the native-filtering prune — and a filter-first plan restricts gathering to
	// exactly these candidates.
	clusters := make(map[int]bool)
	cands.each(func(ord uint32) {
		if int(ord) < len(af.ClusterOf) {
			if c := af.ClusterOf[ord]; c >= 0 {
				clusters[c] = true
			}
		}
	})

	plan := vectorPlan{usable: true, cands: cands, clusters: clusters, attrs: af}

	// Selectivity over the indexed corpus picks the plan. Below the hard threshold
	// the filter is the cheap, selective part: gather its matches directly so a
	// selective predicate can never be wiped out by the vector shortlist.
	total := len(af.Ords)
	if total > 0 && float64(cands.len())/float64(total) < filterFirstThreshold {
		plan.filterFirst = true
	}
	return plan, nil
}

// compileFilter recurses the filter tagged union into a bitmap of the ordinals
// that satisfy it, mirroring Filter.Match's semantics over the bitmap index: "eq"
// looks up the (field, value) bitmap, "and" intersects children, "or" unions them.
// It reports ok=false the moment it meets a field absent from the index or an
// operator it cannot express, because a partial compile could silently drop true
// matches — the caller treats !ok as "no plan" and uses the exact path instead.
func compileFilter(f Filter, af *AttrsFile) (*bitmap, bool) {
	switch f.Op {
	case "", "all":
		// Match-all has no finite ordinal set here; callers handle it before
		// recursing, so reaching it inside a compound filter is unexpected — report
		// it as not-compilable rather than guess.
		return nil, false
	case "eq":
		fv, ok := af.Values[f.Field]
		if !ok {
			return nil, false // field not bitmap-indexed: cannot compile.
		}
		key, ok := valueKey(f.Value)
		if !ok {
			return nil, false // non-scalar comparand: not an indexable eq.
		}
		// A value the index never saw yields the empty set — a perfectly valid,
		// maximally selective compile (the filter matches nothing).
		return bitmapFromSorted(fv[key]), true
	case "and":
		if len(f.Sub) == 0 {
			return nil, false
		}
		var acc *bitmap
		for _, sub := range f.Sub {
			bm, ok := compileFilter(sub, af)
			if !ok {
				return nil, false
			}
			if acc == nil {
				acc = bm
			} else {
				acc = acc.and(bm)
			}
		}
		return acc, true
	case "or":
		if len(f.Sub) == 0 {
			return nil, false
		}
		acc := newBitmap()
		for _, sub := range f.Sub {
			bm, ok := compileFilter(sub, af)
			if !ok {
				return nil, false
			}
			acc = acc.or(bm)
		}
		return acc, true
	default:
		return nil, false
	}
}

// loadAttrs loads and decodes the epoch's bitmap attribute index, returning a nil
// *AttrsFile (not an error) when the file is absent — an epoch built before this
// feature, or with no documents, simply has no bitmaps.json, and the planner then
// falls back to the plain scan. The file is immutable under the epoch prefix, so
// it is served from the cache like the other index objects (rule 2-safe).
func loadAttrs(ctx context.Context, store *cache.Store, ns string, epoch int64) (*AttrsFile, error) {
	body, err := store.GetCached(ctx, attrsKey(ns, epoch))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading attribute bitmaps: %w", err)
	}
	var af AttrsFile
	if err := json.Unmarshal(body, &af); err != nil {
		return nil, fmt.Errorf("decoding attribute bitmaps: %w", err)
	}
	return &af, nil
}

// vectorCandidatesFromIndex fills dists/attrs with candidates drawn from the live
// IVF epoch. With a filter-first plan it gathers exactly the filter's matching
// ordinals (recall-safe for selective predicates); otherwise it runs the
// search-first path — the nProbe nearest centroids, RaBitQ-lite agreement
// prefilter, then exact rerank — pruning any probed cluster the plan proved cannot
// match the filter. Either way runVectorQuery applies Filter.Match afterward, so
// the gathered set only ever shrinks work, never changes the answer
// (docs/extensions/bitmap-attribute-indexes.md).
// indexNS/indexEpoch/metric are resolved by the caller: for a branch they point
// at the parent's frozen, immutable epoch (docs/extensions/branches-copy-on-write.md),
// for a root at its own.
func vectorCandidatesFromIndex(ctx context.Context, store *cache.Store, indexNS string, indexEpoch int64, metric string, query []float32, nProbe, topK int, plan vectorPlan, dists map[string]float64, attrs map[string]map[string]any) error {
	if plan.filterFirst {
		return filterFirstCandidates(ctx, store, indexNS, indexEpoch, metric, query, plan, dists, attrs)
	}

	body, err := store.GetCached(ctx, centroidsKey(indexNS, indexEpoch))
	if err != nil {
		// A published epoch with no vectors (text-only data) has no centroids
		// file; a 404 is not an error, there are simply no indexed vector
		// candidates and the tail overlay handles the rest. Any other failure
		// (transport, decode upstream) must surface rather than silently
		// truncate the result set.
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("reading centroids: %w", err)
	}
	var cf CentroidsFile
	if err := json.Unmarshal(body, &cf); err != nil {
		return fmt.Errorf("decoding centroids: %w", err)
	}
	if len(cf.Centroids) == 0 {
		return nil
	}

	probes, _ := probeClusters(&cf, metric, query, nProbe)

	// Per probed cluster, score every member by the binary prefilter, keep a
	// shortlist of the best by that cheap estimate, then exact-rerank just the
	// shortlist. When the epoch carries a True RaBitQ rotation we score by the
	// paper's unbiased *estimated distance* (lower is better); without one (a
	// pre-RaBitQ epoch) we fall back to the legacy sign-bit Hamming agreement
	// (higher is better). score is normalized so that LOWER is always better, so a
	// single sort handles both: the estimator's distance directly, and the lite
	// agreement negated.
	type scored struct {
		entry ClusterEntry
		score float64 // lower = better candidate
	}
	var shortlist []scored

	// The query-side randomized rounding (Eq. 18) is unbiased for any seed; a fixed
	// seed keeps results reproducible across runs of the same query.
	qrng := rand.New(rand.NewSource(rotationSeed))

	for _, c := range probes {
		// Native-filtering prune: a probed cluster the plan proved holds no document
		// matching the filter is skipped without a fetch. Safe because the plan's
		// cluster set is exact (built from the same assignment), so a skipped cluster
		// could contribute only documents Filter.Match would reject anyway.
		if plan.usable && !plan.clusters[c] {
			continue
		}
		cbody, err := store.GetCached(ctx, clusterKey(indexNS, indexEpoch, c))
		if err != nil {
			return fmt.Errorf("reading cluster %d: %w", c, err)
		}
		var clf ClusterFile
		if err := json.Unmarshal(cbody, &clf); err != nil {
			return fmt.Errorf("decoding cluster %d: %w", c, err)
		}

		if cf.Rotation != nil {
			// True RaBitQ: build the cluster's QueryCode once, then estimate each
			// member's distance via the unbiased estimator (docs/extensions/true-rabitq.md).
			qc := EncodeQuery(query, clf.Centroid, cf.Rotation, qrng)
			for _, mem := range clf.Members {
				var s float64
				if mem.RaBitQ != nil {
					s = EstimateDistance(metric, *mem.RaBitQ, qc)
				} else {
					// Mixed epoch (a member without a True RaBitQ code): fall back to
					// an exact distance so it is neither unfairly dropped nor kept.
					s = Distance(metric, query, mem.Vector)
				}
				shortlist = append(shortlist, scored{entry: mem, score: s})
			}
			continue
		}

		// Legacy lite epoch: rank by sign-bit agreement (higher better), negated so
		// the shared "lower = better" sort orders it correctly.
		qCode := ResidualCode(query, clf.Centroid)
		for _, mem := range clf.Members {
			shortlist = append(shortlist, scored{entry: mem, score: -float64(Agreement(qCode, mem.Code, cf.Dimension))})
		}
	}

	// Keep the top (topK * multiplier) by the prefilter score before the exact
	// rerank. The True RaBitQ estimate is close enough to the real distance that a
	// far smaller multiplier preserves recall, but the cutoff logic is identical.
	limit := topK * shortlistMultiplier
	if len(shortlist) > limit {
		sort.Slice(shortlist, func(i, j int) bool {
			if shortlist[i].score != shortlist[j].score {
				return shortlist[i].score < shortlist[j].score
			}
			return shortlist[i].entry.ID < shortlist[j].entry.ID
		})
		shortlist = shortlist[:limit]
	}

	for _, s := range shortlist {
		dists[s.entry.ID] = Distance(metric, query, s.entry.Vector)
		attrs[s.entry.ID] = s.entry.Attrs
	}
	return nil
}

// PrefilterShortlist returns the ids that survive the binary-scan prefilter into
// the top-`limit` shortlist for one query against the live epoch, scored either by
// the True RaBitQ unbiased estimator (rabitq=true) or the legacy lite sign-bit
// agreement (rabitq=false). It is the exact scoring vectorCandidatesFromIndex
// uses, exposed so a benchmark can measure the headline win — the shortlist size
// True RaBitQ needs to retain a true neighbor versus the lite heuristic
// (docs/extensions/true-rabitq.md) — without reaching into engine internals.
//
// It is read-only: it loads only immutable index/v{epoch}/* objects via GetCached
// (rule 2-safe) and writes nothing, so it has no CAS implications. If the epoch
// has no rotation, rabitq=true is meaningless and it returns an error so a caller
// cannot silently compare lite against lite.
func PrefilterShortlist(ctx context.Context, store *cache.Store, ns string, m Manifest, query []float32, nProbe, limit int, rabitq bool) ([]string, error) {
	if m.IndexEpoch == 0 {
		return nil, nil
	}
	body, err := store.GetCached(ctx, centroidsKey(ns, m.IndexEpoch))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading centroids: %w", err)
	}
	var cf CentroidsFile
	if err := json.Unmarshal(body, &cf); err != nil {
		return nil, fmt.Errorf("decoding centroids: %w", err)
	}
	if len(cf.Centroids) == 0 {
		return nil, nil
	}
	if rabitq && cf.Rotation == nil {
		return nil, fmt.Errorf("epoch %d has no True RaBitQ rotation; cannot score by the estimator", m.IndexEpoch)
	}

	probes, _ := probeClusters(&cf, m.Metric, query, nProbe)
	type scored struct {
		id    string
		score float64 // lower = better
	}
	var all []scored
	qrng := rand.New(rand.NewSource(rotationSeed))
	for _, c := range probes {
		cbody, err := store.GetCached(ctx, clusterKey(ns, m.IndexEpoch, c))
		if err != nil {
			return nil, fmt.Errorf("reading cluster %d: %w", c, err)
		}
		var clf ClusterFile
		if err := json.Unmarshal(cbody, &clf); err != nil {
			return nil, fmt.Errorf("decoding cluster %d: %w", c, err)
		}
		if rabitq {
			qc := EncodeQuery(query, clf.Centroid, cf.Rotation, qrng)
			for _, mem := range clf.Members {
				var s float64
				if mem.RaBitQ != nil {
					s = EstimateDistance(m.Metric, *mem.RaBitQ, qc)
				} else {
					s = Distance(m.Metric, query, mem.Vector)
				}
				all = append(all, scored{id: mem.ID, score: s})
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
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = s.id
	}
	return out, nil
}

// filterFirstCandidates is the pre-filter plan: it scores ONLY the documents the
// filter bitmap selected. Because the candidate set is already the filter's exact
// match set, no vector shortlist can drop a true match before the filter runs —
// the recall trap a selective filter springs on search-first retrieval. It fetches
// the clusters those candidates live in, picks out the matching members, and
// computes their exact distances. The vector-less matches (ClusterOf == -1) are
// left to the docs.json-backed tail/exact paths; they carry no vector to rank, so
// they cannot be vector hits anyway.
func filterFirstCandidates(ctx context.Context, store *cache.Store, indexNS string, indexEpoch int64, metric string, query []float32, plan vectorPlan, dists map[string]float64, attrs map[string]map[string]any) error {
	// Group the candidate ordinals by the cluster their vector lives in, so each
	// cluster file is fetched once and only its matching members are scored.
	wanted := make(map[int]map[string]bool) // cluster -> set of candidate ids in it
	plan.cands.each(func(ord uint32) {
		if int(ord) >= len(plan.attrs.ClusterOf) {
			return
		}
		c := plan.attrs.ClusterOf[ord]
		if c < 0 {
			return // vector-less candidate: not a vector hit.
		}
		ids := wanted[c]
		if ids == nil {
			ids = make(map[string]bool)
			wanted[c] = ids
		}
		ids[plan.attrs.Ords[ord]] = true
	})

	for c, ids := range wanted {
		cbody, err := store.GetCached(ctx, clusterKey(indexNS, indexEpoch, c))
		if err != nil {
			return fmt.Errorf("reading cluster %d: %w", c, err)
		}
		var clf ClusterFile
		if err := json.Unmarshal(cbody, &clf); err != nil {
			return fmt.Errorf("decoding cluster %d: %w", c, err)
		}
		for _, mem := range clf.Members {
			if !ids[mem.ID] {
				continue
			}
			dists[mem.ID] = Distance(metric, query, mem.Vector)
			attrs[mem.ID] = mem.Attrs
		}
	}
	return nil
}

// probeClusters chooses which IVF clusters a vector query scans and returns them
// nearest-first alongside the measured fan-out (the number of centroid-distance
// comparisons performed to reach that decision). When the epoch carries a
// hierarchical centroid tree it beam-descends the tree
// (docs/extensions/hierarchical-centroid-tree.md); otherwise it falls back to the
// flat O(K) scan of nearestClusters. Either way the result is a set of flat
// cluster ids the caller fetches and reranks, so the two strategies return the
// SAME documents to score — the tree only changes how the cluster set is selected.
//
// CORRECTNESS of the same-top-K invariant: the beam width is sized so the tree
// surfaces AT LEAST as many leaves as the flat path would probe (treeBeamFactor *
// nProbe, clamped to K). A wide beam reproduces the flat ranking; the tests pin
// this by asserting beam descent returns the identical clusters a flat scan picks.
//
// HONEST FRAMING: the returned fan-out lets a caller SEE that, at this clone's
// scale, the tree performs roughly as many (or more) comparisons as the flat
// K-way scan — it is pedagogical here and only pays off at the large K a flat scan
// can no longer afford.
func probeClusters(cf *CentroidsFile, metric string, query []float32, nProbe int) (clusters []int, fanout int) {
	if cf.Tree != nil {
		// A wider beam than nProbe keeps boundary-adjacent leaves so the tree's leaf
		// set is a superset of the flat top-nProbe, preserving recall; we then keep
		// the nProbe nearest of those leaves to match the flat probe budget exactly.
		beam := nProbe * treeBeamFactor
		if beam < nProbe {
			beam = nProbe
		}
		if beam > len(cf.Centroids) {
			beam = len(cf.Centroids)
		}
		leaves, fo := BeamDescend(cf.Tree, query, metric, beam)
		if nProbe < len(leaves) {
			leaves = leaves[:nProbe]
		}
		return leaves, fo
	}
	probes := nearestClusters(metric, query, cf.Centroids, nProbe)
	// The flat scan compares the query against every centroid: K comparisons.
	return probes, len(cf.Centroids)
}

// treeBeamFactor widens the beam relative to nProbe so beam descent keeps enough
// boundary-adjacent leaves to reproduce the flat scan's top-nProbe clusters. A
// factor > 1 trades a few extra in-memory node comparisons for recall safety at
// split boundaries (SPANN §3.1 boundary issue); the surviving leaves are still
// trimmed back to nProbe so the cluster-fetch budget is unchanged.
const treeBeamFactor = 4

// nearestClusters returns the indices of the nProbe centroids closest to the
// query vector under metric, nearest first. nProbe is clamped to the number of
// clusters available.
func nearestClusters(metric string, query []float32, centroids [][]float32, nProbe int) []int {
	type clusterDist struct {
		idx  int
		dist float64
	}
	cds := make([]clusterDist, len(centroids))
	for i, c := range centroids {
		cds[i] = clusterDist{idx: i, dist: Distance(metric, query, c)}
	}
	sort.Slice(cds, func(i, j int) bool {
		if cds[i].dist != cds[j].dist {
			return cds[i].dist < cds[j].dist
		}
		return cds[i].idx < cds[j].idx
	})
	if nProbe > len(cds) {
		nProbe = len(cds)
	}
	probes := make([]int, nProbe)
	for i := 0; i < nProbe; i++ {
		probes[i] = cds[i].idx
	}
	return probes
}

// runBM25Query ranks documents by BM25 score against the query text. It scores
// the live indexed corpus with its global statistics, overlays the WAL tail
// (scoring fresh docs against the same stats so the scores are comparable),
// subtracts tombstones, applies the filter, and returns the TopK by descending
// $score.
func runBM25Query(ctx context.Context, store *cache.Store, ns string, m Manifest, v readView, p QueryParams, topK int) ([]QueryResult, error) {
	if m.TextField == "" {
		return nil, fmt.Errorf("querying %q: text query requested but the namespace has no text field", ns)
	}

	queryTerms := Tokenize(p.RankBy.Text)

	scores := make(map[string]float64)
	attrs := make(map[string]map[string]any)

	// idx holds the global BM25 statistics (N, df, AvgDL) used to score both
	// indexed and tail documents; an empty index yields finite, comparable
	// scores. Carried out of the index block so the tail scan can reuse it.
	var idx BM25File

	// The indexed epoch comes from the resolved view: a branch reads the parent's
	// frozen bm25.json/docs.json until it reindexes
	// (docs/extensions/branches-copy-on-write.md).
	if v.indexEpoch > 0 {
		loaded, docs, err := loadBM25Epoch(ctx, store, v.indexNS, v.indexEpoch)
		if err != nil {
			return nil, fmt.Errorf("querying %q: %w", ns, err)
		}
		idx = loaded
		for id, score := range Score(idx, queryTerms) {
			scores[id] = score
			attrs[id] = docs[id]
		}
	}

	// Overlay the unindexed tail, folding the branch chain when this is a branch.
	// Each fresh document is scored against the same global stats so its score is
	// comparable to the indexed hits; a newer version overwrites the indexed
	// score, and a tombstone removes the id — across the fork boundary too.
	live, deleted, err := MaterializeView(ctx, store, v, m.IndexedUpTo, m.WALSeq)
	if err != nil {
		return nil, fmt.Errorf("querying %q: scanning wal tail [%d,%d): %w", ns, m.IndexedUpTo, m.WALSeq, err)
	}
	for id, d := range live {
		text := textFieldValue(d.Attributes, m.TextField)
		score := ScoreDoc(idx, text, queryTerms)
		if score <= 0 {
			// A fresh doc that matches no query term is not a hit, and it must
			// also clear any stale indexed score for the same id (rule 5: the
			// newest version, which no longer matches, wins).
			delete(scores, id)
			delete(attrs, id)
			continue
		}
		scores[id] = score
		attrs[id] = d.Attributes
	}
	for id := range deleted {
		delete(scores, id)
		delete(attrs, id)
	}

	results := make([]QueryResult, 0, len(scores))
	for id, score := range scores {
		if !p.Filter.Match(attrs[id]) {
			continue
		}
		results = append(results, QueryResult{ID: id, Score: score, Attributes: attrs[id]})
	}

	// Highest score first; break ties on id for determinism.
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})
	if len(results) > topK {
		results = results[:topK]
	}
	return results, nil
}

// loadBM25Epoch loads the inverted index and the attribute map for the live
// epoch. The bm25.json file is absent when the namespace was indexed with no
// text field; an empty namespace still has a docs.json. Both are immutable under
// the epoch prefix, so they are served from the cache.
func loadBM25Epoch(ctx context.Context, store *cache.Store, ns string, epoch int64) (BM25File, map[string]map[string]any, error) {
	body, err := store.GetCached(ctx, bm25Key(ns, epoch))
	if err != nil {
		// No bm25.json under this epoch (404): nothing indexed for text, the
		// tail scan supplies any results. Any other failure must surface rather
		// than silently degrade the query to tail-only results.
		if errors.Is(err, storage.ErrNotFound) {
			return BM25File{}, map[string]map[string]any{}, nil
		}
		return BM25File{}, nil, fmt.Errorf("reading bm25 index: %w", err)
	}
	var idx BM25File
	if err := json.Unmarshal(body, &idx); err != nil {
		return BM25File{}, nil, fmt.Errorf("decoding bm25 index: %w", err)
	}

	dbody, err := store.GetCached(ctx, docsKey(ns, epoch))
	if err != nil {
		return BM25File{}, nil, fmt.Errorf("reading docs: %w", err)
	}
	var df DocsFile
	if err := json.Unmarshal(dbody, &df); err != nil {
		return BM25File{}, nil, fmt.Errorf("decoding docs: %w", err)
	}
	docs := df.Docs
	if docs == nil {
		docs = map[string]map[string]any{}
	}
	return idx, docs, nil
}
