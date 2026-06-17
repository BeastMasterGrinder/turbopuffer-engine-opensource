package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	switch {
	case vec && text:
		return nil, fmt.Errorf("querying %q: both vector and text rank modes set, exactly one is required", ns)
	case !vec && !text:
		return nil, fmt.Errorf("querying %q: no rank mode set, set either a vector or text query", ns)
	}

	topK := p.TopK
	if topK <= 0 {
		topK = defaultTopK
	}

	if vec {
		return runVectorQuery(ctx, store, ns, m, p, topK)
	}
	return runBM25Query(ctx, store, ns, m, p, topK)
}

// runVectorQuery ranks documents by distance to the query vector. It gathers
// candidates from the live IVF index (NProbe nearest clusters, RaBitQ-lite
// agreement prefilter, then exact rerank), overlays the full-precision WAL tail
// so newer writes win, subtracts tombstones, applies the filter, and returns the
// TopK nearest (ascending $dist).
func runVectorQuery(ctx context.Context, store *cache.Store, ns string, m Manifest, p QueryParams, topK int) ([]QueryResult, error) {
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

	if m.IndexEpoch > 0 {
		if err := vectorCandidatesFromIndex(ctx, store, ns, m, p.RankBy.Vector, nProbe, topK, dists, attrs); err != nil {
			return nil, fmt.Errorf("querying %q: %w", ns, err)
		}
	}

	// Overlay the unindexed tail at full precision. A newer upsert overwrites the
	// indexed distance; a tombstone removes the id entirely.
	live, deleted, err := MaterializeLiveAndDeleted(ctx, store, ns, m.IndexedUpTo, m.WALSeq)
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

// vectorCandidatesFromIndex fills dists/attrs with candidates drawn from the live
// IVF epoch: it picks the nProbe nearest centroids, prefilters their members by
// RaBitQ-lite sign-bit agreement down to a shortlist, then reranks the shortlist
// at full precision. The shortlist keeps the prefilter cheap while letting the
// exact distance decide the final order (docs/02, docs/03).
func vectorCandidatesFromIndex(ctx context.Context, store *cache.Store, ns string, m Manifest, query []float32, nProbe, topK int, dists map[string]float64, attrs map[string]map[string]any) error {
	body, err := store.GetCached(ctx, centroidsKey(ns, m.IndexEpoch))
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

	probes := nearestClusters(m.Metric, query, cf.Centroids, nProbe)

	// Compute the query's own sign-bit code per probed cluster (the code is
	// relative to that cluster's centroid), then rank members by agreement.
	type scored struct {
		entry     ClusterEntry
		agreement int
	}
	var shortlist []scored
	for _, c := range probes {
		cbody, err := store.GetCached(ctx, clusterKey(ns, m.IndexEpoch, c))
		if err != nil {
			return fmt.Errorf("reading cluster %d: %w", c, err)
		}
		var clf ClusterFile
		if err := json.Unmarshal(cbody, &clf); err != nil {
			return fmt.Errorf("decoding cluster %d: %w", c, err)
		}
		qCode := ResidualCode(query, clf.Centroid)
		for _, mem := range clf.Members {
			shortlist = append(shortlist, scored{entry: mem, agreement: Agreement(qCode, mem.Code, cf.Dimension)})
		}
	}

	// Keep the top (topK * multiplier) by agreement before the exact rerank.
	limit := topK * shortlistMultiplier
	if len(shortlist) > limit {
		sort.Slice(shortlist, func(i, j int) bool {
			if shortlist[i].agreement != shortlist[j].agreement {
				return shortlist[i].agreement > shortlist[j].agreement
			}
			return shortlist[i].entry.ID < shortlist[j].entry.ID
		})
		shortlist = shortlist[:limit]
	}

	for _, s := range shortlist {
		dists[s.entry.ID] = Distance(m.Metric, query, s.entry.Vector)
		attrs[s.entry.ID] = s.entry.Attrs
	}
	return nil
}

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
func runBM25Query(ctx context.Context, store *cache.Store, ns string, m Manifest, p QueryParams, topK int) ([]QueryResult, error) {
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

	if m.IndexEpoch > 0 {
		loaded, docs, err := loadBM25Epoch(ctx, store, ns, m.IndexEpoch)
		if err != nil {
			return nil, fmt.Errorf("querying %q: %w", ns, err)
		}
		idx = loaded
		for id, score := range Score(idx, queryTerms) {
			scores[id] = score
			attrs[id] = docs[id]
		}
	}

	// Overlay the unindexed tail. Each fresh document is scored against the same
	// global stats so its score is comparable to the indexed hits; a newer
	// version overwrites the indexed score, and a tombstone removes the id.
	live, deleted, err := MaterializeLiveAndDeleted(ctx, store, ns, m.IndexedUpTo, m.WALSeq)
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
