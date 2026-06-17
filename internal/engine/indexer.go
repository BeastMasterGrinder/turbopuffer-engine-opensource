package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// kmeansIters is the number of Lloyd's refinement passes the indexer runs when
// building the IVF clusters (docs/06). KMeans stops early once assignments
// stabilize, so this is an upper bound, generous for the small N this
// educational clone targets.
const kmeansIters = 25

// indexPrefix returns the object-key prefix for a namespace's index epoch:
// {ns}/index/v{epoch}/. Every object written under it is write-once and
// immutable, so the cache may serve it via GetCached and the indexer writes it
// with an unconditional Put.
func indexPrefix(ns string, epoch int64) string {
	return fmt.Sprintf("%s/index/v%d/", ns, epoch)
}

func centroidsKey(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "centroids.json"
}

func clusterKey(ns string, epoch int64, cluster int) string {
	return fmt.Sprintf("%scluster-%d.json", indexPrefix(ns, epoch), cluster)
}

func bm25Key(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "bm25.json"
}

func docsKey(ns string, epoch int64) string {
	return indexPrefix(ns, epoch) + "docs.json"
}

// BuildIndex folds the durable WAL into a fresh, immutable index epoch and
// publishes it with a single atomic manifest CAS. It is the heart of the
// "object storage is the source of truth, everything else hides its latency"
// bet: the index is just a derived snapshot of the WAL that queries can read
// from cached, write-once objects.
//
// The flow follows docs/06's indexer pseudocode and obeys the CAS correctness
// rules:
//
//   - walUpTo is snapshotted from the manifest at index START (rule 3): any
//     write that lands while we build belongs to the next epoch, and a query
//     overlays the tail [IndexedUpTo, WALSeq) so those writes stay searchable.
//   - All index/v{epoch}/* objects are written first, under the fresh epoch
//     prefix, with unconditional Put (write-once keys; no concurrent writer can
//     collide). The index goes live only at the final SaveManifestCAS that
//     flips IndexEpoch (rule 4 — atomic swap). Until then queries serve the old
//     epoch.
//
// An empty namespace (WALSeq 0) is handled gracefully: live is empty, no vector
// or BM25 work happens, docs.json is an empty map, and the manifest still
// advances to the new epoch so Info reflects an indexed-but-empty namespace.
func BuildIndex(ctx context.Context, store *cache.Store, ns string) error {
	m, _, err := LoadManifest(ctx, store, ns)
	if err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	// Rule 3: snapshot the WAL position NOW. This becomes IndexedUpTo, so the
	// query tail starts exactly where this epoch's coverage ends.
	walUpTo := m.WALSeq
	epoch := m.IndexEpoch + 1

	live, err := MaterializeLive(ctx, store, ns, 0, walUpTo)
	if err != nil {
		return fmt.Errorf("indexing %q: materializing [0,%d): %w", ns, walUpTo, err)
	}

	if err := buildVectorIndex(ctx, store, ns, epoch, m.Metric, live); err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	if m.TextField != "" {
		if err := buildBM25Index(ctx, store, ns, epoch, m.TextField, live); err != nil {
			return fmt.Errorf("indexing %q: %w", ns, err)
		}
	}

	if err := writeDocs(ctx, store, ns, epoch, live); err != nil {
		return fmt.Errorf("indexing %q: %w", ns, err)
	}

	// Rule 4: a single CAS makes the new epoch live. Everything above is already
	// durable under the fresh prefix, so this flip is the only externally
	// visible moment of the swap.
	docCount := len(live)
	if _, err := SaveManifestCAS(ctx, store, ns, func(m *Manifest) {
		m.IndexEpoch = epoch
		m.IndexedUpTo = walUpTo
		m.DocCount = docCount
	}); err != nil {
		return fmt.Errorf("indexing %q: publishing epoch %d: %w", ns, epoch, err)
	}
	return nil
}

// buildVectorIndex clusters the live documents that carry a vector and writes
// centroids.json plus one cluster-{i}.json per IVF cluster. Documents with no
// vector (e.g. text-only records in a namespace that also does BM25) are
// skipped — only vectors participate in k-means. When no live document has a
// vector, nothing is written and the namespace simply has no vector index for
// this epoch.
func buildVectorIndex(ctx context.Context, store *cache.Store, ns string, epoch int64, metric string, live map[string]Document) error {
	// Gather the vector-bearing docs in a stable order. The order itself does
	// not matter for correctness, but collecting ids and points in lockstep
	// keeps each point paired with its document for the per-doc residual code.
	var ids []string
	var points [][]float32
	var dim int
	for id, d := range live {
		if d.Vector == nil {
			continue
		}
		ids = append(ids, id)
		points = append(points, d.Vector)
		dim = len(d.Vector)
	}
	if len(points) == 0 {
		return nil
	}

	k := ChooseK(len(points))
	centroids, assign := KMeans(points, k, metric, kmeansIters)
	// KMeans clamps k to [1, len(points)], so the realized cluster count may be
	// smaller than ChooseK for tiny N; trust len(centroids), not k.
	numClusters := len(centroids)

	// Bucket members per cluster, computing each document's RaBitQ-lite residual
	// code (sign bits of v - its centroid) so query-time can prefilter by
	// agreement before an exact rerank.
	members := make([][]ClusterEntry, numClusters)
	sizes := make([]int, numClusters)
	for i, id := range ids {
		c := assign[i]
		code := ResidualCode(points[i], centroids[c])
		members[c] = append(members[c], ClusterEntry{
			ID:     id,
			Vector: points[i],
			Code:   code,
			Attrs:  live[id].Attributes,
		})
		sizes[c]++
	}

	centroidsFile := CentroidsFile{
		Metric:    metric,
		Dimension: dim,
		K:         numClusters,
		Centroids: centroids,
		Sizes:     sizes,
	}
	if err := putJSON(ctx, store, centroidsKey(ns, epoch), centroidsFile); err != nil {
		return fmt.Errorf("writing centroids: %w", err)
	}

	for c := 0; c < numClusters; c++ {
		cf := ClusterFile{
			Cluster:  c,
			Centroid: centroids[c],
			Members:  members[c],
		}
		if err := putJSON(ctx, store, clusterKey(ns, epoch, c), cf); err != nil {
			return fmt.Errorf("writing cluster %d: %w", c, err)
		}
	}
	return nil
}

// buildBM25Index builds the inverted index over the configured text field of
// every live document and writes bm25.json. A document whose text field is
// absent or non-string contributes an empty document (zero-length, no terms),
// which BuildBM25 handles. The field value is read from a document's attributes
// because the text lives alongside the other payload, not on a dedicated
// Document field.
func buildBM25Index(ctx context.Context, store *cache.Store, ns string, epoch int64, textField string, live map[string]Document) error {
	texts := make(map[string]string, len(live))
	for id, d := range live {
		texts[id] = textFieldValue(d.Attributes, textField)
	}

	idx := BuildBM25(texts)
	if err := putJSON(ctx, store, bm25Key(ns, epoch), idx); err != nil {
		return fmt.Errorf("writing bm25: %w", err)
	}
	return nil
}

// textFieldValue extracts the text-field value from a document's attributes,
// returning "" when the field is missing or not a string. Returning "" rather
// than erroring keeps a malformed or text-less document indexable (it simply
// matches no query term) instead of failing the whole build.
func textFieldValue(attrs map[string]any, field string) string {
	v, ok := attrs[field]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
}

// writeDocs writes docs.json: a map of document id -> attributes for every live
// document. BM25 hits read their attributes from here (vector hits carry attrs
// inline in the cluster file). It is always written, even for an empty
// namespace, so query-time can rely on its presence under a published epoch.
func writeDocs(ctx context.Context, store *cache.Store, ns string, epoch int64, live map[string]Document) error {
	docs := make(map[string]map[string]any, len(live))
	for id, d := range live {
		docs[id] = d.Attributes
	}
	if err := putJSON(ctx, store, docsKey(ns, epoch), DocsFile{Docs: docs}); err != nil {
		return fmt.Errorf("writing docs: %w", err)
	}
	return nil
}

// putJSON marshals v and writes it unconditionally under key. Index objects are
// write-once under a fresh epoch prefix, so an unconditional Put is correct: no
// concurrent writer can target the same key, and the publish only becomes
// visible at the final manifest CAS.
func putJSON(ctx context.Context, store *cache.Store, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling %q: %w", key, err)
	}
	if _, err := store.Put(ctx, key, body); err != nil {
		return fmt.Errorf("putting %q: %w", key, err)
	}
	return nil
}
