// Package engine implements the tpuf vector + full-text search engine: a
// namespace's manifest is the CAS-coordinated source of truth, durable WAL
// segments hold freshly upserted documents, and an immutable per-epoch index
// (centroid/IVF vectors + BM25) is published with a single atomic manifest
// swap. This file defines the data shapes shared across the engine, including
// the on-disk index objects.
package engine

import "reflect"

// Document is a single upserted record. A tombstone (Deleted == true) carries
// only an ID and removes a prior document at materialize time.
type Document struct {
	ID         string         `json:"id"`
	Vector     []float32      `json:"vector,omitempty"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Deleted    bool           `json:"deleted,omitempty"` // tombstone in WAL
}

// WALSegment is one durable batch of upsert operations, stored at
// wal/{seq:020d}.json. Segments [0,WALSeq) exist for a namespace.
type WALSegment struct {
	Seq int64      `json:"seq"`
	Ops []Document `json:"ops"`
}

// Manifest is the CAS-coordinated source of truth for a namespace. The object's
// S3 ETag is the real CAS token; Version is informational only.
type Manifest struct {
	Version     int64  `json:"version"`     // informational; ETag is the real CAS token
	Dimension   int    `json:"dimension"`   // vector dimension
	Metric      string `json:"metric"`      // "cosine" | "euclidean"
	TextField   string `json:"textField"`   // "" = no BM25
	WALSeq      int64  `json:"walSeq"`      // next seq; segments [0,WALSeq) exist
	IndexedUpTo int64  `json:"indexedUpTo"` // [0,IndexedUpTo) folded into the live index
	IndexEpoch  int64  `json:"indexEpoch"`  // live index/v{epoch}/; 0 = none built yet
	DocCount    int    `json:"docCount"`    // live document count at last index
}

// NamespaceConfig is the immutable shape chosen at Create time.
type NamespaceConfig struct {
	Dimension int
	Metric    string
	TextField string
}

// RankBy selects the query mode. Exactly one of Vector or Text is set.
type RankBy struct {
	Vector []float32
	Text   string
}

// IsVector reports whether the query ranks by vector distance.
func (r RankBy) IsVector() bool { return r.Vector != nil }

// IsText reports whether the query ranks by BM25 text score.
func (r RankBy) IsText() bool { return r.Text != "" }

// QueryParams are the inputs to Namespace.Query.
type QueryParams struct {
	RankBy RankBy
	Filter Filter
	TopK   int
	NProbe int
}

// Filter is a tagged-union predicate over a document's attributes: a leaf "eq"
// comparison, an "and"/"or" of sub-filters, or "" which matches everything. It
// is JSON-serializable and recursive, avoiding interface dispatch.
type Filter struct {
	Op    string   `json:"op"` // "eq" | "and" | "or" | "" (match-all)
	Field string   `json:"field,omitempty"`
	Value any      `json:"value,omitempty"`
	Sub   []Filter `json:"sub,omitempty"`
}

// Match reports whether attrs satisfies the filter. An empty Op matches every
// document; an "eq" on a missing attribute is false (never panics). Numeric
// comparisons are coerced because JSON decodes all numbers to float64, so 5 and
// 5.0 compare equal.
func (f Filter) Match(attrs map[string]any) bool {
	switch f.Op {
	case "", "all":
		return true
	case "eq":
		got, ok := attrs[f.Field]
		if !ok {
			return false
		}
		return equalValues(got, f.Value)
	case "and":
		for _, sub := range f.Sub {
			if !sub.Match(attrs) {
				return false
			}
		}
		return true
	case "or":
		for _, sub := range f.Sub {
			if sub.Match(attrs) {
				return true
			}
		}
		return false
	default:
		// Unknown operator matches nothing rather than panicking.
		return false
	}
}

// equalValues compares two attribute values for equality. Numbers are compared
// as float64 so that JSON-decoded values (always float64) match Go literals of
// any numeric kind; non-numeric values fall back to a deep equality check.
func equalValues(a, b any) bool {
	af, aok := numericValue(a)
	bf, bok := numericValue(b)
	if aok && bok {
		return af == bf
	}
	if aok != bok {
		// One side is numeric, the other is not: not equal.
		return false
	}
	return reflect.DeepEqual(a, b)
}

// numericValue converts a value to a float64 if it holds any Go numeric kind,
// reporting ok=false for non-numeric values. This bridges JSON's float64
// numbers and the int/float literals used in Go code and tests.
func numericValue(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

// QueryResult is a single ranked hit. Dist is set in vector mode (lower is
// closer); Score is set in BM25 mode (higher is better).
type QueryResult struct {
	ID         string         `json:"id"`
	Dist       float64        `json:"$dist,omitempty"`  // vector mode (lower = closer)
	Score      float64        `json:"$score,omitempty"` // bm25 mode (higher = better)
	Attributes map[string]any `json:"attributes,omitempty"`
}

// CentroidsFile is index/v{epoch}/centroids.json: the IVF cluster centroids and
// their member counts.
type CentroidsFile struct {
	Metric    string
	Dimension int
	K         int
	Centroids [][]float32
	Sizes     []int
}

// ClusterFile is index/v{epoch}/cluster-{i}.json: the members of one IVF
// cluster, each carrying its vector, RaBitQ-lite code, and attributes.
type ClusterFile struct {
	Cluster  int
	Centroid []float32
	Members  []ClusterEntry
}

// ClusterEntry is one document inside a cluster. Attrs is duplicated here so a
// vector hit can filter and return without a second fetch.
type ClusterEntry struct {
	ID     string
	Vector []float32
	Code   []uint64
	Attrs  map[string]any
}

// BM25File is index/v{epoch}/bm25.json: the inverted index plus the global
// statistics needed to score documents.
type BM25File struct {
	N      int            // total documents
	AvgDL  float64        // average document length
	DocLen map[string]int // document id -> token count
	Index  map[string][]Posting
}

// Posting is one term occurrence in the BM25 inverted index.
type Posting struct {
	ID string
	TF int
}

// DocsFile is index/v{epoch}/docs.json: attributes keyed by document id, used
// to return attributes for BM25 hits.
type DocsFile struct {
	Docs map[string]map[string]any
}
