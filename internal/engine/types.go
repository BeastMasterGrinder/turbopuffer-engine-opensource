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
//
// The Parent/Fork* trio is the copy-on-write branch pointer
// (docs/extensions/branches-copy-on-write.md). They are zero for an ordinary
// (root) namespace, so existing manifests stay valid and every non-branch path
// behaves exactly as before. When Parent != "" this manifest is a BRANCH: it
// shares the parent's immutable WAL segments [0, ForkWALSeq) and the parent's
// frozen index epoch ForkIndexEpoch by reference, and diverges only as new
// writes land under its own prefix. Because the shared objects are write-once
// and the parent is never mutated, the branch reads the parent's bytes for free
// — that is the whole CoW trick. A branch's own WAL segments are numbered in the
// SAME logical seq space as the parent's: segment seq < ForkWALSeq resolves to
// the parent (recursing up the chain), seq >= ForkWALSeq resolves to this
// branch's own prefix. WALSeq therefore starts at ForkWALSeq at fork time.
type Manifest struct {
	Version     int64  `json:"version"`     // informational; ETag is the real CAS token
	Dimension   int    `json:"dimension"`   // vector dimension
	Metric      string `json:"metric"`      // "cosine" | "euclidean"
	TextField   string `json:"textField"`   // "" = no BM25
	WALSeq      int64  `json:"walSeq"`      // next seq; segments [0,WALSeq) exist (logical)
	IndexedUpTo int64  `json:"indexedUpTo"` // [0,IndexedUpTo) folded into the live index
	IndexEpoch  int64  `json:"indexEpoch"`  // live index/v{epoch}/; 0 = none built yet
	DocCount    int    `json:"docCount"`    // live document count at last index

	// Branch (copy-on-write) pointer; all zero for a root namespace.
	Parent         string `json:"parent,omitempty"`         // "" = root; else the parent namespace name
	ForkWALSeq     int64  `json:"forkWalSeq,omitempty"`     // child inherits parent WAL [0,ForkWALSeq)
	ForkIndexEpoch int64  `json:"forkIndexEpoch,omitempty"` // parent index epoch frozen at fork; 0 = none
}

// IsBranch reports whether this manifest is a copy-on-write branch of another
// namespace rather than a root namespace.
func (m Manifest) IsBranch() bool { return m.Parent != "" }

// NamespaceConfig is the immutable shape chosen at Create time.
type NamespaceConfig struct {
	Dimension int
	Metric    string
	TextField string
}

// RankBy selects the query mode. Setting Vector alone is a vector query, Text
// alone is a BM25 query, and setting BOTH is a hybrid query: the two retrievals
// run independently and their ranked lists are fused (see runHybridQuery in
// query.go). Setting neither is an error.
type RankBy struct {
	Vector []float32
	Text   string
}

// IsVector reports whether the query carries a vector to rank by.
func (r RankBy) IsVector() bool { return r.Vector != nil }

// IsText reports whether the query carries text to rank by.
func (r RankBy) IsText() bool { return r.Text != "" }

// IsHybrid reports whether both signals are set, so the planner fuses a vector
// ranking and a BM25 ranking into one ordering.
func (r RankBy) IsHybrid() bool { return r.IsVector() && r.IsText() }

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
// closer); Score is set in BM25 mode (higher is better). In hybrid mode RRF
// holds the fused Reciprocal Rank Fusion score (higher is better) and the result
// is ordered by it, while Dist and Score are kept alongside for transparency —
// either may be zero if the hit only appeared in one of the two legs.
type QueryResult struct {
	ID         string         `json:"id"`
	Dist       float64        `json:"$dist,omitempty"`  // vector mode (lower = closer)
	Score      float64        `json:"$score,omitempty"` // bm25 mode (higher = better)
	RRF        float64        `json:"$rrf,omitempty"`   // hybrid mode (higher = better)
	Attributes map[string]any `json:"attributes,omitempty"`
}

// CentroidsFile is index/v{epoch}/centroids.json: the IVF cluster centroids and
// their member counts.
//
// Rotation is the True RaBitQ random orthogonal matrix P for this epoch
// (docs/extensions/true-rabitq.md). It is sampled once at build time from a fixed
// seed and stored here, on the single per-epoch object, rather than duplicated
// into every cluster file. Because index/v{epoch}/* keys are write-once and
// immutable, the rotation is deterministic and cache-safe: query-time reads the
// SAME P the encoder used, so the binary-scan estimator stays self-consistent
// with the stored codes. A nil Rotation marks a pre-RaBitQ ("lite") epoch, which
// the query path still serves by falling back to the sign-bit Agreement prefilter.
//
// Tree is the optional hierarchical centroid tree
// (docs/extensions/hierarchical-centroid-tree.md): a wide, shallow tree whose
// leaves route to the flat Centroids/cluster-{i}.json. It is built once at index
// time OVER the flat centroids and stored on this same per-epoch object, so it is
// immutable and cache-safe alongside the rotation. A query may beam-descend the
// tree to pick which clusters to probe instead of scanning every centroid; a nil
// Tree (the default, and what the indexer emits at this clone's scale where a tree
// buys nothing) means "no hierarchy" and the query path keeps doing the flat O(K)
// centroid scan. Because the leaves ARE the flat clusters, the tree changes only
// HOW clusters are selected, never which documents a cluster holds — so F5's
// rotated RaBitQ codes and F4's bitmap ClusterOf assignment are untouched.
type CentroidsFile struct {
	Metric    string
	Dimension int
	K         int
	Centroids [][]float32
	Sizes     []int
	Rotation  *Rotation     `json:"rotation,omitempty"`
	Tree      *CentroidNode `json:"tree,omitempty"`
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
//
// Code is the legacy RaBitQ-lite sign-bit fingerprint; RaBitQ is the True RaBitQ
// code (sign bits relative to the epoch's rotated frame, plus the ‖oᵣ−c‖ and
// ⟨ō,o⟩ scalars the unbiased estimator needs, docs/extensions/true-rabitq.md).
// Current epochs populate RaBitQ; Code is retained so an epoch built before this
// feature still decodes and is served by the lite Agreement prefilter.
//
// Version is the SPFresh per-vector version number (the version-map analogue,
// SPFresh §4.1, §4.2.1; docs/spfresh-lire/02-implementation-in-tpuf.md). LIRE's
// Reassign moves a vector by APPENDING a higher-version copy to its NPA-correct
// cluster BEFORE the old copy is dropped, so for a window two copies of the same
// id can coexist in the epoch; the higher Version marks the authoritative copy so
// search and GC can tell the stale replica from the live one. Within a single
// published Option-A epoch the reassign loop drops the stale copy before the CAS,
// so a query never sees two copies — but the field is carried on disk (and
// round-trips through JSON) so the version-map mechanism is real groundwork, not a
// stub, and so the deferred Option-B per-cluster-versioned-object phase can shadow
// a stale copy that lingers across objects. It is omitempty so every pre-LIRE
// epoch decodes to version 0, the implicit "original assignment" version.
type ClusterEntry struct {
	ID      string
	Vector  []float32
	Code    []uint64    `json:"Code,omitempty"`
	RaBitQ  *RaBitQCode `json:"RaBitQ,omitempty"`
	Attrs   map[string]any
	Version int `json:"version,omitempty"`
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

// AttrsFile is index/v{epoch}/bitmaps.json: the bitmap attribute index that
// powers filter-first / search-first planning
// (docs/extensions/bitmap-attribute-indexes.md). It is a write-once, immutable
// part of the epoch — built by the indexer under the fresh epoch prefix and made
// live by the same single manifest CAS as the rest of the epoch — so the query
// path may GetCached it (correctness rule 2-safe: an index/v{epoch}/* key is
// never overwritten).
//
// Roaring indexes integers, but document ids are strings, so the indexer assigns
// every live document a dense [0, N) ordinal and records the mapping here:
//
//   - Ords[ordinal] = the document's string id, so a bitmap bit translates back to
//     an id (and thence to docs.json / cluster attrs).
//   - ClusterOf[ordinal] = the IVF cluster the document's vector landed in, or -1
//     for a vector-less document. From it the planner derives, for any candidate
//     ordinal-set, the set of clusters that can still contain a match — the
//     cluster-level pruning turbopuffer calls native filtering, without storing a
//     second bitmap level.
//
// Values holds, per filterable field, a map from the value's canonical string key
// to the sorted ordinals carrying it (the on-disk form of a roaring bitmap; see
// bitmap.toSorted). Only low-cardinality, non-text attributes are indexed: the
// text field is excluded (it is the BM25 surface, not a filter predicate), and a
// field whose cardinality approaches N is skipped as pure overhead with no
// pruning value (a unique-per-doc field would produce N singleton bitmaps). A
// field absent from Values is simply not bitmap-indexed; the planner falls back to
// the per-candidate Filter.Match for it, so correctness never depends on a field
// being indexed.
type AttrsFile struct {
	Ords      []string                       // ordinal -> document id
	ClusterOf []int                          // ordinal -> IVF cluster id, or -1 if vector-less
	Values    map[string]map[string][]uint32 // field -> valueKey -> sorted ordinals
}
