package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// seedWAL appends each segment in order and advances the manifest's WALSeq to
// match, mirroring what Upsert does: durable WAL segments, then a manifest CAS
// that publishes the new sequence. It returns the resulting WALSeq so a test can
// assert IndexedUpTo against the snapshot taken at index start.
func seedWAL(ctx context.Context, t *testing.T, store *cache.Store, ns string, segments [][]Document) int64 {
	t.Helper()
	for seq, ops := range segments {
		if err := AppendWAL(ctx, store, ns, int64(seq), ops); err != nil {
			t.Fatalf("AppendWAL(seq=%d): got err %v, want nil", seq, err)
		}
	}
	walSeq := int64(len(segments))
	if _, err := SaveManifestCAS(ctx, store, ns, func(m *Manifest) {
		m.WALSeq = walSeq
	}); err != nil {
		t.Fatalf("advancing WALSeq to %d: got err %v, want nil", walSeq, err)
	}
	return walSeq
}

// vecDoc builds a document carrying a vector and the text field used by the
// test config so the same record participates in both the vector and BM25
// index paths.
func vecDoc(id string, vec []float32, body string) Document {
	return Document{
		ID:         id,
		Vector:     vec,
		Attributes: map[string]any{"body": body},
	}
}

// objectExists reports whether key is present in the backing store. Index
// objects are written with an unconditional Put, so a successful uncached Get
// proves the object was published.
func objectExists(ctx context.Context, t *testing.T, store *cache.Store, key string) bool {
	t.Helper()
	_, _, err := store.Get(ctx, key)
	if err == nil {
		return true
	}
	if errors.Is(err, storage.ErrNotFound) {
		return false
	}
	t.Fatalf("Get(%q): unexpected err %v", key, err)
	return false
}

// loadJSON fetches key and decodes it into v, failing the test on any error.
func loadJSON(ctx context.Context, t *testing.T, store *cache.Store, key string, v any) {
	t.Helper()
	body, _, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q): got err %v, want nil", key, err)
	}
	if err := json.Unmarshal(body, v); err != nil {
		t.Fatalf("decoding %q: got err %v, want nil", key, err)
	}
}

func TestBuildIndexPublishesEpochAndSnapshotsWALSeq(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	walSeq := seedWAL(ctx, t, store, testNS, [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "the quick brown fox"),
			vecDoc("b", []float32{0, 1, 0, 0}, "lazy dog sleeps"),
		},
		{
			vecDoc("c", []float32{0, 0, 1, 0}, "quick fox runs"),
		},
	})

	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest after index: got err %v, want nil", err)
	}
	if m.IndexEpoch != 1 {
		t.Errorf("IndexEpoch: got %d, want 1", m.IndexEpoch)
	}
	// Rule 3: IndexedUpTo is the WALSeq snapshotted at index start.
	if m.IndexedUpTo != walSeq {
		t.Errorf("IndexedUpTo: got %d, want %d (the start snapshot)", m.IndexedUpTo, walSeq)
	}
	if m.DocCount != 3 {
		t.Errorf("DocCount: got %d, want 3", m.DocCount)
	}

	// Every index object must exist under the live epoch prefix.
	for _, key := range []string{
		centroidsKey(testNS, 1),
		bm25Key(testNS, 1),
		docsKey(testNS, 1),
	} {
		if !objectExists(ctx, t, store, key) {
			t.Errorf("expected index object %q to exist", key)
		}
	}

	// At least cluster-0 must exist; ChooseK(3) rounds sqrt(3) -> 2, so clusters
	// 0 and 1 are written.
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, 1), &cf)
	if cf.K != ChooseK(3) {
		t.Errorf("centroids K: got %d, want %d", cf.K, ChooseK(3))
	}
	if cf.Metric != "cosine" || cf.Dimension != 4 {
		t.Errorf("centroids header: got metric=%q dim=%d, want cosine/4", cf.Metric, cf.Dimension)
	}
	if len(cf.Centroids) != cf.K || len(cf.Sizes) != cf.K {
		t.Fatalf("centroids arrays: got %d centroids, %d sizes, want %d each", len(cf.Centroids), len(cf.Sizes), cf.K)
	}

	var totalMembers, sizeSum int
	for c := 0; c < cf.K; c++ {
		if !objectExists(ctx, t, store, clusterKey(testNS, 1, c)) {
			t.Fatalf("expected cluster object cluster-%d to exist", c)
		}
		var clf ClusterFile
		loadJSON(ctx, t, store, clusterKey(testNS, 1, c), &clf)
		totalMembers += len(clf.Members)
		sizeSum += cf.Sizes[c]
		for _, mem := range clf.Members {
			if len(mem.Code) == 0 {
				t.Errorf("cluster-%d member %q: empty RaBitQ code, want sign bits", c, mem.ID)
			}
			if mem.Attrs["body"] == nil {
				t.Errorf("cluster-%d member %q: missing inlined attrs", c, mem.ID)
			}
		}
	}
	if totalMembers != 3 {
		t.Errorf("total cluster members: got %d, want 3 (every vector assigned once)", totalMembers)
	}
	if sizeSum != 3 {
		t.Errorf("sum of centroid sizes: got %d, want 3", sizeSum)
	}

	// docs.json carries every live document's attributes.
	var df DocsFile
	loadJSON(ctx, t, store, docsKey(testNS, 1), &df)
	gotDocs := sortedKeys(df.Docs)
	wantDocs := []string{"a", "b", "c"}
	if !equalStrings(gotDocs, wantDocs) {
		t.Errorf("docs.json ids: got %v, want %v", gotDocs, wantDocs)
	}

	// bm25.json reflects all three documents.
	var bf BM25File
	loadJSON(ctx, t, store, bm25Key(testNS, 1), &bf)
	if bf.N != 3 {
		t.Errorf("bm25 N: got %d, want 3", bf.N)
	}
	if _, ok := bf.Index["quick"]; !ok {
		t.Errorf("bm25 index missing the term %q", "quick")
	}
}

func TestBuildIndexAppliesTombstones(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// a is upserted then deleted; the live index must hold only b.
	seedWAL(ctx, t, store, testNS, [][]Document{
		{
			vecDoc("a", []float32{1, 0, 0, 0}, "alpha"),
			vecDoc("b", []float32{0, 1, 0, 0}, "beta"),
		},
		{tombstone("a")},
	})

	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	if m.DocCount != 1 {
		t.Errorf("DocCount after tombstone: got %d, want 1", m.DocCount)
	}

	var df DocsFile
	loadJSON(ctx, t, store, docsKey(testNS, m.IndexEpoch), &df)
	if _, ok := df.Docs["a"]; ok {
		t.Errorf("docs.json still contains tombstoned id %q", "a")
	}
	if _, ok := df.Docs["b"]; !ok {
		t.Errorf("docs.json missing surviving id %q", "b")
	}
}

func TestBuildIndexEmptyNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// No WAL segments: WALSeq stays 0.
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex on empty namespace: got err %v, want nil", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	if m.IndexEpoch != 1 {
		t.Errorf("IndexEpoch: got %d, want 1", m.IndexEpoch)
	}
	if m.IndexedUpTo != 0 {
		t.Errorf("IndexedUpTo: got %d, want 0 (empty WAL)", m.IndexedUpTo)
	}
	if m.DocCount != 0 {
		t.Errorf("DocCount: got %d, want 0", m.DocCount)
	}

	// No vectors and no documents: centroids/clusters are skipped, but docs.json
	// is always written and bm25.json is written because the namespace has a
	// text field.
	if objectExists(ctx, t, store, centroidsKey(testNS, 1)) {
		t.Errorf("centroids.json must not be written for an empty namespace")
	}
	if !objectExists(ctx, t, store, docsKey(testNS, 1)) {
		t.Errorf("docs.json must always be written")
	}
	var df DocsFile
	loadJSON(ctx, t, store, docsKey(testNS, 1), &df)
	if len(df.Docs) != 0 {
		t.Errorf("docs.json for empty namespace: got %d docs, want 0", len(df.Docs))
	}
}

func TestBuildIndexTinyNK1(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// A single vector: ChooseK(1) == 1, so exactly one cluster holds it.
	seedWAL(ctx, t, store, testNS, [][]Document{
		{vecDoc("solo", []float32{1, 2, 3, 4}, "only one")},
	})

	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, 1), &cf)
	if cf.K != 1 {
		t.Errorf("K for one vector: got %d, want 1", cf.K)
	}
	if len(cf.Sizes) != 1 || cf.Sizes[0] != 1 {
		t.Errorf("sizes for one vector: got %v, want [1]", cf.Sizes)
	}

	if objectExists(ctx, t, store, clusterKey(testNS, 1, 1)) {
		t.Errorf("cluster-1 must not exist when K == 1")
	}
	var clf ClusterFile
	loadJSON(ctx, t, store, clusterKey(testNS, 1, 0), &clf)
	if len(clf.Members) != 1 || clf.Members[0].ID != "solo" {
		t.Fatalf("cluster-0 members: got %+v, want the single doc solo", clf.Members)
	}
}

func TestBuildIndexSkipsVectorlessDocsInVectorPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	// "hasvec" carries a vector; "textonly" has only a text field. The vector
	// path must skip the text-only doc, but both must appear in docs.json and
	// bm25.json.
	seedWAL(ctx, t, store, testNS, [][]Document{
		{
			vecDoc("hasvec", []float32{1, 0, 0, 0}, "vector and text"),
			{ID: "textonly", Attributes: map[string]any{"body": "text without a vector"}},
		},
	})

	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	// Sum of cluster members must be exactly the one vector-bearing doc.
	var cf CentroidsFile
	loadJSON(ctx, t, store, centroidsKey(testNS, 1), &cf)
	var members int
	for c := 0; c < cf.K; c++ {
		var clf ClusterFile
		loadJSON(ctx, t, store, clusterKey(testNS, 1, c), &clf)
		members += len(clf.Members)
		for _, mem := range clf.Members {
			if mem.ID == "textonly" {
				t.Errorf("text-only doc %q must not appear in the vector index", "textonly")
			}
		}
	}
	if members != 1 {
		t.Errorf("vector index members: got %d, want 1 (only the vector-bearing doc)", members)
	}

	// Both docs are present in docs.json and bm25.json (DocCount counts both).
	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	if m.DocCount != 2 {
		t.Errorf("DocCount: got %d, want 2 (both live docs)", m.DocCount)
	}
	var bf BM25File
	loadJSON(ctx, t, store, bm25Key(testNS, 1), &bf)
	if bf.N != 2 {
		t.Errorf("bm25 N: got %d, want 2", bf.N)
	}
}

func TestBuildIndexIncrementsEpoch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	seedWAL(ctx, t, store, testNS, [][]Document{
		{vecDoc("a", []float32{1, 0, 0, 0}, "first")},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("first BuildIndex: got err %v, want nil", err)
	}
	first, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	if first.IndexEpoch != 1 {
		t.Fatalf("first IndexEpoch: got %d, want 1", first.IndexEpoch)
	}

	// A second build (no new writes) must advance the epoch and write a fresh,
	// separate prefix, leaving the old epoch's objects intact.
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("second BuildIndex: got err %v, want nil", err)
	}
	second, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest: got err %v, want nil", err)
	}
	if second.IndexEpoch != 2 {
		t.Errorf("second IndexEpoch: got %d, want 2", second.IndexEpoch)
	}
	if !objectExists(ctx, t, store, centroidsKey(testNS, 1)) {
		t.Errorf("old epoch v1 centroids must remain (epochs are immutable)")
	}
	if !objectExists(ctx, t, store, centroidsKey(testNS, 2)) {
		t.Errorf("new epoch v2 centroids must be written")
	}
}

func TestBuildIndexNoTextFieldSkipsBM25(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	// A vector-only namespace: no text field, so bm25.json is never written.
	cfg := NamespaceConfig{Dimension: 4, Metric: "euclidean", TextField: ""}
	if err := CreateManifest(ctx, store, testNS, cfg); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	seedWAL(ctx, t, store, testNS, [][]Document{
		{{ID: "a", Vector: []float32{1, 0, 0, 0}}},
	})
	if err := BuildIndex(ctx, store, testNS); err != nil {
		t.Fatalf("BuildIndex: got err %v, want nil", err)
	}

	if objectExists(ctx, t, store, bm25Key(testNS, 1)) {
		t.Errorf("bm25.json must not be written when TextField is empty")
	}
	if !objectExists(ctx, t, store, centroidsKey(testNS, 1)) {
		t.Errorf("centroids.json must be written for a vector namespace")
	}
	if !objectExists(ctx, t, store, docsKey(testNS, 1)) {
		t.Errorf("docs.json must always be written")
	}
}

// sortedKeys returns the sorted keys of a docs map for stable comparison.
func sortedKeys(m map[string]map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
