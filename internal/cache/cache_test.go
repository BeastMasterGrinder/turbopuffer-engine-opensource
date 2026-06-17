package cache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// countingStore wraps an ObjectStore and counts backend Get calls so the tests
// can prove GetCached only reaches the backend once per key. It is safe for
// concurrent use.
type countingStore struct {
	*storage.MemStore

	mu   sync.Mutex
	gets map[string]int
}

func newCountingStore() *countingStore {
	return &countingStore{
		MemStore: storage.New(),
		gets:     make(map[string]int),
	}
}

func (c *countingStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	c.mu.Lock()
	c.gets[key]++
	c.mu.Unlock()
	return c.MemStore.Get(ctx, key)
}

func (c *countingStore) getCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets[key]
}

// seed writes key=body unconditionally, failing the test on error, and returns
// the ETag.
func seed(t *testing.T, store storage.ObjectStore, key string, body []byte) string {
	t.Helper()
	etag, err := store.Put(context.Background(), key, body)
	if err != nil {
		t.Fatalf("seed Put(%q) error = %v, want nil", key, err)
	}
	return etag
}

func TestStoreGetCachedMemoizes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := newCountingStore()
	key := "demo/index/v1/centroids.json"
	want := []byte("immutable-bytes")
	seed(t, backend, key, want)

	s := New(backend)

	first, err := s.GetCached(ctx, key)
	if err != nil {
		t.Fatalf("GetCached(%q) first error = %v, want nil", key, err)
	}
	if string(first) != string(want) {
		t.Errorf("GetCached(%q) first = %q, want %q", key, first, want)
	}
	if got := backend.getCount(key); got != 1 {
		t.Fatalf("backend Get count after first GetCached = %d, want 1", got)
	}

	// Mutate the backing object out from under the cache. A correct cache must
	// keep serving the first (immutable) body and must not hit the backend
	// again — this is safe precisely because index/v{epoch}/ keys never change
	// in real use; here we abuse Put only to prove no backend round-trip.
	if _, err := backend.Put(ctx, key, []byte("tampered")); err != nil {
		t.Fatalf("Put(%q) error = %v, want nil", key, err)
	}

	second, err := s.GetCached(ctx, key)
	if err != nil {
		t.Fatalf("GetCached(%q) second error = %v, want nil", key, err)
	}
	if string(second) != string(want) {
		t.Errorf("GetCached(%q) second = %q, want cached %q (not the tampered backend value)", key, second, want)
	}
	if got := backend.getCount(key); got != 1 {
		t.Errorf("backend Get count after second GetCached = %d, want 1 (served from cache)", got)
	}
}

func TestStoreGetCachedPropagatesError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	s := New(newCountingStore())

	_, err := s.GetCached(ctx, "demo/index/v1/missing.json")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("GetCached(missing) error = %v, want ErrNotFound", err)
	}
}

// TestStoreGetIsUncached is correctness rule 2: Get must never be cached, so a
// fresh backend write is always visible. This is what keeps the manifest CAS
// loop correct.
func TestStoreGetIsUncached(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := newCountingStore()
	key := "demo/manifest.json"
	seed(t, backend, key, []byte("v1"))

	s := New(backend)

	body, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v, want nil", key, err)
	}
	if string(body) != "v1" {
		t.Errorf("Get(%q) = %q, want %q", key, body, "v1")
	}

	if _, err := backend.Put(ctx, key, []byte("v2")); err != nil {
		t.Fatalf("Put(%q) error = %v, want nil", key, err)
	}

	body, _, err = s.Get(ctx, key)
	if err != nil {
		t.Fatalf("second Get(%q) error = %v, want nil", key, err)
	}
	if string(body) != "v2" {
		t.Errorf("Get(%q) after backend write = %q, want latest %q", key, body, "v2")
	}
	if got := backend.getCount(key); got != 2 {
		t.Errorf("backend Get count = %d, want 2 (every Get is a passthrough)", got)
	}
}

// TestStoreCachedAndUncachedAreIndependent proves GetCached pins the body it
// first read while Get continues to reflect later backend writes — the two
// paths must not share state.
func TestStoreCachedAndUncachedAreIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := storage.New()
	key := "demo/index/v1/cluster-0.json"
	seed(t, backend, key, []byte("epoch-bytes"))

	s := New(backend)

	cached, err := s.GetCached(ctx, key)
	if err != nil {
		t.Fatalf("GetCached(%q) error = %v, want nil", key, err)
	}
	if string(cached) != "epoch-bytes" {
		t.Errorf("GetCached(%q) = %q, want %q", key, cached, "epoch-bytes")
	}

	if _, err := backend.Put(ctx, key, []byte("rewritten")); err != nil {
		t.Fatalf("Put(%q) error = %v, want nil", key, err)
	}

	cachedAgain, err := s.GetCached(ctx, key)
	if err != nil {
		t.Fatalf("GetCached(%q) again error = %v, want nil", key, err)
	}
	if string(cachedAgain) != "epoch-bytes" {
		t.Errorf("GetCached(%q) again = %q, want pinned %q", key, cachedAgain, "epoch-bytes")
	}

	fresh, _, err := s.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get(%q) error = %v, want nil", key, err)
	}
	if string(fresh) != "rewritten" {
		t.Errorf("Get(%q) = %q, want latest %q", key, fresh, "rewritten")
	}
}

func TestStorePassthroughs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("PutIfAbsent then conflict", func(t *testing.T) {
		t.Parallel()
		s := New(storage.New())
		key := "demo/wal/00000000000000000000.json"

		etag, err := s.PutIfAbsent(ctx, key, []byte("seg0"))
		if err != nil {
			t.Fatalf("PutIfAbsent(%q) error = %v, want nil", key, err)
		}
		if etag == "" {
			t.Errorf("PutIfAbsent(%q) etag = %q, want non-empty", key, etag)
		}

		if _, err := s.PutIfAbsent(ctx, key, []byte("seg0-dup")); !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Errorf("PutIfAbsent(existing) error = %v, want ErrPreconditionFailed", err)
		}
	})

	t.Run("PutCAS honours ETag", func(t *testing.T) {
		t.Parallel()
		s := New(storage.New())
		key := "demo/manifest.json"

		etag := seed(t, s, key, []byte("v1"))

		if _, err := s.PutCAS(ctx, key, []byte("v2"), "stale"); !errors.Is(err, storage.ErrPreconditionFailed) {
			t.Errorf("PutCAS(stale etag) error = %v, want ErrPreconditionFailed", err)
		}

		newETag, err := s.PutCAS(ctx, key, []byte("v2"), etag)
		if err != nil {
			t.Fatalf("PutCAS(matching etag) error = %v, want nil", err)
		}
		body, gotETag, err := s.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v, want nil", key, err)
		}
		if string(body) != "v2" {
			t.Errorf("body after PutCAS = %q, want %q", body, "v2")
		}
		if gotETag != newETag {
			t.Errorf("Get etag = %q, want %q", gotETag, newETag)
		}
	})

	t.Run("List passes prefix through", func(t *testing.T) {
		t.Parallel()
		s := New(storage.New())
		seed(t, s, "demo/manifest.json", []byte("m"))
		seed(t, s, "demo/index/v1/centroids.json", []byte("c"))
		seed(t, s, "other/manifest.json", []byte("o"))

		got, err := s.List(ctx, "demo/")
		if err != nil {
			t.Fatalf("List(demo/) error = %v, want nil", err)
		}
		sort.Strings(got)
		want := []string{"demo/index/v1/centroids.json", "demo/manifest.json"}
		if len(got) != len(want) {
			t.Fatalf("List(demo/) = %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("List(demo/)[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	})
}

// TestStoreGetCachedConcurrent exercises the cache mutex under -race: many
// goroutines hammer GetCached on the same immutable key and must all observe the
// same body without data races. Backed by a countingStore, it also proves the
// memoization is real under concurrency: the backend is hit at least once but no
// more than once per goroutine (GetCached deliberately does not single-flight, so
// a concurrent miss may fetch more than once but never unboundedly), and a final
// sequential GetCached after the goroutines drain is served purely from the cache.
func TestStoreGetCachedConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := newCountingStore()
	key := "demo/index/v1/bm25.json"
	want := []byte("postings")
	seed(t, backend, key, want)

	s := New(backend)

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			body, err := s.GetCached(ctx, key)
			if err != nil {
				t.Errorf("GetCached(%q) error = %v, want nil", key, err)
				return
			}
			if string(body) != string(want) {
				t.Errorf("GetCached(%q) = %q, want %q", key, body, want)
			}
		}()
	}
	wg.Wait()

	// The cache must have absorbed most of the concurrent traffic: at least one
	// backend fetch happened, and at most one per goroutine. A regression that
	// made GetCached bypass the cache and hit the backend on every call would
	// still land inside [1, goroutines] only by coincidence, so the sequential
	// check below pins memoization down exactly.
	concurrentGets := backend.getCount(key)
	if concurrentGets < 1 || concurrentGets > goroutines {
		t.Errorf("backend Get count after concurrent GetCached = %d, want between 1 and %d", concurrentGets, goroutines)
	}

	// Once the key is cached, a further GetCached must not touch the backend.
	body, err := s.GetCached(ctx, key)
	if err != nil {
		t.Fatalf("GetCached(%q) after drain error = %v, want nil", key, err)
	}
	if string(body) != string(want) {
		t.Errorf("GetCached(%q) after drain = %q, want %q", key, body, want)
	}
	if got := backend.getCount(key); got != concurrentGets {
		t.Errorf("backend Get count after cached GetCached = %d, want unchanged at %d (served from cache)", got, concurrentGets)
	}
}

func TestStatsCountsHitsAndMisses(t *testing.T) {
	ctx := context.Background()
	store := New(newCountingStore())
	const a, b = "ns/index/v1/centroids.json", "ns/index/v1/cluster-0.json"
	if _, err := store.Put(ctx, a, []byte("A")); err != nil {
		t.Fatalf("seed a: %v", err)
	}
	if _, err := store.Put(ctx, b, []byte("B")); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	// First read of each key is a cold miss; the repeat read of a is a hot hit.
	for _, k := range []string{a, b, a} {
		if _, err := store.GetCached(ctx, k); err != nil {
			t.Fatalf("GetCached(%q): %v", k, err)
		}
	}

	got := store.Stats()
	if got.Hits != 1 || got.Misses != 2 {
		t.Errorf("Stats = %+v, want {Hits:1 Misses:2}", got)
	}
	if want := 1.0 / 3.0; got.HitRate() != want {
		t.Errorf("HitRate = %v, want %v", got.HitRate(), want)
	}
}

func TestCacheStatsSubAndEmptyHitRate(t *testing.T) {
	if (CacheStats{}).HitRate() != 0 {
		t.Errorf("empty HitRate = %v, want 0", (CacheStats{}).HitRate())
	}
	delta := CacheStats{Hits: 10, Misses: 4}.Sub(CacheStats{Hits: 7, Misses: 4})
	if delta != (CacheStats{Hits: 3, Misses: 0}) {
		t.Errorf("Sub = %+v, want {Hits:3 Misses:0}", delta)
	}
}

func TestCapacityEvictsLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	backend := newCountingStore()
	store := NewWithCapacity(backend, 2) // hold only 2 objects

	key := func(k string) string { return "ns/index/v1/" + k }
	for _, k := range []string{"a", "b", "c"} {
		if _, err := store.Put(ctx, key(k), []byte(k)); err != nil {
			t.Fatalf("seed %q: %v", k, err)
		}
	}

	// Access a, b (fills the cache to capacity), then a again (now a is the most
	// recently used and b is the LRU), then c. Inserting c overflows capacity 2
	// and evicts the LRU — b. End state: {a, c} resident, b gone.
	for _, k := range []string{"a", "b", "a", "c"} {
		if _, err := store.GetCached(ctx, key(k)); err != nil {
			t.Fatalf("GetCached %q: %v", k, err)
		}
	}
	if got := store.Stats().Evictions; got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}

	// a and c survived: re-reading them must not touch the backend. Check the
	// survivors first — reading the evicted key below forces another eviction.
	for _, k := range []string{"a", "c"} {
		before := backend.getCount(key(k))
		if _, err := store.GetCached(ctx, key(k)); err != nil {
			t.Fatalf("GetCached %q: %v", k, err)
		}
		if got := backend.getCount(key(k)); got != before {
			t.Errorf("%q backend Get count = %d, want unchanged %d (should still be cached)", k, got, before)
		}
	}

	// b was evicted, so reading it again is a cold miss that re-fetches.
	beforeB := backend.getCount(key("b"))
	if _, err := store.GetCached(ctx, key("b")); err != nil {
		t.Fatalf("GetCached b (after eviction): %v", err)
	}
	if got := backend.getCount(key("b")); got != beforeB+1 {
		t.Errorf("b backend Get count = %d, want %d (b was evicted, so it must re-fetch)", got, beforeB+1)
	}
}
