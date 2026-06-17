// Package cache is the DRAM tier that sits in front of an ObjectStore — the
// "everything else exists to hide object-storage latency" half of tpuf's bet.
//
// It is a thin wrapper: Get, PutCAS, PutIfAbsent, Put, and List pass straight
// through to the backend, so the manifest and the WAL are always read fresh
// (correctness rule 2 — caching the manifest would break the CAS retry loop,
// which depends on observing the current ETag every iteration). Only GetCached
// memoizes, and it is meant exclusively for the immutable objects written under
// an index/v{epoch}/ prefix: those keys are write-once, so once a body is read
// it can never change, and serving it from a map is always correct.
//
// The cache can be capacity-bounded (NewWithCapacity). Real DRAM is finite, and
// with many namespaces resident at once their index objects cannot all stay
// cached — so a bounded LRU models the eviction that makes cold-start misses
// recur, which is exactly the behavior turbopuffer's (here-omitted) NVMe tier
// exists to soften. New gives the unbounded cache, which is the right default
// for a single namespace.
package cache

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// Store is an ObjectStore-backed read cache. The zero value is not usable; call
// New or NewWithCapacity. It is safe for concurrent use.
type Store struct {
	backend  storage.ObjectStore
	capacity int // max cached objects; 0 = unbounded

	mu    sync.Mutex
	byKey map[string]*list.Element // key -> element whose Value is *entry
	lru   *list.List               // front = most recently used

	// GetCached counters. A hit is served from the DRAM map; a miss fell through
	// to the backend (a cold read); an eviction dropped the least-recently-used
	// object to stay within capacity. Atomic so reading Stats never contends with
	// concurrent GetCached callers.
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// entry is one cached object; it lives in both byKey and the lru list.
type entry struct {
	key  string
	body []byte
}

// CacheStats is a snapshot of the GetCached counters. Hits were served from
// memory (hot); Misses fell through to the object store (cold); Evictions were
// LRU drops under capacity pressure. The tail scan does not touch GetCached, so
// a phase with all-zero stats simply never consulted the index cache.
type CacheStats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

// HitRate is the fraction of GetCached calls served from memory, in [0,1]. An
// empty snapshot (no calls) reports 0.
func (c CacheStats) HitRate() float64 {
	total := c.Hits + c.Misses
	if total == 0 {
		return 0
	}
	return float64(c.Hits) / float64(total)
}

// Sub returns the delta between two snapshots (later minus earlier), so callers
// can attribute hits, misses, and evictions to a single phase of work.
func (c CacheStats) Sub(earlier CacheStats) CacheStats {
	return CacheStats{
		Hits:      c.Hits - earlier.Hits,
		Misses:    c.Misses - earlier.Misses,
		Evictions: c.Evictions - earlier.Evictions,
	}
}

// New returns an unbounded Store that caches every immutable index read in front
// of backend.
func New(backend storage.ObjectStore) *Store {
	return NewWithCapacity(backend, 0)
}

// NewWithCapacity returns a Store that caches at most capacity objects, evicting
// the least-recently-used on overflow. A capacity of 0 means unbounded.
func NewWithCapacity(backend storage.ObjectStore, capacity int) *Store {
	return &Store{
		backend:  backend,
		capacity: capacity,
		byKey:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Get reads the latest body and ETag for key straight from the backend, with no
// caching. Use it for the manifest and WAL, which must always be fresh.
func (s *Store) Get(ctx context.Context, key string) (body []byte, etag string, err error) {
	return s.backend.Get(ctx, key)
}

// PutCAS passes an If-Match conditional write through to the backend.
func (s *Store) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	return s.backend.PutCAS(ctx, key, body, ifMatch)
}

// PutIfAbsent passes a write-once (If-None-Match: "*") write through to the
// backend.
func (s *Store) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	return s.backend.PutIfAbsent(ctx, key, body)
}

// Put passes an unconditional write through to the backend.
func (s *Store) Put(ctx context.Context, key string, body []byte) (string, error) {
	return s.backend.Put(ctx, key, body)
}

// List passes a prefix listing through to the backend.
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	return s.backend.List(ctx, prefix)
}

// Stats returns the cumulative GetCached counters since construction.
func (s *Store) Stats() CacheStats {
	return CacheStats{Hits: s.hits.Load(), Misses: s.misses.Load(), Evictions: s.evictions.Load()}
}

// GetCached returns the body for key, serving it from memory after the first
// read. It is correct ONLY for immutable objects (the index/v{epoch}/* files):
// because those keys are never overwritten, a cached body can never go stale.
// Never call it for the manifest or WAL.
func (s *Store) GetCached(ctx context.Context, key string) (body []byte, err error) {
	s.mu.Lock()
	if el, ok := s.byKey[key]; ok {
		s.lru.MoveToFront(el)
		b := el.Value.(*entry).body
		s.mu.Unlock()
		s.hits.Add(1)
		return b, nil
	}
	s.mu.Unlock()
	s.misses.Add(1)

	// Fetch outside the lock so a slow backend read doesn't serialize every
	// other cache reader. A concurrent miss on the same key may fetch twice,
	// but the bodies are identical (immutable key), so insert is idempotent.
	b, _, err := s.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.insertLocked(key, b)
	s.mu.Unlock()
	return b, nil
}

// insertLocked adds (or refreshes) key at the front of the LRU and evicts the
// tail while over capacity. Caller must hold s.mu.
func (s *Store) insertLocked(key string, body []byte) {
	if el, ok := s.byKey[key]; ok {
		el.Value.(*entry).body = body
		s.lru.MoveToFront(el)
		return
	}
	s.byKey[key] = s.lru.PushFront(&entry{key: key, body: body})
	for s.capacity > 0 && s.lru.Len() > s.capacity {
		oldest := s.lru.Back()
		if oldest == nil {
			break
		}
		s.lru.Remove(oldest)
		delete(s.byKey, oldest.Value.(*entry).key)
		s.evictions.Add(1)
	}
}
