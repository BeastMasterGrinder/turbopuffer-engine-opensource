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
// recur. New gives the unbounded cache, which is the right default for a single
// namespace.
//
// Under the DRAM map sits an OPTIONAL second cache tier: a fixed-size FIFO ring
// buffer on local disk (nvme.go), modeling turbopuffer's NVMe SSD tier
// (docs/extensions/nvme-ring-buffer-cache.md). When enabled (NewWithNVMe), a
// DRAM miss consults the ring before paying the object-storage round-trip, and a
// backend fetch populates BOTH tiers on the way back — so a DRAM eviction
// becomes a fast local-disk read instead of a cold S3 fetch on the next access.
// The ring is nil by default, so every existing caller keeps today's two-tier
// (DRAM → backend) behavior unchanged. Like the DRAM map, the disk tier caches
// ONLY the immutable index/v{epoch}/* objects GetCached gates, so its bodies can
// never go stale.
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

	disk *nvmeRing // optional NVMe (local-disk) tier under the DRAM map; nil = off

	mu    sync.Mutex
	byKey map[string]*list.Element // key -> element whose Value is *entry
	lru   *list.List               // front = most recently used

	// GetCached counters. A DRAM hit is served from the in-memory map; an NVMe
	// hit was promoted from the local-disk ring (fast, no network); a miss fell
	// all the way through to the backend (the only true cold read); an eviction
	// dropped the least-recently-used object from DRAM to stay within capacity.
	// Atomic so reading Stats never contends with concurrent GetCached callers.
	dramHits  atomic.Uint64
	nvmeHits  atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// entry is one cached object; it lives in both byKey and the lru list.
type entry struct {
	key  string
	body []byte
}

// CacheStats is a snapshot of the GetCached counters, split across the three
// cache tiers. A GetCached call resolves to exactly one of: a DRAM hit (served
// from the in-memory map), an NVMe hit (promoted from the local-disk ring), or a
// Miss (fell through to object storage — the only true cold read). Evictions are
// LRU drops from DRAM under capacity pressure. The tail scan does not touch
// GetCached, so a phase with all-zero stats simply never consulted the index
// cache.
//
// Hits is retained as DRAMHits so existing callers (and the bench's two-tier
// panel) keep working: with the NVMe tier off, the only fast path is DRAM, so
// Hits == DRAMHits is exactly "served from memory" as before. When the disk tier
// is on, read DRAMHits/NVMeHits/Misses for the full three-way breakdown.
type CacheStats struct {
	Hits      uint64 // DRAM hits; alias of DRAMHits, kept for existing callers
	DRAMHits  uint64 // served from the in-memory map (tier 1)
	NVMeHits  uint64 // promoted from the local-disk ring (tier 2)
	Misses    uint64 // fell through to object storage (tier 3, the cold read)
	Evictions uint64 // LRU drops from DRAM under capacity pressure
}

// HitRate is the fraction of GetCached calls served WITHOUT a backend round-trip
// (from DRAM or the NVMe ring), in [0,1]. An empty snapshot (no calls) reports 0.
// With the disk tier off this is just the DRAM hit rate, as before.
func (c CacheStats) HitRate() float64 {
	total := c.DRAMHits + c.NVMeHits + c.Misses
	if total == 0 {
		return 0
	}
	return float64(c.DRAMHits+c.NVMeHits) / float64(total)
}

// Sub returns the delta between two snapshots (later minus earlier), so callers
// can attribute the three-way tier split, misses, and evictions to a single
// phase of work.
func (c CacheStats) Sub(earlier CacheStats) CacheStats {
	return CacheStats{
		Hits:      c.Hits - earlier.Hits,
		DRAMHits:  c.DRAMHits - earlier.DRAMHits,
		NVMeHits:  c.NVMeHits - earlier.NVMeHits,
		Misses:    c.Misses - earlier.Misses,
		Evictions: c.Evictions - earlier.Evictions,
	}
}

// New returns an unbounded Store that caches every immutable index read in front
// of backend.
func New(backend storage.ObjectStore) *Store {
	return NewWithCapacity(backend, 0)
}

// NewWithCapacity returns a Store that caches at most capacity objects in DRAM,
// evicting the least-recently-used on overflow. A capacity of 0 means unbounded.
// The NVMe tier is off; for that warm tier use NewWithNVMe.
func NewWithCapacity(backend storage.ObjectStore, capacity int) *Store {
	return &Store{
		backend:  backend,
		capacity: capacity,
		byKey:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// NewWithNVMe returns a Store with both cache tiers active: a DRAM map of at most
// dramCapacity objects (0 = unbounded) over a FIFO ring buffer of nvmeSlots
// objects on local disk rooted at dir. On a DRAM miss the ring is consulted
// before the backend; on a backend fetch both tiers are populated. It models
// turbopuffer's full three-tier DRAM/NVMe/S3 cache
// (docs/extensions/nvme-ring-buffer-cache.md). nvmeSlots must be positive; if dir
// already holds a compatible ring index the warm data is reloaded so a restart
// does not re-cold-start from object storage.
func NewWithNVMe(backend storage.ObjectStore, dramCapacity int, dir string, nvmeSlots int) (*Store, error) {
	ring, err := newNVMERing(dir, nvmeSlots)
	if err != nil {
		return nil, err
	}
	s := NewWithCapacity(backend, dramCapacity)
	s.disk = ring
	return s, nil
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

// Stats returns the cumulative GetCached counters since construction, including
// the NVMe ring's hit/miss split when the disk tier is enabled. The miss count
// reported here is the TRUE cold count — calls that fell through both the DRAM
// map and the ring to the backend — so DRAMHits + NVMeHits + Misses is the total
// number of GetCached calls.
func (s *Store) Stats() CacheStats {
	st := CacheStats{
		Hits:      s.dramHits.Load(),
		DRAMHits:  s.dramHits.Load(),
		NVMeHits:  s.nvmeHits.Load(),
		Misses:    s.misses.Load(),
		Evictions: s.evictions.Load(),
	}
	return st
}

// GetCached returns the body for key, serving it from cache after the first
// read. It walks the tiers in order: the DRAM map, then (if enabled) the NVMe
// ring, then the backend. A backend fetch populates BOTH cache tiers on the way
// back, so the next access skips the network. It is correct ONLY for immutable
// objects (the index/v{epoch}/* files): because those keys are never
// overwritten, a cached body can never go stale at either tier. Never call it
// for the manifest or WAL.
func (s *Store) GetCached(ctx context.Context, key string) (body []byte, err error) {
	// Tier 1: DRAM map.
	s.mu.Lock()
	if el, ok := s.byKey[key]; ok {
		s.lru.MoveToFront(el)
		b := el.Value.(*entry).body
		s.mu.Unlock()
		s.dramHits.Add(1)
		return b, nil
	}
	s.mu.Unlock()

	// Tier 2: NVMe ring (warm local disk). A ring hit is promoted into DRAM so
	// the hottest objects climb to the fastest tier, then returned without ever
	// touching the network.
	if s.disk != nil {
		if b, ok := s.disk.get(key); ok {
			s.nvmeHits.Add(1)
			s.mu.Lock()
			s.insertLocked(key, b)
			s.mu.Unlock()
			return b, nil
		}
	}

	// Tier 3: object storage (the cold read). Fetch outside the lock so a slow
	// backend read doesn't serialize every other cache reader. A concurrent miss
	// on the same key may fetch twice, but the bodies are identical (immutable
	// key), so populating both tiers is idempotent.
	s.misses.Add(1)
	b, _, err := s.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// Populate the warm tier first, then DRAM. A ring put failure must not fail
	// the read — the body is already in hand — so we ignore it (the next access
	// simply re-fetches from the backend).
	if s.disk != nil {
		_ = s.disk.put(key, b)
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
