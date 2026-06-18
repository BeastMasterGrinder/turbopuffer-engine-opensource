package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// nvmeRing is the warm middle tier of tpuf's three-tier cache: a fixed-size
// FIFO ring buffer of cached objects on local disk, modeling turbopuffer's NVMe
// SSD cache (docs/extensions/nvme-ring-buffer-cache.md). It sits UNDER the DRAM
// map and OVER object storage: a DRAM miss consults the ring before paying the
// S3 round-trip, and a backend fetch populates it on the way back.
//
// The defining property is that eviction is FIFO with no recency bookkeeping —
// no LRU list to splice, no access timestamps to bump on every read. A single
// write cursor advances through a bounded set of slots; a new object overwrites
// whatever slot the cursor lands on (the oldest), then the cursor advances and
// wraps. That is the whole eviction policy, which is what keeps the real thing
// ~200 lines and fast: the access pattern that LRU optimizes for costs more
// bookkeeping at NVMe speeds than the slightly-better hit ratio is worth (the
// SOSP'23 S3-FIFO result; see the doc). We implement the honest first cut the
// doc recommends — plain FIFO, no skip-ahead heuristic.
//
// Faithful-but-clear layout: each ring slot is a {sha256(key)}.obj file in the
// ring directory, and an in-memory index (slots []ringSlot + cursor) records
// which key currently occupies each slot. The index is persisted as index.json
// next to the slot files so a process restart finds its warm data instead of
// re-cold-starting from S3 — half the point of the tier. We track byte offsets
// as separate files rather than one big mmap'd file with device-aligned writes;
// a clone can ignore alignment and fragmentation (the doc says to, as long as we
// say so). It is safe for concurrent use.
//
// Correctness note: this tier is ONLY ever fed the immutable index/v{epoch}/*
// objects, exactly the keys Store.GetCached already gates. Those keys are
// write-once, so a cached body can never go stale, and an epoch swap needs no
// invalidation — the new epoch's keys simply differ and the old ones age out as
// the cursor laps them (CAS rule 2). On read we still verify the slot's recorded
// key matches the requested key, so a lapped slot reports a miss rather than
// serving a different object's bytes.
type nvmeRing struct {
	dir      string // directory holding the {hash}.obj slot files + index.json
	capacity int    // number of slots in the ring (> 0)

	mu     sync.Mutex
	slots  []ringSlot     // slots[i] is the object currently in ring position i
	byKey  map[string]int // key -> slot index, for O(1) lookup
	cursor int            // next slot the writer will overwrite (the oldest)

	// Counters for the disk tier, surfaced through CacheStats. Guarded by mu,
	// which every Get/Put already holds, so no separate atomics are needed.
	hits   uint64 // served from a valid slot file
	misses uint64 // key not resident (never cached, or lapped by the cursor)
}

// ringSlot records which key occupies a ring position. An empty Key means the
// slot has never been written (the ring is not yet full). Hash is the slot
// file's basename, persisted so a restart can match files to keys.
type ringSlot struct {
	Key  string `json:"key"`
	Hash string `json:"hash"`
}

// ringIndex is the on-disk form of the ring's metadata, written to index.json
// so the warm tier survives a process restart.
type ringIndex struct {
	Capacity int        `json:"capacity"`
	Cursor   int        `json:"cursor"`
	Slots    []ringSlot `json:"slots"`
}

// newNVMERing opens (or creates) a FIFO ring buffer of capacity slots rooted at
// dir, creating the directory if needed. If dir already holds a compatible
// index.json the ring reloads it, so a restarted process keeps its warm data;
// an index for a different capacity is discarded and the ring starts empty (we
// don't try to resize a ring across restarts — clarity over cleverness).
func newNVMERing(dir string, capacity int) (*nvmeRing, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("nvme ring: capacity must be positive, got %d", capacity)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("nvme ring: creating dir %q: %w", dir, err)
	}

	r := &nvmeRing{
		dir:      dir,
		capacity: capacity,
		slots:    make([]ringSlot, capacity),
		byKey:    make(map[string]int),
	}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// indexPath is the location of the persisted ring metadata.
func (r *nvmeRing) indexPath() string { return filepath.Join(r.dir, "index.json") }

// slotPath is the on-disk location of the object cached in the given slot.
func (r *nvmeRing) slotPath(hash string) string { return filepath.Join(r.dir, hash+".obj") }

// load reads a persisted index.json, rebuilding the in-memory key map. A missing
// or mismatched index leaves the freshly-zeroed (empty) ring in place. Caller
// holds no lock — load runs only during construction, before the ring is shared.
func (r *nvmeRing) load() error {
	data, err := os.ReadFile(r.indexPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first run: empty ring
		}
		return fmt.Errorf("nvme ring: reading index: %w", err)
	}
	var idx ringIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return fmt.Errorf("nvme ring: parsing index: %w", err)
	}
	// Only adopt an index that matches this ring's shape; otherwise start empty
	// rather than mapping keys onto the wrong number of slots.
	if idx.Capacity != r.capacity || len(idx.Slots) != r.capacity {
		return nil
	}
	r.cursor = idx.Cursor % r.capacity
	copy(r.slots, idx.Slots)
	for i, s := range r.slots {
		if s.Key != "" {
			r.byKey[s.Key] = i
		}
	}
	return nil
}

// persist writes the current ring metadata to index.json. Caller must hold r.mu.
// A persist failure is returned to the caller of put; the body is already on
// disk, so the worst case is a stale index after a crash, which load tolerates
// by validating each slot file on read.
func (r *nvmeRing) persist() error {
	idx := ringIndex{Capacity: r.capacity, Cursor: r.cursor, Slots: r.slots}
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("nvme ring: marshaling index: %w", err)
	}
	if err := os.WriteFile(r.indexPath(), data, 0o644); err != nil {
		return fmt.Errorf("nvme ring: writing index: %w", err)
	}
	return nil
}

// hashKey maps an object key to a stable slot-file basename. The hash only names
// the file; the slot's recorded Key is the source of truth for what it holds, so
// a (vanishingly unlikely) hash collision still reads back correctly because Get
// compares the full key.
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// get returns the cached body for key and true on a ring hit, or (nil, false) on
// a miss. A miss means either the key was never cached or the cursor has since
// lapped its slot and overwritten it with another object. On a hit it reads the
// slot file fresh; a read error (e.g. the file was removed out-of-band) is
// treated as a miss so the caller falls through to the backend.
func (r *nvmeRing) get(key string) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	i, ok := r.byKey[key]
	if !ok {
		r.misses++
		return nil, false
	}
	// Defense in depth: the slot must still claim this exact key. byKey is kept
	// in sync with slots, but verifying here makes the "lapped slot" invariant
	// explicit — we never serve one key's bytes for another (see type doc).
	if r.slots[i].Key != key {
		r.misses++
		return nil, false
	}
	body, err := os.ReadFile(r.slotPath(r.slots[i].Hash))
	if err != nil {
		// The file vanished or is unreadable; drop the stale mapping and miss.
		delete(r.byKey, key)
		r.slots[i] = ringSlot{}
		r.misses++
		return nil, false
	}
	r.hits++
	return body, true
}

// put writes body for key into the slot at the cursor, evicting whatever object
// occupied that slot (FIFO: the oldest), then advances and wraps the cursor.
// Writing the same key twice simply lands it in a new slot and abandons the old
// one as a miss-on-next-lap; that is fine because the keys are immutable, so the
// bytes are identical either way. A persist error is returned but the body is
// already durable on disk.
func (r *nvmeRing) put(key string, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If this key is already resident, its existing slot becomes stale once we
	// write the new one; clear the mapping so a later lookup doesn't point at a
	// slot that may get reused for a different key.
	if old, ok := r.byKey[key]; ok {
		r.slots[old] = ringSlot{}
		delete(r.byKey, key)
	}

	slot := r.cursor
	// Evict the current occupant of this slot: delete its file and unmap it.
	if prev := r.slots[slot]; prev.Key != "" {
		delete(r.byKey, prev.Key)
		// Best effort: a delete failure (e.g. already gone) must not block the
		// write — the overwrite below makes the slot authoritative regardless.
		_ = os.Remove(r.slotPath(prev.Hash))
	}

	hash := hashKey(key)
	if err := os.WriteFile(r.slotPath(hash), body, 0o644); err != nil {
		// Leave the slot empty on failure so we never claim to hold bytes we
		// didn't write; the caller still has the body from the backend.
		r.slots[slot] = ringSlot{}
		return fmt.Errorf("nvme ring: writing slot: %w", err)
	}
	r.slots[slot] = ringSlot{Key: key, Hash: hash}
	r.byKey[key] = slot
	r.cursor = (r.cursor + 1) % r.capacity

	return r.persist()
}

// stats returns the disk tier's hit and miss counters. Safe for concurrent use.
func (r *nvmeRing) stats() (hits, misses uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits, r.misses
}
