package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// keyN builds an immutable-looking index key so the tests read like the real
// GetCached call sites (which only ever pass index/v{epoch}/* keys).
func keyN(n int) string { return fmt.Sprintf("ns/index/v1/cluster-%d.json", n) }

// TestNVMeRingFIFOEviction proves the ring overwrites the OLDEST slot when full:
// a 2-slot ring that has cached a, b then caches c must drop a (the oldest),
// keep b and c, and report a as a miss while b and c hit. No recency is tracked,
// so re-reading b between the b and c writes must NOT save it from eviction —
// that is the whole point of FIFO over LRU.
func TestNVMeRingFIFOEviction(t *testing.T) {
	t.Parallel()
	ring, err := newNVMERing(t.TempDir(), 2)
	if err != nil {
		t.Fatalf("newNVMERing: %v", err)
	}

	a, b, c := keyN(0), keyN(1), keyN(2)
	mustPut(t, ring, a, []byte("A"))
	mustPut(t, ring, b, []byte("B"))

	// Touch a (the oldest) to prove reads do not influence eviction order.
	if _, ok := ring.get(a); !ok {
		t.Fatalf("get(a) before overflow = miss, want hit")
	}

	// Writing c overflows the 2 slots and overwrites the slot at the cursor,
	// which is a's — the oldest written — regardless of a having just been read.
	mustPut(t, ring, c, []byte("C"))

	if _, ok := ring.get(a); ok {
		t.Errorf("get(a) after overflow = hit, want miss (a was the oldest and got overwritten)")
	}
	if body, ok := ring.get(b); !ok || string(body) != "B" {
		t.Errorf("get(b) = (%q, %v), want (\"B\", true) — b must survive", body, ok)
	}
	if body, ok := ring.get(c); !ok || string(body) != "C" {
		t.Errorf("get(c) = (%q, %v), want (\"C\", true)", body, ok)
	}
}

// TestNVMeRingWrapsAndReusesSlots drives the cursor past capacity several times
// to confirm it wraps cleanly: after writing k > capacity objects, exactly the
// last `capacity` are resident and everything older is a miss.
func TestNVMeRingWrapsAndReusesSlots(t *testing.T) {
	t.Parallel()
	const capacity = 3
	ring, err := newNVMERing(t.TempDir(), capacity)
	if err != nil {
		t.Fatalf("newNVMERing: %v", err)
	}

	const total = 10
	for i := 0; i < total; i++ {
		mustPut(t, ring, keyN(i), []byte(fmt.Sprintf("v%d", i)))
	}

	// Only the last `capacity` writes survive the ring.
	for i := 0; i < total; i++ {
		body, ok := ring.get(keyN(i))
		wantResident := i >= total-capacity
		if ok != wantResident {
			t.Errorf("get(key-%d) resident = %v, want %v", i, ok, wantResident)
		}
		if wantResident && string(body) != fmt.Sprintf("v%d", i) {
			t.Errorf("get(key-%d) = %q, want %q", i, body, fmt.Sprintf("v%d", i))
		}
	}
}

// TestNVMeRingReloadsAfterRestart proves the warm tier survives a process
// restart: a second ring opened on the same directory reads back the resident
// objects from index.json + the slot files, instead of cold-starting empty.
func TestNVMeRingReloadsAfterRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	ring, err := newNVMERing(dir, 2)
	if err != nil {
		t.Fatalf("newNVMERing: %v", err)
	}
	mustPut(t, ring, keyN(0), []byte("persisted-0"))
	mustPut(t, ring, keyN(1), []byte("persisted-1"))

	// "Restart": a fresh ring over the same dir must find the warm data.
	reopened, err := newNVMERing(dir, 2)
	if err != nil {
		t.Fatalf("reopen newNVMERing: %v", err)
	}
	for i, want := range map[int]string{0: "persisted-0", 1: "persisted-1"} {
		body, ok := reopened.get(keyN(i))
		if !ok || string(body) != want {
			t.Errorf("after restart get(key-%d) = (%q, %v), want (%q, true)", i, body, ok, want)
		}
	}

	// A ring opened with a different capacity must not adopt the old index — it
	// starts empty rather than mapping keys onto the wrong slot count.
	resized, err := newNVMERing(dir, 5)
	if err != nil {
		t.Fatalf("resize newNVMERing: %v", err)
	}
	if _, ok := resized.get(keyN(0)); ok {
		t.Errorf("resized ring served key-0, want miss (capacity changed → index discarded)")
	}
}

// TestStoreTierPromotionFromNVMe is the headline behavior: when DRAM is too
// small to hold an object but the NVMe ring is large, evicting it from DRAM and
// reading it again must be served from the disk ring (an NVMe hit) WITHOUT a
// second backend round-trip. It also confirms the promoted object lands back in
// DRAM (so the very next read is a DRAM hit).
func TestStoreTierPromotionFromNVMe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := newCountingStore()
	a, b := keyN(0), keyN(1)
	seed(t, backend, a, []byte("A"))
	seed(t, backend, b, []byte("B"))

	// DRAM holds 1 object; NVMe holds 8. Reading a then b evicts a from DRAM but
	// leaves it warm on disk.
	store, err := NewWithNVMe(backend, 1, t.TempDir(), 8)
	if err != nil {
		t.Fatalf("NewWithNVMe: %v", err)
	}

	if _, err := store.GetCached(ctx, a); err != nil { // miss → backend, populate both tiers
		t.Fatalf("GetCached(a): %v", err)
	}
	if _, err := store.GetCached(ctx, b); err != nil { // miss → backend; evicts a from DRAM (cap 1)
		t.Fatalf("GetCached(b): %v", err)
	}
	if got := backend.getCount(a); got != 1 {
		t.Fatalf("backend Get(a) after warmup = %d, want 1", got)
	}

	// a is gone from DRAM but warm on the ring: this read must be an NVMe hit,
	// served with NO further backend round-trip.
	body, err := store.GetCached(ctx, a)
	if err != nil {
		t.Fatalf("GetCached(a) after eviction: %v", err)
	}
	if string(body) != "A" {
		t.Errorf("GetCached(a) = %q, want %q", body, "A")
	}
	if got := backend.getCount(a); got != 1 {
		t.Errorf("backend Get(a) after NVMe hit = %d, want unchanged 1 (the ring served it, not the backend)", got)
	}

	st := store.Stats()
	if st.NVMeHits != 1 {
		t.Errorf("NVMeHits = %d, want 1", st.NVMeHits)
	}

	// The NVMe hit promoted a back into DRAM, so the next read is a DRAM hit and
	// still no backend traffic.
	dramBefore := store.Stats().DRAMHits
	if _, err := store.GetCached(ctx, a); err != nil {
		t.Fatalf("GetCached(a) third: %v", err)
	}
	if got := store.Stats().DRAMHits; got != dramBefore+1 {
		t.Errorf("DRAMHits = %d, want %d (promotion should have put a back in DRAM)", got, dramBefore+1)
	}
	if got := backend.getCount(a); got != 1 {
		t.Errorf("backend Get(a) after DRAM hit = %d, want unchanged 1", got)
	}
}

// TestStoreThreeTierCounters walks one object through all three tiers and checks
// the DRAMHits / NVMeHits / Misses split, plus HitRate counting both fast tiers
// as non-cold. A single key: first read is a cold Miss, second (DRAM) is a
// DRAMHit, and after a forced DRAM eviction a third read is an NVMeHit.
func TestStoreThreeTierCounters(t *testing.T) {
	t.Parallel()

	backend := newCountingStore()
	a, b := keyN(0), keyN(1)
	seed(t, backend, a, []byte("A"))
	seed(t, backend, b, []byte("B"))

	store, err := NewWithNVMe(backend, 1, t.TempDir(), 8)
	if err != nil {
		t.Fatalf("NewWithNVMe: %v", err)
	}

	mustGet(t, store, a) // Miss   (cold; populates DRAM + NVMe)
	mustGet(t, store, a) // DRAMHit
	mustGet(t, store, b) // Miss   (cold; evicts a from DRAM, cap 1)
	mustGet(t, store, a) // NVMeHit (a warm on disk, gone from DRAM)

	st := store.Stats()
	if st.DRAMHits != 1 || st.NVMeHits != 1 || st.Misses != 2 {
		t.Errorf("Stats = {DRAM:%d NVMe:%d Miss:%d}, want {DRAM:1 NVMe:1 Miss:2}", st.DRAMHits, st.NVMeHits, st.Misses)
	}
	// Hits aliases DRAMHits for legacy callers.
	if st.Hits != st.DRAMHits {
		t.Errorf("Hits = %d, want it to alias DRAMHits = %d", st.Hits, st.DRAMHits)
	}
	// Both fast tiers count toward HitRate: 2 fast / 4 total = 0.5.
	if want := 0.5; st.HitRate() != want {
		t.Errorf("HitRate = %v, want %v", st.HitRate(), want)
	}
}

// TestStoreNVMeHitDoesNotReachBackend isolates the load-bearing claim of the
// whole tier: across many DRAM evictions, the backend is touched exactly once
// per distinct key — every recurrence is absorbed by the ring, never S3.
func TestStoreNVMeHitDoesNotReachBackend(t *testing.T) {
	t.Parallel()

	backend := newCountingStore()
	const n = 6
	for i := 0; i < n; i++ {
		seed(t, backend, keyN(i), []byte(fmt.Sprintf("v%d", i)))
	}

	// DRAM capacity 1 forces a DRAM eviction on every distinct read, but the
	// ring is big enough to hold all n objects — so re-reads stay on disk.
	store, err := NewWithNVMe(backend, 1, t.TempDir(), n)
	if err != nil {
		t.Fatalf("NewWithNVMe: %v", err)
	}

	// Two full passes over all keys. Pass 1 is all cold misses (first touch);
	// pass 2 finds everything warm on the ring (DRAM keeps evicting under cap 1).
	for pass := 0; pass < 2; pass++ {
		for i := 0; i < n; i++ {
			mustGet(t, store, keyN(i))
		}
	}

	for i := 0; i < n; i++ {
		if got := backend.getCount(keyN(i)); got != 1 {
			t.Errorf("backend Get(key-%d) = %d, want 1 (the ring must absorb every re-read)", i, got)
		}
	}
	st := store.Stats()
	if st.Misses != n {
		t.Errorf("Misses = %d, want %d (one cold read per distinct key)", st.Misses, n)
	}
	if st.NVMeHits != n {
		t.Errorf("NVMeHits = %d, want %d (the entire second pass)", st.NVMeHits, n)
	}
}

// TestStoreNVMeConcurrent hammers GetCached from many goroutines with DRAM
// capacity 1 so the ring is on the hot path constantly, guarding the ring's
// locking under -race. Every reader must observe the correct immutable body.
func TestStoreNVMeConcurrent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	backend := newCountingStore()
	const n = 8
	for i := 0; i < n; i++ {
		seed(t, backend, keyN(i), []byte(fmt.Sprintf("v%d", i)))
	}
	store, err := NewWithNVMe(backend, 1, t.TempDir(), n)
	if err != nil {
		t.Fatalf("NewWithNVMe: %v", err)
	}

	const goroutines = 32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				k := keyN((g + i) % n)
				want := fmt.Sprintf("v%d", (g+i)%n)
				body, err := store.GetCached(ctx, k)
				if err != nil {
					t.Errorf("GetCached(%q): %v", k, err)
					return
				}
				if string(body) != want {
					t.Errorf("GetCached(%q) = %q, want %q", k, body, want)
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestNewWithNVMeRejectsZeroSlots confirms an invalid ring size is an error, not
// a silent no-op tier.
func TestNewWithNVMeRejectsZeroSlots(t *testing.T) {
	t.Parallel()
	if _, err := NewWithNVMe(storage.New(), 0, t.TempDir(), 0); err == nil {
		t.Errorf("NewWithNVMe(nvmeSlots=0) error = nil, want non-nil")
	}
}

// mustPut writes key=body into the ring, failing the test on error.
func mustPut(t *testing.T, ring *nvmeRing, key string, body []byte) {
	t.Helper()
	if err := ring.put(key, body); err != nil {
		t.Fatalf("ring.put(%q): %v", key, err)
	}
}

// mustGet reads key through the store, failing the test on error.
func mustGet(t *testing.T, store *Store, key string) []byte {
	t.Helper()
	body, err := store.GetCached(context.Background(), key)
	if err != nil {
		t.Fatalf("GetCached(%q): %v", key, err)
	}
	return body
}
