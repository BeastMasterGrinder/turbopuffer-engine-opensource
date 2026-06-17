package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

const testNS = "demo"

func testConfig() NamespaceConfig {
	return NamespaceConfig{Dimension: 4, Metric: "cosine", TextField: "body"}
}

// newTestStore returns a cache.Store backed by a fresh in-memory object store.
func newTestStore() *cache.Store {
	return cache.New(storage.New())
}

func TestCreateManifest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	cfg := testConfig()

	if err := CreateManifest(ctx, store, testNS, cfg); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	m, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest after create: got err %v, want nil", err)
	}

	if m.Dimension != cfg.Dimension {
		t.Errorf("Dimension: got %d, want %d", m.Dimension, cfg.Dimension)
	}
	if m.Metric != cfg.Metric {
		t.Errorf("Metric: got %q, want %q", m.Metric, cfg.Metric)
	}
	if m.TextField != cfg.TextField {
		t.Errorf("TextField: got %q, want %q", m.TextField, cfg.TextField)
	}
	if m.Version != 1 {
		t.Errorf("Version: got %d, want 1", m.Version)
	}
	if m.WALSeq != 0 || m.IndexedUpTo != 0 || m.IndexEpoch != 0 || m.DocCount != 0 {
		t.Errorf("fresh manifest counters: got walSeq=%d indexedUpTo=%d epoch=%d docCount=%d, want all 0",
			m.WALSeq, m.IndexedUpTo, m.IndexEpoch, m.DocCount)
	}
}

func TestCreateManifestExistsGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()
	cfg := testConfig()

	if err := CreateManifest(ctx, store, testNS, cfg); err != nil {
		t.Fatalf("first CreateManifest: got err %v, want nil", err)
	}

	err := CreateManifest(ctx, store, testNS, cfg)
	if err == nil {
		t.Fatalf("second CreateManifest: got nil error, want already-exists error")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("second CreateManifest error: got %q, want it to mention %q", err.Error(), "already exists")
	}
}

func TestLoadManifestNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	_, _, err := LoadManifest(ctx, store, "missing")
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("LoadManifest on missing namespace: got err %v, want ErrNotFound", err)
	}
}

func TestSaveManifestCASSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	got, err := SaveManifestCAS(ctx, store, testNS, func(m *Manifest) {
		m.WALSeq = 7
		m.DocCount = 3
	})
	if err != nil {
		t.Fatalf("SaveManifestCAS: got err %v, want nil", err)
	}

	if got.WALSeq != 7 {
		t.Errorf("returned WALSeq: got %d, want 7", got.WALSeq)
	}
	if got.DocCount != 3 {
		t.Errorf("returned DocCount: got %d, want 3", got.DocCount)
	}
	if got.Version != 2 {
		t.Errorf("returned Version: got %d, want 2 (1 from create + 1 from this save)", got.Version)
	}

	// The mutation must be durably committed, not just returned.
	reloaded, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest after save: got err %v, want nil", err)
	}
	if reloaded.WALSeq != 7 || reloaded.Version != 2 {
		t.Errorf("reloaded manifest: got walSeq=%d version=%d, want walSeq=7 version=2",
			reloaded.WALSeq, reloaded.Version)
	}
}

// conflictStore wraps an ObjectStore and, exactly once, injects a competing
// write immediately before the first PutCAS. That bumps the live ETag so the
// caller's If-Match (taken from the now-stale read) fails with a 412, exercising
// the reload-and-retry path without racing goroutines.
type conflictStore struct {
	storage.ObjectStore
	mu          sync.Mutex
	injected    bool
	casAttempts int
}

func (c *conflictStore) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
	c.mu.Lock()
	c.casAttempts++
	if !c.injected {
		c.injected = true
		c.mu.Unlock()
		// A competing writer commits first, invalidating the caller's ETag.
		if _, err := c.ObjectStore.Put(ctx, key, []byte(`{"version":99}`)); err != nil {
			return "", err
		}
		return c.ObjectStore.PutCAS(ctx, key, body, ifMatch)
	}
	c.mu.Unlock()
	return c.ObjectStore.PutCAS(ctx, key, body, ifMatch)
}

func TestSaveManifestCASRetriesOn412(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cs := &conflictStore{ObjectStore: storage.New()}
	store := cache.New(cs)

	if err := CreateManifest(ctx, store, testNS, testConfig()); err != nil {
		t.Fatalf("CreateManifest: got err %v, want nil", err)
	}

	got, err := SaveManifestCAS(ctx, store, testNS, func(m *Manifest) {
		m.IndexEpoch = 5
	})
	if err != nil {
		t.Fatalf("SaveManifestCAS under conflict: got err %v, want nil", err)
	}

	if cs.casAttempts < 2 {
		t.Errorf("PutCAS attempts: got %d, want >= 2 (first 412, then retry)", cs.casAttempts)
	}
	if got.IndexEpoch != 5 {
		t.Errorf("returned IndexEpoch: got %d, want 5", got.IndexEpoch)
	}

	// The retry must have re-read the competing writer's state (version 99)
	// before re-applying the mutation, so the final committed version reflects
	// that reload rather than the pre-conflict value.
	reloaded, _, err := LoadManifest(ctx, store, testNS)
	if err != nil {
		t.Fatalf("LoadManifest after conflict: got err %v, want nil", err)
	}
	if reloaded.IndexEpoch != 5 {
		t.Errorf("reloaded IndexEpoch: got %d, want 5", reloaded.IndexEpoch)
	}
	if reloaded.Version != 100 {
		t.Errorf("reloaded Version: got %d, want 100 (competing version 99 + 1 from retry)", reloaded.Version)
	}
}

func TestSaveManifestCASMissingNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newTestStore()

	_, err := SaveManifestCAS(ctx, store, "missing", func(m *Manifest) { m.WALSeq = 1 })
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("SaveManifestCAS on missing namespace: got err %v, want ErrNotFound", err)
	}
}
