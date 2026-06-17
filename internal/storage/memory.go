package storage

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// MemStore is an in-memory ObjectStore that honours the same conditional-write
// (If-Match / If-None-Match → 412) contract as the real S3/MinIO backend. It
// lets the entire engine — manifest CAS, the WAL PutIfAbsent race, the indexer
// epoch swap — be unit-tested with no Docker and fully deterministically. It is
// safe for concurrent use.
type MemStore struct {
	mu   sync.Mutex
	objs map[string]memObj // key -> {body, etag}
	seq  int64             // monotonic ETag generator
}

type memObj struct {
	body []byte
	etag string
}

// New returns an empty MemStore.
func New() *MemStore {
	return &MemStore{objs: make(map[string]memObj)}
}

// nextETag returns a fresh, monotonically increasing ETag. Callers must hold
// m.mu.
func (m *MemStore) nextETag() string {
	m.seq++
	return strconv.FormatInt(m.seq, 10)
}

// store writes a defensive copy of body under key with a fresh ETag and returns
// that ETag. Callers must hold m.mu.
func (m *MemStore) store(key string, body []byte) string {
	etag := m.nextETag()
	m.objs[key] = memObj{body: bytes.Clone(body), etag: etag}
	return etag
}

// Get implements ObjectStore.
func (m *MemStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.objs[key]
	if !ok {
		return nil, "", fmt.Errorf("get %q: %w", key, ErrNotFound)
	}
	return bytes.Clone(cur.body), cur.etag, nil
}

// PutCAS implements ObjectStore: it writes only if key exists and its current
// ETag equals ifMatchETag, otherwise it returns ErrPreconditionFailed.
func (m *MemStore) PutCAS(ctx context.Context, key string, body []byte, ifMatchETag string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.objs[key]
	if !ok || cur.etag != ifMatchETag {
		return "", fmt.Errorf("put-cas %q: %w", key, ErrPreconditionFailed)
	}
	return m.store(key, body), nil
}

// PutIfAbsent implements ObjectStore: it writes only if key does not yet exist,
// otherwise it returns ErrPreconditionFailed.
func (m *MemStore) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.objs[key]; ok {
		return "", fmt.Errorf("put-if-absent %q: %w", key, ErrPreconditionFailed)
	}
	return m.store(key, body), nil
}

// Put implements ObjectStore: it writes unconditionally, overwriting any
// existing object, and always returns a new ETag.
func (m *MemStore) Put(ctx context.Context, key string, body []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.store(key, body), nil
}

// List implements ObjectStore: it returns every key with the given prefix. The
// order is unspecified (Go map iteration order).
func (m *MemStore) List(ctx context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}
