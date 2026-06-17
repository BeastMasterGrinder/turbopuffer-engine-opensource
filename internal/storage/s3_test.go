//go:build integration

// These tests run against a real S3-compatible backend (MinIO via
// `docker compose up -d`) and are gated behind the `integration` build tag so
// the default `go test ./...` stays infra-free. Run with:
//
//	docker compose up -d
//	set -a; source .env.example; set +a
//	go test ./internal/storage -tags=integration
//
// The headline contract under test is the CAS 412: a stale If-Match PutCAS must
// return ErrPreconditionFailed, which is what the whole manifest-CAS retry loop
// relies on.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// newTestStore builds an S3Store from the TPUF_S3_* env, skipping the test when
// the env is not configured so the suite degrades gracefully on a bare machine.
func newTestStore(t *testing.T) *S3Store {
	t.Helper()
	store, err := NewS3StoreFromEnv()
	if err != nil {
		t.Skipf("skipping integration test: %v (start MinIO and source .env.example)", err)
	}
	return store
}

// uniqueKey returns a per-test key under a fixed prefix so parallel runs and
// reruns never collide.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("itest/%s-%d.bin", t.Name(), time.Now().UnixNano())
}

func TestS3StoreCASContract(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t)

	// Seed the object with a write-once PutIfAbsent and capture its ETag.
	first := []byte("v1")
	etag1, err := store.PutIfAbsent(ctx, key, first)
	if err != nil {
		t.Fatalf("PutIfAbsent seed: got err %v, want nil", err)
	}
	if etag1 == "" {
		t.Fatalf("PutIfAbsent seed: got empty etag, want non-empty")
	}

	// Advance the object with a matching PutCAS; this returns a fresh ETag and
	// makes etag1 stale.
	etag2, err := store.PutCAS(ctx, key, []byte("v2"), etag1)
	if err != nil {
		t.Fatalf("PutCAS with current etag: got err %v, want nil", err)
	}
	if etag2 == etag1 {
		t.Fatalf("PutCAS: got unchanged etag %q, want a new etag", etag2)
	}

	// The core assertion: a PutCAS with the now-stale etag1 must be a 412.
	if _, err := store.PutCAS(ctx, key, []byte("v3"), etag1); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("PutCAS with stale etag: got err %v, want ErrPreconditionFailed", err)
	}

	// The losing write must not have taken effect: the body is still "v2".
	body, etagNow, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after failed CAS: got err %v, want nil", err)
	}
	if !bytes.Equal(body, []byte("v2")) {
		t.Errorf("Get body after failed CAS: got %q, want %q", body, "v2")
	}
	if etagNow != etag2 {
		t.Errorf("Get etag after failed CAS: got %q, want %q", etagNow, etag2)
	}

	t.Cleanup(func() { cleanup(store, key) })
}

func TestS3StorePutIfAbsentContract(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t)
	t.Cleanup(func() { cleanup(store, key) })

	if _, err := store.PutIfAbsent(ctx, key, []byte("once")); err != nil {
		t.Fatalf("PutIfAbsent on absent key: got err %v, want nil", err)
	}

	// Writing the same key again must be rejected: this is the WAL-segment race
	// where the loser gets a 412 and rewrites at the next seq.
	if _, err := store.PutIfAbsent(ctx, key, []byte("twice")); !errors.Is(err, ErrPreconditionFailed) {
		t.Fatalf("PutIfAbsent on existing key: got err %v, want ErrPreconditionFailed", err)
	}

	body, _, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: got err %v, want nil", err)
	}
	if !bytes.Equal(body, []byte("once")) {
		t.Errorf("Get body: got %q, want %q", body, "once")
	}
}

func TestS3StoreGetNotFound(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	key := uniqueKey(t) // never written

	if _, _, err := store.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on missing key: got err %v, want ErrNotFound", err)
	}
}

func TestS3StorePutAndList(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("itest/list-%d/", time.Now().UnixNano())

	keys := []string{prefix + "a.bin", prefix + "b.bin", prefix + "c.bin"}
	for _, k := range keys {
		if _, err := store.Put(ctx, k, []byte(k)); err != nil {
			t.Fatalf("Put %q: got err %v, want nil", k, err)
		}
		t.Cleanup(func() { cleanup(store, k) })
	}

	got, err := store.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List %q: got err %v, want nil", prefix, err)
	}
	if len(got) != len(keys) {
		t.Fatalf("List %q: got %d keys (%v), want %d", prefix, len(got), got, len(keys))
	}

	want := make(map[string]bool, len(keys))
	for _, k := range keys {
		want[k] = true
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("List returned unexpected key %q", k)
		}
	}
}

// cleanup best-effort deletes a test object. Failures are ignored: the test has
// already asserted what it cares about, and leftover itest/ objects are
// harmless.
func cleanup(store *S3Store, key string) {
	_ = store.deleteForTest(context.Background(), key)
}
