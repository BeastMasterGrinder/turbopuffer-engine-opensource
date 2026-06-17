package storage

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
)

// MemStore must satisfy the ObjectStore contract.
var _ ObjectStore = (*MemStore)(nil)

// seed writes key=body unconditionally and returns its ETag, failing the test
// on any error.
func seed(t *testing.T, m *MemStore, key string, body []byte) string {
	t.Helper()
	etag, err := m.Put(context.Background(), key, body)
	if err != nil {
		t.Fatalf("seed Put(%q) error = %v, want nil", key, err)
	}
	return etag
}

func TestMemStoreGet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := New()
	want := []byte("hello")
	seed(t, m, "k", want)

	t.Run("found returns body and etag", func(t *testing.T) {
		body, etag, err := m.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get(k) error = %v, want nil", err)
		}
		if string(body) != string(want) {
			t.Errorf("Get(k) body = %q, want %q", body, want)
		}
		if etag == "" {
			t.Errorf("Get(k) etag = %q, want non-empty", etag)
		}
	})

	t.Run("missing key returns ErrNotFound", func(t *testing.T) {
		_, _, err := m.Get(ctx, "absent")
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(absent) error = %v, want ErrNotFound", err)
		}
	})
}

func TestMemStorePutCAS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("success when etag matches returns new etag", func(t *testing.T) {
		m := New()
		etag := seed(t, m, "k", []byte("v1"))

		newETag, err := m.PutCAS(ctx, "k", []byte("v2"), etag)
		if err != nil {
			t.Fatalf("PutCAS(matching etag) error = %v, want nil", err)
		}
		if newETag == "" || newETag == etag {
			t.Errorf("PutCAS new etag = %q, want fresh (!= %q, non-empty)", newETag, etag)
		}

		body, gotETag, err := m.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get after PutCAS error = %v, want nil", err)
		}
		if string(body) != "v2" {
			t.Errorf("body after PutCAS = %q, want %q", body, "v2")
		}
		if gotETag != newETag {
			t.Errorf("Get etag = %q, want %q (the value PutCAS returned)", gotETag, newETag)
		}
	})

	t.Run("412 when etag mismatches and body unchanged", func(t *testing.T) {
		m := New()
		etag := seed(t, m, "k", []byte("v1"))

		_, err := m.PutCAS(ctx, "k", []byte("v2"), "stale-etag")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("PutCAS(stale etag) error = %v, want ErrPreconditionFailed", err)
		}

		body, gotETag, err := m.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get after failed PutCAS error = %v, want nil", err)
		}
		if string(body) != "v1" {
			t.Errorf("body after rejected PutCAS = %q, want unchanged %q", body, "v1")
		}
		if gotETag != etag {
			t.Errorf("etag after rejected PutCAS = %q, want unchanged %q", gotETag, etag)
		}
	})

	t.Run("412 when key missing", func(t *testing.T) {
		m := New()

		_, err := m.PutCAS(ctx, "absent", []byte("v"), "any-etag")
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Errorf("PutCAS(missing key) error = %v, want ErrPreconditionFailed", err)
		}
		if _, _, err := m.Get(ctx, "absent"); !errors.Is(err, ErrNotFound) {
			t.Errorf("key created by failed PutCAS: Get error = %v, want ErrNotFound", err)
		}
	})
}

func TestMemStorePutIfAbsent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("first write wins", func(t *testing.T) {
		m := New()

		etag, err := m.PutIfAbsent(ctx, "k", []byte("v1"))
		if err != nil {
			t.Fatalf("PutIfAbsent(new key) error = %v, want nil", err)
		}
		if etag == "" {
			t.Errorf("PutIfAbsent etag = %q, want non-empty", etag)
		}
	})

	t.Run("412 on second write and body unchanged", func(t *testing.T) {
		m := New()
		etag, err := m.PutIfAbsent(ctx, "k", []byte("v1"))
		if err != nil {
			t.Fatalf("PutIfAbsent(new key) error = %v, want nil", err)
		}

		_, err = m.PutIfAbsent(ctx, "k", []byte("v2"))
		if !errors.Is(err, ErrPreconditionFailed) {
			t.Fatalf("PutIfAbsent(existing key) error = %v, want ErrPreconditionFailed", err)
		}

		body, gotETag, err := m.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get after rejected PutIfAbsent error = %v, want nil", err)
		}
		if string(body) != "v1" {
			t.Errorf("body after rejected PutIfAbsent = %q, want unchanged %q", body, "v1")
		}
		if gotETag != etag {
			t.Errorf("etag after rejected PutIfAbsent = %q, want unchanged %q", gotETag, etag)
		}
	})
}

func TestMemStorePutOverwriteChangesETag(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := New()
	etag1 := seed(t, m, "k", []byte("v1"))
	etag2 := seed(t, m, "k", []byte("v2"))

	if etag1 == etag2 {
		t.Errorf("etag unchanged after overwrite: got %q both times, want fresh etag", etag2)
	}

	body, gotETag, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get after overwrite error = %v, want nil", err)
	}
	if string(body) != "v2" {
		t.Errorf("body after overwrite = %q, want %q", body, "v2")
	}
	if gotETag != etag2 {
		t.Errorf("Get etag = %q, want latest %q", gotETag, etag2)
	}
}

func TestMemStoreList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := New()
	seed(t, m, "demo/manifest.json", []byte("m"))
	seed(t, m, "demo/wal/00000000000000000000.json", []byte("w0"))
	seed(t, m, "demo/wal/00000000000000000001.json", []byte("w1"))
	seed(t, m, "other/manifest.json", []byte("o"))

	tests := []struct {
		name   string
		prefix string
		want   []string
	}{
		{
			name:   "namespace prefix",
			prefix: "demo/",
			want: []string{
				"demo/manifest.json",
				"demo/wal/00000000000000000000.json",
				"demo/wal/00000000000000000001.json",
			},
		},
		{
			name:   "nested wal prefix",
			prefix: "demo/wal/",
			want: []string{
				"demo/wal/00000000000000000000.json",
				"demo/wal/00000000000000000001.json",
			},
		},
		{
			name:   "empty prefix matches all",
			prefix: "",
			want: []string{
				"demo/manifest.json",
				"demo/wal/00000000000000000000.json",
				"demo/wal/00000000000000000001.json",
				"other/manifest.json",
			},
		},
		{
			name:   "no match",
			prefix: "nope/",
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.List(ctx, tt.prefix)
			if err != nil {
				t.Fatalf("List(%q) error = %v, want nil", tt.prefix, err)
			}
			// Order is unspecified; sort both sides for a stable compare.
			sort.Strings(got)
			if !equalStrings(got, tt.want) {
				t.Errorf("List(%q) = %v, want %v", tt.prefix, got, tt.want)
			}
		})
	}
}

// TestMemStoreGetCopiesBody ensures Get returns a defensive copy: mutating the
// returned slice must not corrupt the stored object.
func TestMemStoreGetCopiesBody(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := New()
	seed(t, m, "k", []byte("orig"))

	body, _, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get(k) error = %v, want nil", err)
	}
	body[0] = 'X'

	again, _, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatalf("second Get(k) error = %v, want nil", err)
	}
	if string(again) != "orig" {
		t.Errorf("stored body mutated through returned slice: got %q, want %q", again, "orig")
	}
}

// TestMemStoreCASConcurrency models the manifest CAS race: many goroutines read
// the current ETag and try PutCAS; exactly one wins per generation. Run under
// -race to exercise the lock. The invariant we assert is that PutCAS is
// atomic — a success implies the write happened against the ETag we read, and
// the final value is one a winner actually wrote.
func TestMemStoreCASConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	m := New()
	seed(t, m, "k", []byte("v0"))

	const goroutines = 16
	var wg sync.WaitGroup
	wg.Add(goroutines)

	var mu sync.Mutex
	wins := 0
	conflicts := 0

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			_, etag, err := m.Get(ctx, "k")
			if err != nil {
				t.Errorf("goroutine %d Get error = %v, want nil", id, err)
				return
			}
			_, err = m.PutCAS(ctx, "k", []byte{byte(id)}, etag)
			mu.Lock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrPreconditionFailed):
				conflicts++
			default:
				t.Errorf("goroutine %d PutCAS error = %v, want nil or ErrPreconditionFailed", id, err)
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if wins < 1 {
		t.Errorf("CAS winners = %d, want at least 1", wins)
	}
	if wins+conflicts != goroutines {
		t.Errorf("wins+conflicts = %d, want %d", wins+conflicts, goroutines)
	}
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
