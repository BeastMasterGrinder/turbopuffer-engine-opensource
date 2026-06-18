package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// maxCASAttempts bounds the read-modify-write retry loop in SaveManifestCAS.
// Ten attempts is generous for an educational single-process clone: each
// conflict means another writer committed first, so we reload and try again.
const maxCASAttempts = 10

// manifestKey returns the object key of a namespace's manifest. The manifest is
// the CAS-coordinated source of truth (correctness rule 2: it is never cached).
func manifestKey(ns string) string {
	return ns + "/manifest.json"
}

// LoadManifest reads a namespace's manifest fresh from the backend, returning it
// alongside the ETag that serves as the CAS token. The read is intentionally
// uncached (correctness rule 2) — every CAS iteration must observe the current
// ETag, so this goes through Store.Get, never GetCached. ErrNotFound surfaces
// unwrapped so callers can branch on errors.Is.
func LoadManifest(ctx context.Context, store *cache.Store, ns string) (Manifest, string, error) {
	body, etag, err := store.Get(ctx, manifestKey(ns))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return Manifest{}, "", err
		}
		return Manifest{}, "", fmt.Errorf("loading manifest for %q: %w", ns, err)
	}

	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return Manifest{}, "", fmt.Errorf("decoding manifest for %q: %w", ns, err)
	}
	return m, etag, nil
}

// marshalManifest encodes a manifest to its on-disk JSON form, naming the error
// the way every writer wants it. It is the single place manifests are marshaled,
// so CreateManifest, BranchFrom, and the CAS loop all serialize identically.
func marshalManifest(m Manifest) ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("encoding manifest: %w", err)
	}
	return body, nil
}

// CreateManifest writes the initial manifest for a new namespace using a
// write-once PutIfAbsent, so two concurrent creators can never both succeed
// (correctness rule 1's write-once cousin). The 412 loser gets a clear
// "already exists" error rather than a precondition-failed surprise.
func CreateManifest(ctx context.Context, store *cache.Store, ns string, cfg NamespaceConfig) error {
	m := Manifest{
		Version:     1,
		Dimension:   cfg.Dimension,
		Metric:      cfg.Metric,
		TextField:   cfg.TextField,
		WALSeq:      0,
		IndexedUpTo: 0,
		IndexEpoch:  0,
		DocCount:    0,
	}

	body, err := marshalManifest(m)
	if err != nil {
		return fmt.Errorf("encoding manifest for %q: %w", ns, err)
	}

	if _, err := store.PutIfAbsent(ctx, manifestKey(ns), body); err != nil {
		if errors.Is(err, storage.ErrPreconditionFailed) {
			return fmt.Errorf("namespace %q already exists", ns)
		}
		return fmt.Errorf("creating manifest for %q: %w", ns, err)
	}
	return nil
}

// SaveManifestCAS applies mutate to the manifest under a compare-and-swap loop:
// each attempt reads the manifest fresh (correctness rule 2 — never cached),
// applies mutate, bumps Version, and conditionally writes with If-Match set to
// the ETag just observed. A 412 means another writer won the race, so we reload
// and retry against the new state; any other error aborts. On success the
// committed manifest is returned. After maxCASAttempts conflicts it gives up
// with an error rather than spinning forever.
func SaveManifestCAS(ctx context.Context, store *cache.Store, ns string, mutate func(*Manifest)) (Manifest, error) {
	for attempt := 0; attempt < maxCASAttempts; attempt++ {
		m, etag, err := LoadManifest(ctx, store, ns)
		if err != nil {
			return Manifest{}, err
		}

		mutate(&m)
		m.Version++

		body, err := json.Marshal(m)
		if err != nil {
			return Manifest{}, fmt.Errorf("encoding manifest for %q: %w", ns, err)
		}

		_, err = store.PutCAS(ctx, manifestKey(ns), body, etag)
		if err == nil {
			return m, nil
		}
		if errors.Is(err, storage.ErrPreconditionFailed) {
			continue
		}
		return Manifest{}, fmt.Errorf("saving manifest for %q: %w", ns, err)
	}

	return Manifest{}, fmt.Errorf("saving manifest for %q: exhausted %d CAS attempts", ns, maxCASAttempts)
}
