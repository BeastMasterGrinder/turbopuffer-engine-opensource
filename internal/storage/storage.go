// Package storage defines the object-store abstraction that the whole engine
// talks to, plus an in-memory implementation for infra-free tests.
//
// The bet behind tpuf is that object storage is the source of truth. Every
// durable mutation goes through this interface, and the manifest's atomicity
// rests on the conditional-write contract spelled out here: PutCAS maps to an
// S3 If-Match (returning ErrPreconditionFailed on a 412), PutIfAbsent maps to
// If-None-Match: "*" (write-once), and Put is unconditional (immutable index
// files). MemStore honours the exact same contract so the CAS retry loop, the
// WAL PutIfAbsent race, and the indexer epoch swap are all testable without
// MinIO.
package storage

import (
	"context"
	"errors"
)

// ObjectStore is the bucket-scoped object store the engine reads and writes.
// Keys are full object keys (e.g. "demo/manifest.json"); the bucket itself is
// fixed by the concrete implementation.
type ObjectStore interface {
	// Get returns an object's body and its current ETag. It returns ErrNotFound
	// if the key does not exist.
	Get(ctx context.Context, key string) (body []byte, etag string, err error)

	// PutCAS conditionally writes body only if the object's current ETag equals
	// ifMatchETag (an S3 If-Match). It returns the new ETag on success, or
	// ErrPreconditionFailed if the key is missing or its ETag does not match.
	PutCAS(ctx context.Context, key string, body []byte, ifMatchETag string) (newETag string, err error)

	// PutIfAbsent conditionally writes body only if the key does not yet exist
	// (an S3 If-None-Match: "*"). It returns the new ETag on success, or
	// ErrPreconditionFailed if the key already exists.
	PutIfAbsent(ctx context.Context, key string, body []byte) (newETag string, err error)

	// Put unconditionally writes body, overwriting any existing object, and
	// returns the new ETag. Used for immutable index files written under a fresh
	// epoch prefix where no concurrent writer can collide.
	Put(ctx context.Context, key string, body []byte) (newETag string, err error)

	// List returns the keys whose names start with prefix, in no guaranteed
	// order.
	List(ctx context.Context, prefix string) (keys []string, err error)
}

// ErrPreconditionFailed reports that a conditional write (If-Match or
// If-None-Match) was rejected — the S3 412 response that drives the CAS retry
// loop.
var ErrPreconditionFailed = errors.New("storage: precondition failed (412)")

// ErrNotFound reports that a requested key does not exist — the S3 404
// response.
var ErrNotFound = errors.New("storage: not found (404)")
