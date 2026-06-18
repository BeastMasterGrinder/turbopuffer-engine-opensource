package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// maxUpsertAttempts bounds the WAL-append claim loop in Upsert. Each 412 means
// a concurrent upsert claimed our seq first (correctness rule 1), so we climb to
// the next seq and retry. Because every attempt advances the seq, W writers that
// collide at the same starting seq need at most W probes for the last one to
// find a free slot; this bound comfortably exceeds the contention a
// single-process educational clone sees, and the manifest CAS that follows has
// its own separate retry budget.
const maxUpsertAttempts = 64

// Namespace is the engine's public handle to one logical collection. It is a
// thin, stateless façade over the object store: every method reads the manifest
// fresh and coordinates through CAS, so a Namespace value holds no cached state
// and is safe to share across goroutines (the concurrency lives in the store
// and the CAS loops, not here). Construct one with Open.
type Namespace struct {
	store *cache.Store
	name  string
}

// Open returns a handle to the namespace called name, backed by store. It does
// not touch the object store — call Create to materialize a new namespace, or
// any of the read/write methods to operate on an existing one.
func Open(store *cache.Store, name string) *Namespace {
	return &Namespace{store: store, name: name}
}

// Create materializes a brand-new namespace by writing its initial manifest
// write-once (PutIfAbsent). Two concurrent creators can never both succeed: the
// loser gets a clear "already exists" error from CreateManifest rather than a
// precondition surprise. The config is immutable after this point. The error is
// passed through unwrapped because CreateManifest already names the namespace.
func (n *Namespace) Create(ctx context.Context, cfg NamespaceConfig) error {
	return CreateManifest(ctx, n.store, n.name, cfg)
}

// Branch creates a copy-on-write fork of this namespace named child, forking at
// this namespace's current head. The child shares this namespace's immutable WAL
// segments and index epoch by reference (zero data objects copied — an O(1)
// manifest PUT regardless of size) and diverges only as new writes land on
// either side; the two are fully independent afterward, each its own CAS head
// (docs/extensions/branches-copy-on-write.md). The child inherits this
// namespace's schema; it cannot choose a different vector shape because it shares
// the index. The error is passed through unwrapped because BranchFrom already
// names both namespaces.
//
// GC pin (no garbage collector exists today): the child PINS this namespace's
// objects — see the hazard note at the top of branch.go.
func (n *Namespace) Branch(ctx context.Context, child string) error {
	return BranchFrom(ctx, n.store, n.name, child)
}

// Upsert durably appends docs to the namespace and returns only once they are
// committed (durable-before-return): the WAL segment is written first, then the
// manifest CAS advances WALSeq so the data is part of the namespace's source of
// truth before any caller observes success. Tombstones (Deleted == true) ride
// the same path and remove documents at materialize time.
//
// Vectors are validated against the namespace dimension up front (the error
// reports both the offending vector's dimension and the namespace's), so a
// malformed batch never reaches the WAL. A nil/zero-length vector is allowed —
// it is a text-only document — only a present-but-wrong-length vector is
// rejected.
//
// The write obeys correctness rule 1. We claim the next seq with PutIfAbsent;
// if a concurrent upsert already took it we get storage.ErrPreconditionFailed,
// reload the manifest (now advertising a higher WALSeq), and rewrite our ops at
// the fresh seq. Only after the segment is durable do we CAS the manifest to
// bump WALSeq and DocCount, so the WAL never advertises a seq whose segment is
// missing. An empty batch is a no-op that still touches nothing.
func (n *Namespace) Upsert(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	m, _, err := LoadManifest(ctx, n.store, n.name)
	if err != nil {
		return fmt.Errorf("upserting into %q: %w", n.name, err)
	}
	if err := n.validateDocs(m.Dimension, docs); err != nil {
		return fmt.Errorf("upserting into %q: %w", n.name, err)
	}

	return n.commitBatch(ctx, m.WALSeq, docs)
}

// commitBatch is the durable write path shared by the direct Upsert above and
// the group-commit Committer (commit.go): given a starting seq hint and a
// (possibly merged) batch of validated docs, it claims a WAL seq with a single
// PutIfAbsent loop and advances the manifest with a single CAS. Validation is
// deliberately NOT done here — the caller validates each writer's docs against
// the manifest dimension before they are merged, so one malformed vector fails
// only its own caller rather than poisoning a coalesced batch (docs/extensions/
// group-commit.md). seqHint is just where the PutIfAbsent probe starts; the 412
// loop and the CAS both tolerate a stale hint.
func (n *Namespace) commitBatch(ctx context.Context, seqHint int64, docs []Document) error {
	// liveDelta is the net change to the live document count this batch makes:
	// +1 per upsert, -1 per tombstone. It is informational (DocCount is
	// authoritatively recomputed at index time), so we clamp it at zero to keep
	// the manifest's count non-negative when deletes outrun known docs.
	liveDelta := 0
	for _, d := range docs {
		if d.Deleted {
			liveDelta--
		} else {
			liveDelta++
		}
	}

	// Claim a WAL seq with PutIfAbsent (rule 1). On a 412 a concurrent upsert
	// already took this slot, so we climb to the next seq and try again. Probing
	// forward — rather than re-reading the manifest, whose WALSeq still lags
	// until the winning writer's CAS lands — guarantees forward progress: N
	// concurrent writers that all start at the same seq settle into N distinct
	// slots in at most N probes, so the loop never livelocks on a stale count.
	seq := seqHint
	committedSeq := int64(-1)
	for attempt := 0; attempt < maxUpsertAttempts; attempt++ {
		err := AppendWAL(ctx, n.store, n.name, seq, docs)
		if err == nil {
			committedSeq = seq
			break
		}
		if !errors.Is(err, storage.ErrPreconditionFailed) {
			return fmt.Errorf("upserting into %q: %w", n.name, err)
		}
		seq++
	}
	if committedSeq < 0 {
		return fmt.Errorf("upserting into %q: exhausted %d WAL append attempts", n.name, maxUpsertAttempts)
	}

	// The segment is durable; now advance the manifest. SaveManifestCAS reloads
	// fresh each iteration, so it composes correctly with another writer that
	// bumped WALSeq between our append and this CAS: we always extend past the
	// highest committed seq.
	if _, err := SaveManifestCAS(ctx, n.store, n.name, func(m *Manifest) {
		if next := committedSeq + 1; next > m.WALSeq {
			m.WALSeq = next
		}
		if m.DocCount += liveDelta; m.DocCount < 0 {
			m.DocCount = 0
		}
	}); err != nil {
		return fmt.Errorf("upserting into %q: %w", n.name, err)
	}
	return nil
}

// validateDocs rejects any document whose vector is present but does not match
// the namespace dimension, reporting both numbers (correctness edge case from
// docs/06). A document with no vector is valid: it is a text-only record.
func (n *Namespace) validateDocs(dim int, docs []Document) error {
	for _, d := range docs {
		if d.Vector != nil && len(d.Vector) != dim {
			return fmt.Errorf("document %q has vector dimension %d, namespace dimension is %d", d.ID, len(d.Vector), dim)
		}
	}
	return nil
}

// Index folds the durable WAL into a fresh, immutable epoch and publishes it
// with a single atomic manifest CAS. Writes that land mid-build fall into the
// next epoch's coverage and stay searchable via the query tail (correctness
// rules 3 and 4 live in BuildIndex). The error is passed through unwrapped
// because BuildIndex already names the namespace.
//
// This is the INLINE index path — synchronous, in the calling process. It stays
// fully supported alongside the async broker/indexer/queue.json scheduling
// (docs/extensions/broker-indexer-queue.md): the daemons just call this same
// BuildIndex from a separate process, so nothing about the publish changes.
func (n *Namespace) Index(ctx context.Context) error {
	return BuildIndex(ctx, n.store, n.name)
}

// EnqueueReindexIfBehind notes that this namespace may need (re)indexing IF its
// unindexed WAL tail has grown past threshold segments — the clone's analog of
// turbopuffer's unindexed_bytes (docs/extensions/broker-indexer-queue.md). It is
// a best-effort NOTIFICATION layered on top of the already-durable write path,
// never part of it: durability lives on the WAL (rule 1), and if this enqueue
// lagged or failed no data is lost because queries still scan the WAL tail (rule
// 5) until an indexer catches up. The enqueue itself is idempotent and
// deduplicated on namespace, so a writer may call this after every Upsert.
//
// It reads the manifest FRESH (rule 2, via Info — never cached) to measure the
// lag WALSeq - IndexedUpTo, and returns whether a NEW job was enqueued. A
// threshold of 0 or negative means "enqueue whenever there is any unindexed WAL".
func (n *Namespace) EnqueueReindexIfBehind(ctx context.Context, threshold int64) (bool, error) {
	m, err := n.Info(ctx)
	if err != nil {
		return false, err
	}
	lag := m.WALSeq - m.IndexedUpTo
	if lag <= 0 || lag < threshold {
		return false, nil
	}
	added, err := EnqueueReindex(ctx, n.store, n.name, m.WALSeq)
	if err != nil {
		return false, fmt.Errorf("enqueuing reindex for %q: %w", n.name, err)
	}
	return added, nil
}

// Query answers p against the live index and the unindexed WAL tail, so freshly
// upserted data is searchable before it is indexed (correctness rule 5 lives in
// RunQuery). It returns TopK hits ranked nearest-first (vector mode) or
// highest-score-first (BM25 mode). The error is passed through unwrapped
// because RunQuery already names the namespace.
func (n *Namespace) Query(ctx context.Context, p QueryParams) ([]QueryResult, error) {
	return RunQuery(ctx, n.store, n.name, p)
}

// Info returns the namespace's current manifest, read fresh from the object
// store (never cached, per correctness rule 2). The ETag is intentionally
// dropped: callers that need to mutate go through the CAS helpers, which
// re-read it themselves.
func (n *Namespace) Info(ctx context.Context) (Manifest, error) {
	m, _, err := LoadManifest(ctx, n.store, n.name)
	if err != nil {
		return Manifest{}, fmt.Errorf("reading info for %q: %w", n.name, err)
	}
	return m, nil
}
