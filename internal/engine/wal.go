package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/farjad/turbopuffer-clone/internal/cache"
)

// walKey returns the object key of WAL segment seq for ns. The sequence number
// is zero-padded to 20 digits so the keys sort lexically in the same order they
// were written — a List of the wal/ prefix yields the segments in seq order
// without a numeric parse.
func walKey(ns string, seq int64) string {
	return fmt.Sprintf("%s/wal/%020d.json", ns, seq)
}

// AppendWAL durably writes a WAL segment at wal/{seq:020d}.json using a
// write-once PutIfAbsent (correctness rule 1): two concurrent upserts that both
// read the same WALSeq must not clobber each other's segment. The loser of the
// race gets storage.ErrPreconditionFailed and is expected to reload the
// manifest (which now advertises a higher WALSeq), rewrite its ops at the new
// seq, and only then CAS the manifest. AppendWAL itself does not retry; it
// surfaces the 412 so the namespace upsert can drive that loop with the freshly
// observed seq. The WAL is never cached, so this goes straight to the backend.
func AppendWAL(ctx context.Context, store *cache.Store, ns string, seq int64, ops []Document) error {
	seg := WALSegment{Seq: seq, Ops: ops}
	body, err := json.Marshal(seg)
	if err != nil {
		return fmt.Errorf("marshaling wal segment %d: %w", seq, err)
	}

	if _, err := store.PutIfAbsent(ctx, walKey(ns, seq), body); err != nil {
		// Wrap with %w so a 412 stays inspectable via
		// errors.Is(err, storage.ErrPreconditionFailed): the caller (Upsert)
		// detects that and retries at a freshly observed seq.
		return fmt.Errorf("appending wal segment %d: %w", seq, err)
	}
	return nil
}

// ReadWAL fetches and decodes WAL segment seq for ns. It reads uncached
// (correctness rule 2) and returns a wrapped storage.ErrNotFound if the segment
// does not exist.
func ReadWAL(ctx context.Context, store *cache.Store, ns string, seq int64) (WALSegment, error) {
	body, _, err := store.Get(ctx, walKey(ns, seq))
	if err != nil {
		return WALSegment{}, fmt.Errorf("reading wal segment %d: %w", seq, err)
	}

	var seg WALSegment
	if err := json.Unmarshal(body, &seg); err != nil {
		return WALSegment{}, fmt.Errorf("decoding wal segment %d: %w", seq, err)
	}
	return seg, nil
}

// MaterializeLive folds the WAL segments [from, to) in seq order and returns the
// surviving documents keyed by id, applying last-writer-wins per id with
// tombstones removed (a Deleted op deletes any prior document for that id). It
// is the read side of the WAL: the indexer materializes [0, walUpTo) to build an
// epoch, and a query materializes the tail [IndexedUpTo, WALSeq) to overlay
// fresh writes on the indexed result. An empty window (from >= to) yields an
// empty, non-nil map.
func MaterializeLive(ctx context.Context, store *cache.Store, ns string, from, to int64) (map[string]Document, error) {
	live, _, err := MaterializeLiveAndDeleted(ctx, store, ns, from, to)
	if err != nil {
		return nil, err
	}
	return live, nil
}

// MaterializeLiveAndDeleted folds the WAL segments [from, to) in seq order and
// returns both the surviving documents (live, last-writer-wins per id) and the
// set of ids whose final op in the window was a tombstone (deleted). A query
// needs the deleted set to shadow documents that still exist in the indexed
// path: a delete written into the tail must hide an older indexed hit even
// though the tombstone itself is not a live document.
//
// Within the window an id can flip between live and deleted across segments; the
// last op wins, so an upsert after a delete revives the id (live, not deleted)
// and a delete after an upsert removes it (deleted, not live). Both maps are
// non-nil even for an empty window.
func MaterializeLiveAndDeleted(ctx context.Context, store *cache.Store, ns string, from, to int64) (live map[string]Document, deleted map[string]bool, err error) {
	live = make(map[string]Document)
	deleted = make(map[string]bool)

	for seq := from; seq < to; seq++ {
		seg, err := ReadWAL(ctx, store, ns, seq)
		if err != nil {
			return nil, nil, fmt.Errorf("materializing [%d,%d): %w", from, to, err)
		}
		for _, op := range seg.Ops {
			if op.Deleted {
				delete(live, op.ID)
				deleted[op.ID] = true
				continue
			}
			live[op.ID] = op
			// A re-upsert after a delete revives the id within the window.
			delete(deleted, op.ID)
		}
	}
	return live, deleted, nil
}
