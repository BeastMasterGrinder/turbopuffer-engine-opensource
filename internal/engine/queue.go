package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// The indexing job queue — a distributed queue in a single JSON object on
// object storage, coordinated by exactly the same compare-and-swap mechanic the
// manifest uses (docs/extensions/broker-indexer-queue.md). It moves indexing OFF
// the inline `tpuf index` path: a writer notes "this namespace needs reindexing"
// by enqueueing a job; a fleet of indexer daemons each CLAIMS a job by CAS-flipping
// its state, runs the unchanged BuildIndex, then completes it. The loser of a
// concurrent claim simply re-reads queue.json and retries — no lock, no lease
// server, no Raft/Kafka, just object storage and CAS.
//
// > Design note — what is and isn't turbopuffer's. turbopuffer's blog "How to
// > build a distributed queue in a single JSON file on object storage" confirms
// > the FILENAME (queue.json), the read-modify-conditional-write CAS loop, the
// > ○ (unclaimed) → ◐ (claimed) job-state symbols, heartbeats with a dead-worker
// > takeover rule, FIFO + at-least-once delivery, and the "not part of the write
// > path… purely a notification system" framing. The exact FIELD LAYOUT inside
// > queue.json is NOT publicly documented; the Job struct below is OUR design,
// > inferred from the blog. So is the heartbeat timeout value (see
// > defaultHeartbeatTimeout). Treat those two as the clone's choices, not quotes.
//
// Durability still lives on the WAL, never here. Enqueueing is a best-effort
// notification after an already-durable Upsert; if queue.json lagged or dropped a
// job, no data is lost — queries fall back to the exhaustive WAL-tail scan
// (correctness rule 5) until indexing catches up. That fallback is exactly what
// lets the queue be best-effort.

// maxQueueCASAttempts bounds the read-modify-write retry loop on queue.json,
// mirroring the manifest's maxCASAttempts. Each conflict means another writer (an
// enqueueing writer, another indexer claiming, or the broker's group commit) won
// the race, so we reload queue.json fresh and try again. The bound keeps a
// pathological contention storm from spinning forever; under the single-process
// educational load this clone sees it is never approached.
const maxQueueCASAttempts = 32

// defaultHeartbeatTimeout is how long a claimed job may go without a heartbeat
// before another indexer is allowed to reclaim it, on the assumption the original
// worker crashed. turbopuffer does NOT publish its timeout (the blog only says
// "more than some timeout"), so this value is OUR deliberate choice: long enough
// that a slow-but-alive indexer refreshing its heartbeat on a shorter cadence is
// never falsely reclaimed (two indexers building the same namespace is wasteful,
// though still correct because BuildIndex is a pure rebuild), short enough that a
// real crash does not stall indexing for long. Pick the heartbeat cadence well
// under this (see cmd/tpuf-indexer).
const defaultHeartbeatTimeout = 30 * time.Second

// JobState is the lifecycle position of a queued indexing job. The blog draws it
// as ○ (unclaimed) → ◐ (claimed, in-progress) → removed-when-done; we keep the
// two live states explicit and delete completed jobs rather than retaining a done
// marker, so the queue stays small.
type JobState string

const (
	// JobUnclaimed (○) is a job waiting for any indexer to pick it up.
	JobUnclaimed JobState = "unclaimed"
	// JobInProgress (◐) is a job an indexer has claimed and is building; it
	// carries the claimer's id and a heartbeat timestamp for crash detection.
	JobInProgress JobState = "in_progress"
)

// Job is one indexing request for a namespace. It is OUR field layout (the real
// queue.json schema is undocumented — see the file header). RequestedUpTo records
// the WALSeq the enqueueing party observed running ahead of IndexedUpTo; it is
// purely informational, because BuildIndex re-snapshots the live WALSeq at its own
// start (correctness rule 3), so a job never pins a stale position.
type Job struct {
	Namespace     string   `json:"namespace"`
	RequestedUpTo int64    `json:"requestedUpTo"`         // WALSeq seen behind at enqueue time (informational)
	State         JobState `json:"state"`                 // ○ unclaimed | ◐ in_progress
	Worker        string   `json:"worker,omitempty"`      // id of the indexer that claimed it (in_progress only)
	HeartbeatAt   int64    `json:"heartbeatAt,omitempty"` // unix-nanos of the claimer's last heartbeat
	EnqueuedAt    int64    `json:"enqueuedAt"`            // unix-nanos of first enqueue, the FIFO ordering key
}

// Queue is the whole queue.json document: an ordered list of jobs. FIFO is
// preserved by always appending new jobs and always claiming the oldest eligible
// one (by EnqueuedAt), matching the blog's FIFO-execution guarantee. Version is
// informational like the manifest's — the ETag is the real CAS token.
type Queue struct {
	Version int64 `json:"version"`
	Jobs    []Job `json:"jobs"`
}

// queueKey returns the object key of the shared indexing queue. One queue object
// serves all namespaces (the jobs carry their own ns), matching turbopuffer's
// single queue.json; per-namespace queues would sidestep single-object contention
// but the broker + group commit is the documented scaling answer instead
// (docs/extensions/broker-indexer-queue.md).
func queueKey() string {
	return "_queue/queue.json"
}

// LoadQueue reads queue.json fresh from the backend, returning it with the ETag
// that serves as the CAS token. Like the manifest (correctness rule 2) the read
// is intentionally UNCACHED — every CAS iteration must observe the current ETag —
// so it goes through Store.Get, never GetCached. A not-yet-created queue surfaces
// as storage.ErrNotFound so callers can branch on errors.Is and create it.
func LoadQueue(ctx context.Context, store *cache.Store) (Queue, string, error) {
	body, etag, err := store.Get(ctx, queueKey())
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return Queue{}, "", err
		}
		return Queue{}, "", fmt.Errorf("loading queue: %w", err)
	}

	var q Queue
	if err := json.Unmarshal(body, &q); err != nil {
		return Queue{}, "", fmt.Errorf("decoding queue: %w", err)
	}
	return q, etag, nil
}

// EnsureQueue creates an empty queue.json if none exists yet, using a write-once
// PutIfAbsent so two concurrent creators can never both succeed (the 412 loser
// treats "already exists" as success — the queue is there either way). It is
// idempotent and cheap to call before any enqueue/claim. The returned error is
// only a genuine backend failure.
func EnsureQueue(ctx context.Context, store *cache.Store) error {
	if _, _, err := LoadQueue(ctx, store); err == nil {
		return nil
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	body, err := json.Marshal(Queue{Version: 1})
	if err != nil {
		return fmt.Errorf("encoding empty queue: %w", err)
	}
	if _, err := store.PutIfAbsent(ctx, queueKey(), body); err != nil {
		if errors.Is(err, storage.ErrPreconditionFailed) {
			return nil // another creator beat us; the queue exists, which is all we wanted
		}
		return fmt.Errorf("creating queue: %w", err)
	}
	return nil
}

// SaveQueueCAS applies mutate to queue.json under a compare-and-swap loop, the
// EXACT same If-Match/412 shape as SaveManifestCAS: each attempt reads the queue
// fresh (rule 2 — never cached), applies mutate, bumps Version, and conditionally
// writes with If-Match set to the ETag just observed. A 412 means another writer
// won the race, so we reload and retry against the new state; any other error
// aborts. mutate returns false to abort the whole operation WITHOUT writing (e.g.
// a claim that found no eligible job) — that is reported as a clean no-write, not
// an error. On a committed write the new queue is returned.
//
// This is the single coordination primitive every queue operation is built on:
// enqueue, claim, heartbeat, and complete are all just different mutate closures.
// Two indexers racing to claim the SAME job therefore resolve exactly like two
// writers racing the manifest — one PutCAS lands, the other gets 412 and re-reads,
// finds the job already in_progress, and moves on.
func SaveQueueCAS(ctx context.Context, store *cache.Store, mutate func(*Queue) bool) (Queue, bool, error) {
	for attempt := 0; attempt < maxQueueCASAttempts; attempt++ {
		q, etag, err := LoadQueue(ctx, store)
		if err != nil {
			return Queue{}, false, err
		}

		if !mutate(&q) {
			return q, false, nil // nothing to do; deliberately no write
		}
		q.Version++

		body, err := json.Marshal(q)
		if err != nil {
			return Queue{}, false, fmt.Errorf("encoding queue: %w", err)
		}

		if _, err := store.PutCAS(ctx, queueKey(), body, etag); err == nil {
			return q, true, nil
		} else if errors.Is(err, storage.ErrPreconditionFailed) {
			continue
		} else {
			return Queue{}, false, fmt.Errorf("saving queue: %w", err)
		}
	}
	return Queue{}, false, fmt.Errorf("saving queue: exhausted %d CAS attempts", maxQueueCASAttempts)
}

// EnqueueReindex records that ns needs (re)indexing, returning whether a NEW job
// was added. It is idempotent and deduplicated on namespace: if a live job for ns
// already exists (unclaimed OR in-progress) we do not add a second one, we just
// refresh its RequestedUpTo to the latest position — so a flood of writes to one
// namespace produces at most one outstanding job at a time. This is the
// "notification, not write path" contract: calling it repeatedly is safe and
// cheap. requestedUpTo is the WALSeq the caller saw running ahead of IndexedUpTo.
func EnqueueReindex(ctx context.Context, store *cache.Store, ns string, requestedUpTo int64) (bool, error) {
	if err := EnsureQueue(ctx, store); err != nil {
		return false, err
	}

	added := false
	_, _, err := SaveQueueCAS(ctx, store, func(q *Queue) bool {
		for i := range q.Jobs {
			if q.Jobs[i].Namespace == ns {
				// Dedupe: a live job already covers this namespace. Bump its target
				// position (a later notification observed more unindexed WAL) but add
				// no second job. No state change ⇒ no write needed.
				if requestedUpTo > q.Jobs[i].RequestedUpTo {
					q.Jobs[i].RequestedUpTo = requestedUpTo
					added = false
					return true
				}
				return false
			}
		}
		q.Jobs = append(q.Jobs, Job{
			Namespace:     ns,
			RequestedUpTo: requestedUpTo,
			State:         JobUnclaimed,
			EnqueuedAt:    time.Now().UnixNano(),
		})
		added = true
		return true
	})
	if err != nil {
		return false, err
	}
	return added, nil
}

// ClaimNextJob atomically claims the oldest eligible job for worker and returns
// it, or (nil, nil) if there is nothing to do. "Eligible" means unclaimed, OR
// in-progress but whose last heartbeat is older than timeout (the original worker
// is presumed crashed — the dead-worker takeover rule). The claim flips the job to
// in_progress, stamps worker and a fresh heartbeat, all in ONE CAS write, so two
// indexers calling this concurrently resolve by CAS: one PutCAS lands, the loser
// gets 412, re-reads, finds the job already claimed-and-heartbeating, and either
// claims a different job or backs off. A zero timeout falls back to
// defaultHeartbeatTimeout.
func ClaimNextJob(ctx context.Context, store *cache.Store, worker string, timeout time.Duration) (*Job, error) {
	if timeout <= 0 {
		timeout = defaultHeartbeatTimeout
	}
	if err := EnsureQueue(ctx, store); err != nil {
		return nil, err
	}

	var claimed *Job
	_, wrote, err := SaveQueueCAS(ctx, store, func(q *Queue) bool {
		claimed = nil
		idx := pickEligible(q.Jobs, timeout, time.Now())
		if idx < 0 {
			return false // nothing eligible; do not write
		}
		q.Jobs[idx].State = JobInProgress
		q.Jobs[idx].Worker = worker
		q.Jobs[idx].HeartbeatAt = time.Now().UnixNano()
		j := q.Jobs[idx]
		claimed = &j
		return true
	})
	if err != nil {
		return nil, err
	}
	if !wrote {
		return nil, nil
	}
	return claimed, nil
}

// pickEligible returns the index of the oldest (FIFO, by EnqueuedAt) claimable
// job in jobs, or -1 if none. A job is claimable when it is unclaimed, or it is
// in-progress but its heartbeat is older than timeout (presumed-crashed worker —
// reclaim where it left off). It is a pure function of (jobs, timeout, now) so the
// claim CAS closure stays trivial and the selection is unit-testable in isolation.
func pickEligible(jobs []Job, timeout time.Duration, now time.Time) int {
	best := -1
	for i := range jobs {
		eligible := false
		switch jobs[i].State {
		case JobUnclaimed:
			eligible = true
		case JobInProgress:
			if now.Sub(time.Unix(0, jobs[i].HeartbeatAt)) > timeout {
				eligible = true // stale heartbeat ⇒ original worker presumed gone
			}
		}
		if !eligible {
			continue
		}
		if best < 0 || jobs[i].EnqueuedAt < jobs[best].EnqueuedAt {
			best = i
		}
	}
	return best
}

// HeartbeatJob refreshes the heartbeat timestamp of ns's in-progress job IF
// worker still owns it, returning whether the heartbeat was accepted. A worker
// calls this on a cadence well under the timeout while BuildIndex runs, so a
// slow-but-alive build is never falsely reclaimed. If the job was already
// reclaimed by another worker (the heartbeat went stale and someone else took
// over) ownership has moved, the refresh is refused (false), and the original
// worker should abandon — the new owner will publish a valid epoch regardless,
// because BuildIndex is a pure rebuild.
func HeartbeatJob(ctx context.Context, store *cache.Store, ns, worker string) (bool, error) {
	ok := false
	_, _, err := SaveQueueCAS(ctx, store, func(q *Queue) bool {
		for i := range q.Jobs {
			if q.Jobs[i].Namespace == ns && q.Jobs[i].State == JobInProgress && q.Jobs[i].Worker == worker {
				q.Jobs[i].HeartbeatAt = time.Now().UnixNano()
				ok = true
				return true
			}
		}
		return false // not ours (or gone); nothing to write
	})
	if err != nil {
		return false, err
	}
	return ok, nil
}

// CompleteJob removes ns's job from the queue once worker has published its epoch,
// returning whether a job was removed. It only removes a job worker still owns, so
// a worker that was reclaimed mid-build (its job is now owned by someone else)
// does NOT delete the new owner's job — at-least-once is preserved: the survivor's
// own CompleteJob removes it. Removing rather than marking-done keeps the queue
// small; the at-least-once guarantee means a duplicate build is always safe
// because BuildIndex is deterministic over a WAL prefix.
func CompleteJob(ctx context.Context, store *cache.Store, ns, worker string) (bool, error) {
	removed := false
	_, _, err := SaveQueueCAS(ctx, store, func(q *Queue) bool {
		for i := range q.Jobs {
			if q.Jobs[i].Namespace == ns && q.Jobs[i].Worker == worker {
				q.Jobs = append(q.Jobs[:i], q.Jobs[i+1:]...)
				removed = true
				return true
			}
		}
		return false // our job is gone (reclaimed and completed by another); nothing to write
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// QueueSnapshot returns the current jobs sorted FIFO (oldest first) for display
// and tests, read fresh (rule 2). It is a read-only convenience over LoadQueue; an
// absent queue reports as empty, not an error, so callers need not pre-create it.
func QueueSnapshot(ctx context.Context, store *cache.Store) ([]Job, error) {
	q, _, err := LoadQueue(ctx, store)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	jobs := append([]Job(nil), q.Jobs...)
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].EnqueuedAt < jobs[j].EnqueuedAt })
	return jobs, nil
}
