package engine

import (
	"context"
	"fmt"
	"sync"
)

// Group commit batches many concurrent upserts to one namespace into a single
// durable object-storage write, instead of one WAL PUT + manifest CAS per
// caller. The point is to decouple write throughput from per-PUT latency: a PUT
// to S3/MinIO costs ~hundreds of milliseconds whether it carries one document
// or a thousand, so amortizing that fixed cost over a batch is almost pure win
// for throughput. This is the classic database "group commit" technique applied
// to an object-storage PUT rather than a disk fsync (docs/extensions/
// group-commit.md).
//
// It is deliberately OPT-IN. The default write path is Namespace.Upsert, a
// linear read-modify-CAS that mirrors the five correctness rules and is the
// right default for the CLI, which never generates concurrent multi-writer
// load. The Committer is what you reach for when many goroutines write the same
// namespace at once.
//
// > turbopuffer reports its WAL runs at "1 WAL entry per second per namespace …
// > concurrent writes to the same namespace are batched into the same entry"
// > (their architecture page). That is THEIR published figure, not a guarantee
// > we reproduce: this clone batches purely by what is already queued when the
// > previous flush finishes (PostgreSQL's commit_delay = 0 "form of group
// > commit"), with no artificial 1 s timer.

// defaultMaxBatchDocs caps how many documents a single coalesced flush may hold,
// so one giant batch can never grow an unbounded WAL segment object. It is a
// safety bound, not a latency knob — the flusher coalesces only what is already
// queued, so this trips only under sustained high concurrency. A new flush
// simply starts for the overflow.
const defaultMaxBatchDocs = 10_000

// pendingWrite is one caller's enqueued upsert waiting on a shared durable
// write. The caller blocks on result until its batch's WAL PUT + manifest CAS
// are acked, so durable-before-return still holds: enqueuing is not success.
type pendingWrite struct {
	docs   []Document
	result chan error // buffered (cap 1) so the flusher never blocks signaling
}

// Committer is a per-namespace group-commit front end over Namespace.Upsert. A
// single background goroutine owns all WAL writes for the namespace, so within
// the process exactly one goroutine ever claims a WAL seq at a time — the
// AppendWAL 412 probe loop almost never fires intra-process, though it stays for
// cross-process races. Construct one with NewCommitter and always Close it to
// drain pending work and stop the goroutine.
//
// A Committer is safe to share across goroutines; that is the whole point. It
// holds the only mutable per-namespace state in the engine, which is exactly why
// it cannot hang off the stateless Namespace façade.
type Committer struct {
	ns           *Namespace
	maxBatchDocs int

	inbox chan pendingWrite
	done  chan struct{} // closed by the flusher once it has fully exited

	// gate makes shutdown race-free. A sender holds gate.RLock while it sends
	// into inbox; Close takes gate.Lock to flip accepting=false. Because the
	// Lock cannot be acquired until every in-flight RLock holder has released —
	// i.e. until every accepted send has been handed to the loop — no
	// successfully-sent write is ever stranded. After accepting goes false the
	// loop drains the inbox (now provably empty of new senders) and exits.
	gate      sync.RWMutex
	accepting bool
	once      sync.Once // makes Close idempotent
}

// CommitterOption tunes a Committer at construction. The defaults are sensible
// for the in-memory backend used in tests and the demo; the only knob worth
// touching is the batch cap.
type CommitterOption func(*Committer)

// WithMaxBatchDocs bounds the number of documents in a single coalesced flush.
// Values <= 0 are ignored (the default cap stands). Use it to keep a single WAL
// segment object within a reasonable size under heavy load.
func WithMaxBatchDocs(n int) CommitterOption {
	return func(c *Committer) {
		if n > 0 {
			c.maxBatchDocs = n
		}
	}
}

// NewCommitter starts the group-commit goroutine for ns and returns a handle
// whose Upsert enqueues into it. The namespace must already exist (its manifest
// is read on every flush to validate and to seed the CAS); NewCommitter itself
// does no I/O. Always call Close when done, even on the error paths of the
// caller, so the goroutine drains and exits rather than leaking.
func NewCommitter(ns *Namespace, opts ...CommitterOption) *Committer {
	c := &Committer{
		ns:           ns,
		maxBatchDocs: defaultMaxBatchDocs,
		inbox:        make(chan pendingWrite),
		done:         make(chan struct{}),
		accepting:    true,
	}
	for _, opt := range opts {
		opt(c)
	}
	go c.loop()
	return c
}

// Upsert enqueues docs for the next coalesced flush and blocks until that flush
// is durable, returning the shared outcome of its batch. It is a drop-in for
// Namespace.Upsert and keeps the identical contract: durable-before-return
// (the caller only returns after its batch's WAL PUT + manifest CAS are acked),
// last-writer-wins on duplicate IDs (the flusher concatenates batch members in
// arrival order, which MaterializeLive resolves), and an empty batch is a no-op.
//
// Validation happens HERE, before the docs enter the inbox, so a present-but-
// wrong-length vector is rejected to this caller only — it never poisons the
// merged batch every other writer shares (docs/extensions/group-commit.md). The
// manifest read for validation is fresh (rule 2); it does not become the CAS
// token, which the flusher re-reads.
//
// If ctx is cancelled while waiting, Upsert returns ctx.Err(). The flush may
// still complete and land this caller's docs durably — that is acceptable and
// idempotent (re-upsert by ID is last-writer-wins). If the Committer is closed
// while this call is in flight, it returns ErrCommitterClosed.
func (c *Committer) Upsert(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	m, _, err := LoadManifest(ctx, c.ns.store, c.ns.name)
	if err != nil {
		return fmt.Errorf("upserting into %q: %w", c.ns.name, err)
	}
	if err := c.ns.validateDocs(m.Dimension, docs); err != nil {
		return fmt.Errorf("upserting into %q: %w", c.ns.name, err)
	}

	w := pendingWrite{docs: docs, result: make(chan error, 1)}

	// Enqueue under the gate's read lock. Holding RLock across the send is what
	// makes shutdown race-free: Close cannot flip accepting / close the inbox
	// until this send has been received, so a successfully-sent write is never
	// stranded. We still race ctx.Done so a cancelled caller never blocks
	// forever; if it bails before sending, its write simply never enters a batch.
	c.gate.RLock()
	if !c.accepting {
		c.gate.RUnlock()
		return fmt.Errorf("upserting into %q: %w", c.ns.name, ErrCommitterClosed)
	}
	select {
	case c.inbox <- w:
		c.gate.RUnlock()
	case <-ctx.Done():
		c.gate.RUnlock()
		return ctx.Err()
	}

	// Block until this caller's batch is durable (durable-before-return).
	select {
	case err := <-w.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ErrCommitterClosed is returned by Upsert when the Committer has been Closed
// (or is closing) and can no longer accept new writes.
var errCommitterClosed = fmt.Errorf("group-commit committer is closed")

// ErrCommitterClosed reports an Upsert rejected because the Committer is shut
// down. Exported as a value so callers can branch on errors.Is.
var ErrCommitterClosed = errCommitterClosed

// loop is the committer goroutine: it blocks for the first pending write, drains
// whatever else is already queued (without any artificial delay — PostgreSQL's
// commit_delay = 0 "form of group commit"), commits the whole batch as ONE WAL
// segment and ONE manifest CAS, then signals every waiter with the shared
// outcome. Close closes the inbox (only after every accepted send has landed, by
// the gate), so the range here drains every queued write and then exits — no
// caller is ever left blocked.
func (c *Committer) loop() {
	defer close(c.done)
	for first := range c.inbox {
		c.flush(first)
	}
}

// flush coalesces first plus everything already queued into one merged batch and
// commits it once. Every member receives the same error/success, so failure fate
// is shared all-or-nothing: the design never reports success to some members of
// a batch and failure to others. The batch is bounded by maxBatchDocs so a
// single WAL segment object stays a reasonable size; the overflow stays queued
// for the next flush.
func (c *Committer) flush(first pendingWrite) {
	batch := []pendingWrite{first}
	ops := append([]Document(nil), first.docs...)

	// Drain whatever is already queued, in arrival order, until the inbox is
	// momentarily empty or the batch cap is reached. No timer: we take only what
	// is already waiting, so the fastest writer is never slowed by an artificial
	// window.
	for len(ops) < c.maxBatchDocs {
		select {
		case w, ok := <-c.inbox:
			if !ok {
				// Inbox closed mid-drain (Close ran). Flush what we already have;
				// the range in loop has already received every other queued write.
				goto commit
			}
			batch = append(batch, w)
			ops = append(ops, w.docs...)
		default:
			// Nothing more queued right now — flush what we have.
			goto commit
		}
	}

commit:
	// One WAL PUT + one manifest CAS for the whole batch. We seed the seq probe
	// from the manifest's current WALSeq; commitBatch tolerates a stale hint by
	// climbing on a 412 (rule 1). The merged ops slice becomes one WALSegment.Ops
	// — a 1-doc batch and a 5000-doc batch use the identical wire shape.
	m, _, err := LoadManifest(context.Background(), c.ns.store, c.ns.name)
	seqHint := int64(0)
	if err == nil {
		seqHint = m.WALSeq
	}
	if err == nil {
		err = c.ns.commitBatch(context.Background(), seqHint, ops)
	}

	for _, w := range batch {
		w.result <- err
	}
}

// Close stops the committer goroutine after draining any already-enqueued
// writes, so a clean shutdown never loses a write that a caller is still
// blocked on. It is idempotent and safe to call from any goroutine. After Close
// returns, the goroutine has fully exited and no further flushes will run; new
// Upsert calls fail with ErrCommitterClosed.
//
// The full gate.Lock can only be taken once every in-flight sender has released
// its RLock — i.e. once every accepted send has been received by the loop — so
// closing inbox here can never strand a successfully-sent write. The loop's
// range then drains the (now closed) inbox and exits, closing done.
func (c *Committer) Close() {
	c.once.Do(func() {
		c.gate.Lock()
		c.accepting = false
		close(c.inbox)
		c.gate.Unlock()
	})
	<-c.done
}
