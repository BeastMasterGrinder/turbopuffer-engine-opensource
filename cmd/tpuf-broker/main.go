// Command tpuf-broker is the stateless broker that fronts all writes to
// queue.json (docs/extensions/broker-indexer-queue.md). It is the answer to a real
// scaling limit turbopuffer cites: a CAS write to one object is ~200ms and forces
// each write to be non-overlapping in time (~5 writes/s; GCS caps a single object
// at ~1 req/s), so "tens or hundreds of clients" contending over the queue object
// directly would serialize badly. The fix is one process that becomes the SOLE
// writer and runs a single GROUP-COMMIT loop on behalf of every client: in-flight
// enqueue requests buffer in memory and flush together as the next CAS write, and
// a request is not acknowledged until that group commit has landed in object
// storage. (turbopuffer reports "10x lower tail latency" from this shape.)
//
// The broker is a NOTIFICATION front-end, never the write path: durability lives
// on the WAL. A client (a query/write node) tells the broker "this namespace needs
// reindexing"; the broker coalesces and CAS-writes those into queue.json; indexer
// daemons claim from there. If the broker were down, clients could still enqueue
// directly via engine.EnqueueReindex (the broker only earns its keep under
// contention) and, failing even that, queries stay correct via the WAL-tail scan.
//
// Endpoints:
//
//	POST /v1/enqueue   body: {"namespace":"...","requestedUpTo":N}  → group-committed, acked after the CAS lands
//	GET  /v1/queue     the current queue.json jobs (FIFO order), for the demo
//	GET  /healthz      liveness
//
// Config (env): BROKER_ID, PORT (default 8090), TPUF_BACKEND (s3|memory),
// TPUF_S3_ENDPOINT/TPUF_S3_ACCESS_KEY/TPUF_S3_SECRET_KEY/TPUF_BUCKET,
// TPUF_GROUP_COMMIT_INTERVAL (max time a request waits to be batched, default
// 50ms).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpuf-broker:", err)
		os.Exit(1)
	}
}

func run() error {
	backend, err := newBackend()
	if err != nil {
		return err
	}
	store := cache.New(backend)

	// The queue object is created once up front so the first enqueue does not race
	// to create it; EnsureQueue is idempotent and write-once, so this is safe even
	// if several brokers start at once (only one wins the PutIfAbsent, the rest see
	// "already exists" as success).
	if err := engine.EnsureQueue(context.Background(), store); err != nil {
		return fmt.Errorf("ensuring queue exists: %w", err)
	}

	b := newBroker(store, envOr("BROKER_ID", "broker"), envDuration("TPUF_GROUP_COMMIT_INTERVAL", 50*time.Millisecond))
	addr := ":" + envOr("PORT", "8090")
	log.Printf("tpuf-broker %s listening on %s (group-commit interval %s)", b.id, addr, b.interval)
	return http.ListenAndServe(addr, b.routes())
}

// enqueueRequest is one client's "this namespace needs reindexing" notification,
// waiting to be folded into the next group commit. done is closed (with any error)
// once the CAS write that includes this request has landed, so the HTTP handler
// blocks until the notification is durable in queue.json — the "we don't
// acknowledge a write until the group commit has landed" contract.
type enqueueRequest struct {
	ns            string
	requestedUpTo int64
	done          chan error
}

// broker is the stateless single-writer to queue.json. Every enqueue funnels
// through the in channel into one group-commit goroutine, so no two writes to the
// queue object ever overlap in time — exactly the contention the broker exists to
// remove.
type broker struct {
	id       string
	store    *cache.Store
	interval time.Duration
	in       chan enqueueRequest

	startOnce sync.Once
}

func newBroker(store *cache.Store, id string, interval time.Duration) *broker {
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	b := &broker{
		id:       id,
		store:    store,
		interval: interval,
		in:       make(chan enqueueRequest, 1024),
	}
	b.startOnce.Do(func() { go b.commitLoop() })
	return b
}

// commitLoop is the single group-commit loop. It drains all enqueue requests that
// have accumulated, applies them to queue.json in ONE CAS write (engine's
// SaveQueueCAS — the same If-Match/412 retry the manifest uses), then signals
// every batched caller. Because this is the only writer, the CAS practically never
// conflicts with itself; it still retries on 412 to compose correctly with an
// indexer claiming a job concurrently.
//
// Batching is by wall-clock window: the first request opens a window of b.interval
// during which more requests pile in, then the whole batch commits together. This
// is the same group-commit shape the engine's WAL committer uses (commit.go) and
// the WAL itself in turbopuffer.
func (b *broker) commitLoop() {
	for first := range b.in {
		batch := []enqueueRequest{first}

		// Hold the window open, collecting everything that arrives, then flush.
		timer := time.NewTimer(b.interval)
	collect:
		for {
			select {
			case req := <-b.in:
				batch = append(batch, req)
			case <-timer.C:
				break collect
			}
		}

		b.flush(batch)
	}
}

// flush folds a whole batch of notifications into queue.json with a single CAS
// write, deduplicating per namespace inside the closure (EnqueueReindex already
// dedupes, but doing the whole batch in one SaveQueueCAS turns N notifications
// into ONE object write — that is the throughput win). Every caller in the batch
// is then unblocked with the same result.
func (b *broker) flush(batch []enqueueRequest) {
	ctx := context.Background()

	// Apply the entire batch in one CAS mutation. Within the closure we merge by
	// namespace exactly like EnqueueReindex: a live job for a namespace is reused
	// (its RequestedUpTo bumped), so the batch adds at most one job per distinct ns.
	_, _, err := engine.SaveQueueCAS(ctx, b.store, func(q *engine.Queue) bool {
		changed := false
		for _, req := range batch {
			if mergeNotification(q, req.ns, req.requestedUpTo) {
				changed = true
			}
		}
		return changed
	})

	for _, req := range batch {
		req.done <- err
		close(req.done)
	}
	if err != nil {
		log.Printf("%s: group commit of %d notification(s) failed: %v", b.id, len(batch), err)
		return
	}
	log.Printf("%s: group-committed %d notification(s) to queue.json", b.id, len(batch))
}

// mergeNotification adds or refreshes a reindex job for ns inside an in-memory
// queue, returning whether anything changed. It mirrors EnqueueReindex's dedupe
// rule (one live job per namespace) but operates on the already-loaded Queue so a
// whole batch shares one CAS write. A namespace with no existing job gets a fresh
// unclaimed job; an existing one only bumps RequestedUpTo if this notification
// observed a later position.
func mergeNotification(q *engine.Queue, ns string, requestedUpTo int64) bool {
	for i := range q.Jobs {
		if q.Jobs[i].Namespace == ns {
			if requestedUpTo > q.Jobs[i].RequestedUpTo {
				q.Jobs[i].RequestedUpTo = requestedUpTo
				return true
			}
			return false
		}
	}
	q.Jobs = append(q.Jobs, engine.Job{
		Namespace:     ns,
		RequestedUpTo: requestedUpTo,
		State:         engine.JobUnclaimed,
		EnqueuedAt:    time.Now().UnixNano(),
	})
	return true
}

func (b *broker) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /v1/enqueue", b.handleEnqueue)
	mux.HandleFunc("GET /v1/queue", b.handleQueue)
	return mux
}

type enqueueBody struct {
	Namespace     string `json:"namespace"`
	RequestedUpTo int64  `json:"requestedUpTo"`
}

// handleEnqueue submits one notification to the group-commit loop and blocks until
// the batch it lands in has been CAS-written to queue.json — so a 200 means the
// notification is durably queued, never merely accepted into memory.
func (b *broker) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Tpuf-Broker", b.id)
	var body enqueueBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, fmt.Sprintf("decoding request: %v", err), http.StatusBadRequest)
		return
	}
	if body.Namespace == "" {
		http.Error(w, "request must set \"namespace\"", http.StatusBadRequest)
		return
	}

	req := enqueueRequest{ns: body.Namespace, requestedUpTo: body.RequestedUpTo, done: make(chan error, 1)}
	select {
	case b.in <- req:
	case <-r.Context().Done():
		http.Error(w, "request canceled before submission", http.StatusRequestTimeout)
		return
	}

	if err := <-req.done; err != nil {
		http.Error(w, fmt.Sprintf("group commit failed: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"broker": b.id, "namespace": body.Namespace, "queued": true})
}

// handleQueue returns the current queue.json jobs in FIFO order, so the deploy
// demo can watch jobs appear, get claimed, and drain.
func (b *broker) handleQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Tpuf-Broker", b.id)
	jobs, err := engine.QueueSnapshot(r.Context(), b.store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if jobs == nil {
		jobs = []engine.Job{}
	}
	writeJSON(w, map[string]any{"broker": b.id, "count": len(jobs), "jobs": jobs})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("encoding response: %v", err)
	}
}

// newBackend builds the object store from TPUF_BACKEND (default s3). A memory
// backend is per-process, so a broker over it cannot share a queue with separate
// indexer processes — use s3/MinIO for the multi-process demo.
func newBackend() (storage.ObjectStore, error) {
	switch backend := envOr("TPUF_BACKEND", "s3"); backend {
	case "memory":
		return storage.New(), nil
	case "s3":
		store, err := storage.NewS3StoreFromEnv()
		if err != nil {
			return nil, fmt.Errorf("connecting to s3 backend: %w", err)
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unknown TPUF_BACKEND %q (want \"s3\" or \"memory\")", backend)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
