// Command tpuf-indexer is the async indexing daemon: the "separate indexing
// nodes" half of turbopuffer's compute-compute separation
// (docs/extensions/broker-indexer-queue.md). It moves indexing OFF the query/write
// path — instead of an inline `tpuf index`, a fleet of these processes share one
// object store (MinIO) and one queue.json, each claiming work and running the
// unchanged engine.BuildIndex.
//
// The loop is the blog's worker lifecycle, built on the manifest's exact CAS shape:
//
//	for {
//	    job := ClaimNextJob(worker)     // CAS ○→◐; loser of a race gets nil and polls again
//	    if job == nil { sleep; continue }
//	    heartbeat(job) on a cadence     // CAS-refresh so a slow build isn't falsely reclaimed
//	    engine.Open(store, job.ns).Index(ctx)   // the EXISTING BuildIndex — one manifest CAS publish
//	    CompleteJob(job)                // CAS ◐→removed
//	}
//
// Run two or more of these against the same store and they coordinate purely
// through queue.json CAS: a job is claimed by exactly one worker, a crashed
// worker's job is reclaimed after its heartbeat goes stale, and a duplicate build
// is always safe because BuildIndex is a deterministic rebuild over a WAL prefix
// (at-least-once delivery + idempotent execution).
//
// Config (env): WORKER_ID, TPUF_BACKEND (s3|memory),
// TPUF_S3_ENDPOINT/TPUF_S3_ACCESS_KEY/TPUF_S3_SECRET_KEY/TPUF_BUCKET,
// TPUF_POLL_INTERVAL (idle poll, default 2s), TPUF_HEARTBEAT_TIMEOUT (stale-claim
// takeover threshold, default 30s). The heartbeat cadence is derived as one-third
// of the timeout, so a live build refreshes comfortably before it could be
// reclaimed.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tpuf-indexer:", err)
		os.Exit(1)
	}
}

func run() error {
	backend, err := newBackend()
	if err != nil {
		return err
	}
	// DRAM cache only: BuildIndex reads the manifest/WAL fresh (never cached, rule
	// 2) and the cache only memoizes immutable index objects, so a per-process
	// DRAM tier is correct here exactly as in the query nodes.
	store := cache.New(backend)

	worker := envOr("WORKER_ID", "indexer")
	pollInterval := envDuration("TPUF_POLL_INTERVAL", 2*time.Second)
	heartbeatTimeout := envDuration("TPUF_HEARTBEAT_TIMEOUT", 30*time.Second)

	// Refresh the heartbeat at a third of the timeout: comfortably faster than the
	// takeover threshold, so a slow-but-alive build is never falsely reclaimed.
	heartbeatEvery := heartbeatTimeout / 3
	if heartbeatEvery <= 0 {
		heartbeatEvery = time.Second
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("tpuf-indexer %s: polling every %s, heartbeat timeout %s", worker, pollInterval, heartbeatTimeout)
	loop(ctx, store, worker, pollInterval, heartbeatTimeout, heartbeatEvery)
	log.Printf("tpuf-indexer %s: shutting down", worker)
	return nil
}

// loop is the claim/heartbeat/build/complete cycle. It is split from run so the
// dependencies (store, ids, intervals) are explicit and the body stays readable.
// It returns when ctx is canceled (SIGINT/SIGTERM).
func loop(ctx context.Context, store *cache.Store, worker string, pollInterval, heartbeatTimeout, heartbeatEvery time.Duration) {
	for {
		if ctx.Err() != nil {
			return
		}

		job, err := engine.ClaimNextJob(ctx, store, worker, heartbeatTimeout)
		if err != nil {
			log.Printf("%s: claim error: %v", worker, err)
			if sleepCtx(ctx, pollInterval) {
				return
			}
			continue
		}
		if job == nil {
			// Nothing to do; wait before polling again. Sleeping (rather than
			// busy-looping) keeps the single queue object well under its write-rate
			// ceiling, which is the whole reason the broker/group-commit exists.
			if sleepCtx(ctx, pollInterval) {
				return
			}
			continue
		}

		log.Printf("%s: claimed %q (requestedUpTo=%d)", worker, job.Namespace, job.RequestedUpTo)
		processJob(ctx, store, worker, job.Namespace, heartbeatEvery)
	}
}

// processJob runs BuildIndex for one claimed job while a background heartbeat
// keeps the claim alive, then completes the job. The heartbeat goroutine refreshes
// queue.json on a cadence under the timeout; if it ever fails because ownership
// moved (we were presumed crashed and reclaimed), the new owner publishes a valid
// epoch regardless — BuildIndex is a pure rebuild — so we simply stop heartbeating
// and our late CompleteJob is harmlessly refused.
func processJob(ctx context.Context, store *cache.Store, worker, ns string, heartbeatEvery time.Duration) {
	buildCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go heartbeatLoop(buildCtx, store, worker, ns, heartbeatEvery)

	start := time.Now()
	if err := engine.Open(store, ns).Index(ctx); err != nil {
		log.Printf("%s: BuildIndex for %q failed: %v (job stays claimed; another worker reclaims after heartbeat timeout)", worker, ns, err)
		return
	}
	cancelHeartbeat() // build done; stop refreshing before we complete

	if _, err := engine.CompleteJob(ctx, store, ns, worker); err != nil {
		log.Printf("%s: completing %q failed: %v", worker, ns, err)
		return
	}
	log.Printf("%s: indexed %q in %s, epoch advanced (no inline index call)", worker, ns, time.Since(start).Round(time.Millisecond))
}

// heartbeatLoop refreshes the claim's heartbeat until ctx is canceled. A refused
// heartbeat means ownership moved (we were reclaimed), so we stop — there is no
// point fighting the new owner, whose rebuild is equally valid.
func heartbeatLoop(ctx context.Context, store *cache.Store, worker, ns string, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := engine.HeartbeatJob(ctx, store, ns, worker)
			if err != nil {
				log.Printf("%s: heartbeat for %q error: %v", worker, ns, err)
				continue
			}
			if !ok {
				log.Printf("%s: lost ownership of %q (reclaimed); stopping heartbeat", worker, ns)
				return
			}
		}
	}
}

// sleepCtx waits for d or until ctx is canceled. It returns true if ctx was
// canceled (the caller should stop), false on a normal timeout.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

// newBackend builds the object store from TPUF_BACKEND (default s3), mirroring the
// other commands' wiring so a single env file configures the whole fleet.
func newBackend() (storage.ObjectStore, error) {
	switch backend := envOr("TPUF_BACKEND", "s3"); backend {
	case "memory":
		// A memory backend is per-process, so an indexer over it can only see its
		// own writes — useful for a smoke test, not a real multi-process demo.
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
