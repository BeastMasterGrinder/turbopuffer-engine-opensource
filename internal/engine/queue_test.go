package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

// newQueueStore returns a cache.Store over a fresh in-memory object store. The
// whole queue mechanism — enqueue, claim, heartbeat, complete — is exercised
// against storage.New(), so no Docker is needed and -race covers the CAS paths.
func newQueueStore() *cache.Store {
	return cache.New(storage.New())
}

func TestEnqueueDedupesByNamespace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	added, err := EnqueueReindex(ctx, store, "alpha", 5)
	if err != nil || !added {
		t.Fatalf("first enqueue: added=%v err=%v, want true/nil", added, err)
	}
	// Re-enqueuing the same namespace with a higher position must NOT add a second
	// job; it should just bump RequestedUpTo. Dedupe keeps one outstanding job per ns.
	added, err = EnqueueReindex(ctx, store, "alpha", 9)
	if err != nil {
		t.Fatalf("second enqueue: err=%v", err)
	}
	if added {
		t.Errorf("second enqueue of same ns: added=true, want false (deduped)")
	}

	jobs, err := QueueSnapshot(ctx, store)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("job count after dedup: got %d, want 1", len(jobs))
	}
	if jobs[0].RequestedUpTo != 9 {
		t.Errorf("RequestedUpTo: got %d, want 9 (bumped to latest)", jobs[0].RequestedUpTo)
	}
	if jobs[0].State != JobUnclaimed {
		t.Errorf("state: got %q, want %q", jobs[0].State, JobUnclaimed)
	}
}

// TestTwoIndexersRaceForOneJob is the headline correctness test: many indexers
// concurrently claim against a single job; EXACTLY ONE wins via CAS, every loser
// gets (nil, nil). Run under -race, this proves the queue.json claim is as safe as
// the manifest CAS — one PutCAS lands, the rest get 412 and observe the job
// already in-progress.
func TestTwoIndexersRaceForOneJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	if _, err := EnqueueReindex(ctx, store, "solo", 3); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const racers = 8
	var winners atomic.Int64
	var wg sync.WaitGroup
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func(id int) {
			defer wg.Done()
			job, err := ClaimNextJob(ctx, store, workerName(id), time.Minute)
			if err != nil {
				t.Errorf("worker %d claim: %v", id, err)
				return
			}
			if job != nil {
				winners.Add(1)
			}
		}(i)
	}
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("exactly one worker must win the single job: got %d winners", got)
	}

	jobs, err := QueueSnapshot(ctx, store)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != JobInProgress {
		t.Fatalf("after race: want one in_progress job, got %+v", jobs)
	}
}

// TestBacklogDrains has a pool of indexers compete over a backlog of distinct-ns
// jobs until the queue empties. Each job is claimed exactly once (no namespace is
// completed twice), proving FIFO at-least-once delivery across a fleet. -race
// covers the concurrent CAS contention.
func TestBacklogDrains(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	const jobsN = 20
	for i := 0; i < jobsN; i++ {
		if _, err := EnqueueReindex(ctx, store, namespaceName(i), int64(i+1)); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var mu sync.Mutex
	completed := make(map[string]int)

	const workers = 4
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			name := workerName(id)
			for {
				job, err := ClaimNextJob(ctx, store, name, time.Minute)
				if err != nil {
					t.Errorf("worker %d claim: %v", id, err)
					return
				}
				if job == nil {
					return // backlog drained
				}
				// Simulate BuildIndex, then complete the job.
				removed, err := CompleteJob(ctx, store, job.Namespace, name)
				if err != nil {
					t.Errorf("worker %d complete: %v", id, err)
					return
				}
				if removed {
					mu.Lock()
					completed[job.Namespace]++
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	if len(completed) != jobsN {
		t.Fatalf("namespaces completed: got %d, want %d", len(completed), jobsN)
	}
	for ns, n := range completed {
		if n != 1 {
			t.Errorf("namespace %q completed %d times, want exactly 1", ns, n)
		}
	}

	jobs, err := QueueSnapshot(ctx, store)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("queue not drained: %d jobs remain", len(jobs))
	}
}

// TestCrashMidJobIsReclaimable models an indexer that claims a job then dies
// before completing. With a tiny heartbeat timeout the stale claim becomes
// eligible again, and a second indexer reclaims and completes it — exactly the
// dead-worker takeover rule. The original (crashed) worker's late CompleteJob must
// NOT remove the new owner's job.
func TestCrashMidJobIsReclaimable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	if _, err := EnqueueReindex(ctx, store, "crashy", 7); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Worker A claims, then "crashes" (never heartbeats, never completes).
	jobA, err := ClaimNextJob(ctx, store, "A", time.Millisecond)
	if err != nil || jobA == nil {
		t.Fatalf("worker A claim: job=%v err=%v", jobA, err)
	}

	// Let the heartbeat go stale.
	time.Sleep(5 * time.Millisecond)

	// Worker B reclaims the same job (A's heartbeat is older than the timeout).
	jobB, err := ClaimNextJob(ctx, store, "B", time.Millisecond)
	if err != nil || jobB == nil {
		t.Fatalf("worker B reclaim: job=%v err=%v", jobB, err)
	}
	if jobB.Namespace != "crashy" {
		t.Fatalf("reclaimed wrong namespace: got %q", jobB.Namespace)
	}

	// The zombie worker A finally tries to complete — it must be refused, because
	// ownership has moved to B. Otherwise A would delete B's in-flight job.
	removedByA, err := CompleteJob(ctx, store, "crashy", "A")
	if err != nil {
		t.Fatalf("worker A late complete: %v", err)
	}
	if removedByA {
		t.Fatalf("zombie worker A completed a job it no longer owns")
	}

	// B's heartbeat still works (it owns the job).
	beat, err := HeartbeatJob(ctx, store, "crashy", "B")
	if err != nil || !beat {
		t.Fatalf("worker B heartbeat: ok=%v err=%v, want true/nil", beat, err)
	}

	// B completes successfully and the queue empties.
	removedByB, err := CompleteJob(ctx, store, "crashy", "B")
	if err != nil || !removedByB {
		t.Fatalf("worker B complete: removed=%v err=%v, want true/nil", removedByB, err)
	}
	jobs, err := QueueSnapshot(ctx, store)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("queue not empty after takeover+complete: %d jobs", len(jobs))
	}
}

func TestHeartbeatRefusedForNonOwner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	if _, err := EnqueueReindex(ctx, store, "ns", 1); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := ClaimNextJob(ctx, store, "owner", time.Minute); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A non-owner heartbeat must be refused (false), not error.
	ok, err := HeartbeatJob(ctx, store, "ns", "intruder")
	if err != nil {
		t.Fatalf("intruder heartbeat: %v", err)
	}
	if ok {
		t.Errorf("intruder heartbeat accepted, want refused")
	}
}

func TestClaimEmptyQueueReturnsNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()

	job, err := ClaimNextJob(ctx, store, "idle", time.Minute)
	if err != nil {
		t.Fatalf("claim on empty queue: %v", err)
	}
	if job != nil {
		t.Errorf("claim on empty queue: got %+v, want nil", job)
	}
}

// TestPickEligibleFIFO checks the pure selection: the oldest unclaimed job wins,
// and a fresh in-progress job is skipped while a stale-heartbeat one is reclaimed.
func TestPickEligibleFIFO(t *testing.T) {
	t.Parallel()
	now := time.Unix(1000, 0)

	jobs := []Job{
		{Namespace: "young", State: JobUnclaimed, EnqueuedAt: now.Add(-1 * time.Second).UnixNano()},
		{Namespace: "old", State: JobUnclaimed, EnqueuedAt: now.Add(-5 * time.Second).UnixNano()},
		{Namespace: "fresh-inprogress", State: JobInProgress, HeartbeatAt: now.UnixNano(), EnqueuedAt: now.Add(-9 * time.Second).UnixNano()},
	}
	idx := pickEligible(jobs, 10*time.Second, now)
	if idx < 0 || jobs[idx].Namespace != "old" {
		t.Fatalf("FIFO pick: got idx=%d (%v), want the 'old' unclaimed job", idx, namespaceAt(jobs, idx))
	}

	// Only a stale in-progress job remains: it must be reclaimable.
	stale := []Job{
		{Namespace: "stale", State: JobInProgress, HeartbeatAt: now.Add(-30 * time.Second).UnixNano(), EnqueuedAt: now.UnixNano()},
	}
	if idx := pickEligible(stale, 10*time.Second, now); idx != 0 {
		t.Fatalf("stale reclaim: got idx=%d, want 0", idx)
	}

	// A fresh in-progress job alone is NOT claimable.
	live := []Job{
		{Namespace: "live", State: JobInProgress, HeartbeatAt: now.UnixNano(), EnqueuedAt: now.UnixNano()},
	}
	if idx := pickEligible(live, 10*time.Second, now); idx != -1 {
		t.Fatalf("live job must not be claimable: got idx=%d, want -1", idx)
	}
}

// TestEnqueueReindexIfBehind exercises the namespace notification hook end to end:
// no job below threshold, a job once the unindexed lag crosses it, and dedupe on
// repeat calls. It runs against a real created+upserted namespace over the memory
// store.
func TestEnqueueReindexIfBehind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()
	ns := Open(store, "hooky")
	if err := ns.Create(ctx, NamespaceConfig{Dimension: 2, Metric: "cosine"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// One WAL segment, threshold 3 ⇒ not behind enough ⇒ no enqueue.
	if err := ns.Upsert(ctx, []Document{{ID: "a", Vector: []float32{1, 0}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	added, err := ns.EnqueueReindexIfBehind(ctx, 3)
	if err != nil {
		t.Fatalf("enqueue-if-behind below threshold: %v", err)
	}
	if added {
		t.Errorf("enqueued while below threshold (WALSeq=1, threshold=3)")
	}

	// Grow the WAL past the threshold ⇒ enqueue exactly one job.
	for i := 0; i < 3; i++ {
		if err := ns.Upsert(ctx, []Document{{ID: "x", Vector: []float32{0, 1}}}); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	added, err = ns.EnqueueReindexIfBehind(ctx, 3)
	if err != nil {
		t.Fatalf("enqueue-if-behind above threshold: %v", err)
	}
	if !added {
		t.Errorf("did not enqueue while above threshold")
	}

	// Calling again must dedupe — no second job.
	added, err = ns.EnqueueReindexIfBehind(ctx, 3)
	if err != nil {
		t.Fatalf("enqueue-if-behind repeat: %v", err)
	}
	if added {
		t.Errorf("second call enqueued a duplicate job")
	}

	jobs, err := QueueSnapshot(ctx, store)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(jobs) != 1 || jobs[0].Namespace != "hooky" {
		t.Fatalf("want one job for 'hooky', got %+v", jobs)
	}
}

// TestEndToEndAsyncIndex proves the win at the engine level: an upsert leaves the
// namespace unindexed (IndexEpoch 0), a writer enqueues, a worker claims and runs
// the UNCHANGED BuildIndex, completes the job — and the epoch advances WITHOUT any
// inline tpuf index call. This is the go-test analog of the deploy/ compose demo.
func TestEndToEndAsyncIndex(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newQueueStore()
	ns := Open(store, "async")
	if err := ns.Create(ctx, NamespaceConfig{Dimension: 2, Metric: "cosine"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ns.Upsert(ctx, []Document{{ID: "a", Vector: []float32{1, 0}}, {ID: "b", Vector: []float32{0, 1}}}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	before, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("info before: %v", err)
	}
	if before.IndexEpoch != 0 {
		t.Fatalf("precondition: want unindexed (epoch 0), got %d", before.IndexEpoch)
	}

	// Writer notifies; worker claims, builds, completes — all via the queue.
	if _, err := ns.EnqueueReindexIfBehind(ctx, 0); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	job, err := ClaimNextJob(ctx, store, "indexer-1", time.Minute)
	if err != nil || job == nil {
		t.Fatalf("claim: job=%v err=%v", job, err)
	}
	if err := Open(store, job.Namespace).Index(ctx); err != nil {
		t.Fatalf("BuildIndex via claimed job: %v", err)
	}
	if _, err := CompleteJob(ctx, store, job.Namespace, "indexer-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	after, err := ns.Info(ctx)
	if err != nil {
		t.Fatalf("info after: %v", err)
	}
	if after.IndexEpoch != 1 {
		t.Errorf("epoch did not advance via async path: got %d, want 1", after.IndexEpoch)
	}
	if after.IndexedUpTo != before.WALSeq {
		t.Errorf("IndexedUpTo: got %d, want %d (WALSeq at index start)", after.IndexedUpTo, before.WALSeq)
	}
}

// --- small helpers to keep the table tests readable ---

func workerName(id int) string   { return "worker-" + string(rune('A'+id%26)) + "-" + itoa(id) }
func namespaceName(i int) string { return "ns-" + itoa(i) }
func namespaceAt(j []Job, i int) string {
	if i < 0 || i >= len(j) {
		return "<none>"
	}
	return j[i].Namespace
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
