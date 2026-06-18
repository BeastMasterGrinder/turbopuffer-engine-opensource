// Command tpuf-bench measures tpuf operation latency as percentiles (p50..p99.9).
//
// It builds a fresh namespace on the chosen backend, upserts synthetic documents
// in batches, then times vector and BM25 queries BOTH before indexing (each query
// exhaustively scans the unindexed WAL tail) and after indexing (served from the
// epoch index with the DRAM cache warm). The contrast between those two is the
// point: it shows what the index — and the cache over object storage — buy you at
// the tail.
//
// Backends (TPUF_BACKEND or --backend):
//
//	memory  in-process, no infra — measures pure engine/algorithm cost (default)
//	s3      MinIO/S3 via TPUF_S3_ENDPOINT/TPUF_S3_ACCESS_KEY/TPUF_S3_SECRET_KEY/
//	        TPUF_BUCKET — measures real object-storage round-trip latency
//
// With --namespaces > 1 it switches to a multi-tenant concurrent benchmark:
// it creates that many namespaces (each upserted + indexed), then drives
// --concurrency worker goroutines issuing queries across all tenants, reporting
// aggregate percentiles, achieved wall-clock throughput, and the shared DRAM
// cache's hit/miss/eviction behavior. Pair it with --cache-objects (a bounded
// cache) below the resident working set to watch cold-start misses recur under
// tenant churn — the regime turbopuffer actually runs in.
//
// Examples:
//
//	set -a; source .env.example; set +a
//	go run ./cmd/tpuf-bench --backend s3 --docs 2000 --batch 100 --queries 500
//	go run ./cmd/tpuf-bench --backend s3 --namespaces 20 --concurrency 16 \
//	    --dim 256 --docs 2000 --queries 20000 --cache-objects 200
//
// Add --nvme-dir to insert the optional NVMe ring-buffer tier under the DRAM
// cache (docs/extensions/nvme-ring-buffer-cache.md). With DRAM capped small and
// the ring large, DRAM evictions that would otherwise be S3-cold reads are served
// from the local-disk ring instead, which the cache panels then report as a real
// three-way DRAM/NVMe/S3 split:
//
//	go run ./cmd/tpuf-bench --namespaces 20 --concurrency 16 --queries 20000 \
//	    --cache-objects 8 --nvme-dir /tmp/tpuf-nvme --nvme-slots 4096
//
// Add --filter-plan to compare the bitmap attribute index's filter-first vs
// search-first plans: it times the same vector query filtered at several
// selectivity bands and reports the cold-cache index-object fetch count each plan
// pays (docs/extensions/bitmap-attribute-indexes.md):
//
//	go run ./cmd/tpuf-bench --filter-plan --docs 3000 --dim 32 --queries 100
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/farjad/turbopuffer-clone/internal/bench"
	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "tpuf-bench:", err)
		os.Exit(1)
	}
}

// config holds the parsed benchmark parameters.
type config struct {
	backend     string
	namespace   string
	dim         int
	metric      string
	textField   string
	docs        int
	batch       int
	queries     int
	warmup      int
	topK        int
	nProbe      int
	seed        int64
	namespaces  int    // >1 switches to multi-tenant concurrent mode
	concurrency int    // worker goroutines in multi-tenant mode
	cacheObj    int    // DRAM cache capacity in objects; 0 = unbounded
	nvmeDir     string // if set, enable the NVMe ring-buffer tier rooted here
	nvmeSlots   int    // NVMe ring capacity in objects (used when nvmeDir is set)
	coldTrials  int    // >0 runs the cold-vs-hot same-query experiment
	hybrid      bool   // run the hybrid-fusion recall experiment instead of the latency phases
	groupCommit bool   // run the group-commit segment-count comparison instead of the latency phases
	filterPlan  bool   // run the bitmap filter-plan latency comparison (selective vs broad)
	rabitq      bool   // run the True RaBitQ vs lite shortlist-recall experiment
}

// nvmeEnabled reports whether the NVMe ring-buffer cache tier is configured.
func (c config) nvmeEnabled() bool { return c.nvmeDir != "" }

// run parses args and dispatches to the single-tenant or multi-tenant
// benchmark. It is the testable entry point: with --backend memory either mode
// runs end to end with no infrastructure.
func run(args []string, out io.Writer) error {
	cfg, err := parseFlags(args, out)
	if err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.groupCommit {
		return runGroupCommit(context.Background(), cfg, out)
	}
	if cfg.filterPlan {
		return runFilterPlan(context.Background(), cfg, out)
	}
	if cfg.hybrid {
		return runHybrid(context.Background(), cfg, out)
	}
	if cfg.rabitq {
		return runRaBitQ(context.Background(), cfg, out)
	}
	if cfg.coldTrials > 0 {
		return runColdStart(context.Background(), cfg, out)
	}
	if cfg.namespaces > 1 {
		return runMultiTenant(context.Background(), cfg, out)
	}
	return runSingle(context.Background(), cfg, out)
}

// runSingle benchmarks one namespace sequentially, contrasting the unindexed
// WAL-tail scan against the indexed path and reporting the cache cold/hot split.
func runSingle(ctx context.Context, cfg config, out io.Writer) error {
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}
	store := cache.New(backend)
	ns := engine.Open(store, cfg.namespace)
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: cfg.textField}); err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}

	rng := rand.New(rand.NewSource(cfg.seed))
	docs := makeDocs(rng, cfg.docs, cfg.dim, cfg.textField)
	queryVecs := makeQueryVectors(rng, cfg.queries, cfg.dim)
	queryTexts := makeQueryTexts(rng, cfg.queries)

	segments := (cfg.docs + cfg.batch - 1) / cfg.batch
	fmt.Fprintf(out, "tpuf-bench  backend=%s  ns=%s\n", cfg.backend, cfg.namespace)
	fmt.Fprintf(out, "dim=%d metric=%s text-field=%q docs=%d batch=%d (=%d WAL segments) queries=%d warmup=%d top-k=%d n-probe=%d seed=%d\n\n",
		cfg.dim, cfg.metric, cfg.textField, cfg.docs, cfg.batch, segments, cfg.queries, cfg.warmup, cfg.topK, cfg.nProbe, cfg.seed)

	warmVecs := queryVecs[:min(cfg.warmup, len(queryVecs))]
	warmTexts := queryTexts[:min(cfg.warmup, len(queryTexts))]

	// 1. Write path: time each batch (each batch is one WAL segment + a manifest CAS).
	upsertRec := bench.NewRecorder("upsert (batch)")
	for start := 0; start < len(docs); start += cfg.batch {
		end := min(start+cfg.batch, len(docs))
		batch := docs[start:end]
		if err := upsertRec.Time(func() error { return ns.Upsert(ctx, batch) }); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}

	// 2. Query the UNINDEXED state: every query scans the WAL tail [0, WALSeq).
	// These phases never touch GetCached — the WAL is always read fresh — so the
	// cache counters stay flat here, which is itself the point.
	vecTail := bench.NewRecorder("query-vec (tail scan)")
	if err := warmVector(ctx, ns, warmVecs, cfg); err != nil {
		return fmt.Errorf("vector tail warmup: %w", err)
	}
	if err := measureVector(ctx, ns, vecTail, queryVecs, cfg); err != nil {
		return fmt.Errorf("vector tail scan: %w", err)
	}
	bm25Tail := bench.NewRecorder("query-bm25 (tail scan)")
	if cfg.textField != "" {
		if err := warmText(ctx, ns, warmTexts, cfg); err != nil {
			return fmt.Errorf("bm25 tail warmup: %w", err)
		}
		if err := measureText(ctx, ns, bm25Tail, queryTexts, cfg); err != nil {
			return fmt.Errorf("bm25 tail scan: %w", err)
		}
	}

	// 3. Build the index (single bulk operation).
	indexRec := bench.NewRecorder("index (build)")
	if err := indexRec.Time(func() error { return ns.Index(ctx) }); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	// 4. Query the INDEXED state. Snapshot the cache around the warmup and the
	// measured window separately, so we can show the cold start (warmup, mostly
	// misses as each index object is read for the first time) against the steady
	// state (measured, near-100% hits served from the DRAM tier).
	vecIdx := bench.NewRecorder("query-vec (indexed)")
	s0 := store.Stats()
	if err := warmVector(ctx, ns, warmVecs, cfg); err != nil {
		return fmt.Errorf("vector indexed warmup: %w", err)
	}
	s1 := store.Stats()
	if err := measureVector(ctx, ns, vecIdx, queryVecs, cfg); err != nil {
		return fmt.Errorf("vector indexed: %w", err)
	}
	vecCold, vecHot := s1.Sub(s0), store.Stats().Sub(s1)

	bm25Idx := bench.NewRecorder("query-bm25 (indexed)")
	var bm25Cold, bm25Hot cache.CacheStats
	if cfg.textField != "" {
		t0 := store.Stats()
		if err := warmText(ctx, ns, warmTexts, cfg); err != nil {
			return fmt.Errorf("bm25 indexed warmup: %w", err)
		}
		t1 := store.Stats()
		if err := measureText(ctx, ns, bm25Idx, queryTexts, cfg); err != nil {
			return fmt.Errorf("bm25 indexed: %w", err)
		}
		bm25Cold, bm25Hot = t1.Sub(t0), store.Stats().Sub(t1)
	}

	stats := []bench.Stats{
		upsertRec.Summarize(),
		vecTail.Summarize(),
		bm25Tail.Summarize(),
		indexRec.Summarize(),
		vecIdx.Summarize(),
		bm25Idx.Summarize(),
	}
	if err := bench.WriteTable(out, stats); err != nil {
		return err
	}

	info, err := ns.Info(ctx)
	if err != nil {
		return fmt.Errorf("reading manifest: %w", err)
	}
	fmt.Fprintf(out, "\nmanifest: docCount=%d walSeq=%d indexedUpTo=%d indexEpoch=%d\n",
		info.DocCount, info.WALSeq, info.IndexedUpTo, info.IndexEpoch)
	if err := printIndexSummary(ctx, out, store, cfg.namespace, info.IndexEpoch); err != nil {
		return err
	}

	// 5. Cache hot/cold breakdown for the indexed query phases. Misses are cold
	// reads that fell through to object storage; hits were served from DRAM.
	fmt.Fprintf(out, "\nDRAM cache (indexed query phases — tail scans bypass it by design):\n")
	printCacheLine(out, "query-vec  warmup (cold start)", vecCold)
	printCacheLine(out, "query-vec  measured (steady)  ", vecHot)
	if cfg.textField != "" {
		printCacheLine(out, "query-bm25 warmup (cold start)", bm25Cold)
		printCacheLine(out, "query-bm25 measured (steady)  ", bm25Hot)
	}

	if cfg.backend == "s3" {
		fmt.Fprintf(out, "\nnamespace %q persists in the bucket; delete it from the MinIO console to reclaim space.\n", cfg.namespace)
	}
	return nil
}

// runMultiTenant creates many namespaces (each upserted + indexed), then drives a
// concurrent query load across them. This is turbopuffer's real regime: many
// tenants sharing one DRAM cache, queried in parallel. With --cache-objects set
// below the resident working set, the shared cache evicts and cold-start misses
// recur — the pressure a bounded DRAM tier (and, in the real product, an NVMe
// tier) exists to absorb.
func runMultiTenant(ctx context.Context, cfg config, out io.Writer) error {
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}
	store, err := newCacheStore(cfg, backend, cfg.cacheObj, "multitenant")
	if err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(cfg.seed))

	capDesc := "unbounded"
	if cfg.cacheObj > 0 {
		capDesc = fmt.Sprintf("%d objects", cfg.cacheObj)
	}
	nvmeDesc := "off"
	if cfg.nvmeEnabled() {
		nvmeDesc = fmt.Sprintf("%d-slot ring at %s", cfg.nvmeSlots, cfg.nvmeDir)
	}
	fmt.Fprintf(out, "tpuf-bench  backend=%s  MULTI-TENANT\n", cfg.backend)
	fmt.Fprintf(out, "namespaces=%d concurrency=%d dram=%s nvme=%s | per-tenant: dim=%d docs=%d batch=%d | queries=%d top-k=%d n-probe=%d seed=%d\n\n",
		cfg.namespaces, cfg.concurrency, capDesc, nvmeDesc, cfg.dim, cfg.docs, cfg.batch, cfg.queries, cfg.topK, cfg.nProbe, cfg.seed)

	// Setup: create, upsert, and index every tenant. Time the per-tenant index
	// build so the table shows what publishing N epochs costs.
	handles := make([]*engine.Namespace, cfg.namespaces)
	names := make([]string, cfg.namespaces)
	tenantHits := make([]atomic.Int64, cfg.namespaces) // queries served per tenant during the load
	indexRec := bench.NewRecorder("index (build/tenant)")
	setupStart := time.Now()
	for i := range handles {
		name := fmt.Sprintf("%s-t%d", cfg.namespace, i)
		ns := engine.Open(store, name)
		if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: cfg.textField}); err != nil {
			return fmt.Errorf("creating %s: %w", name, err)
		}
		docs := makeDocs(rng, cfg.docs, cfg.dim, cfg.textField) // rng advances → distinct data per tenant
		for start := 0; start < len(docs); start += cfg.batch {
			end := min(start+cfg.batch, len(docs))
			if err := ns.Upsert(ctx, docs[start:end]); err != nil {
				return fmt.Errorf("upsert %s: %w", name, err)
			}
		}
		if err := indexRec.Time(func() error { return ns.Index(ctx) }); err != nil {
			return fmt.Errorf("index %s: %w", name, err)
		}
		handles[i] = ns
		names[i] = name
	}
	fmt.Fprintf(out, "setup: %d tenants × %d docs created + indexed in %s\n\n",
		cfg.namespaces, cfg.docs, time.Since(setupStart).Round(time.Millisecond))

	// Shared query pools (random queries; vectors need only match the dimension).
	// Bounded so a huge --queries does not blow up memory.
	poolSize := min(cfg.queries, 2000)
	if poolSize < 1 {
		poolSize = 1
	}
	vecPool := makeQueryVectors(rng, poolSize, cfg.dim)
	textPool := makeQueryTexts(rng, poolSize)

	// Warm every tenant once so steady-state numbers aren't dominated by each
	// tenant's first cold read; with a bounded cache the eviction story still
	// shows during the measured window once the working set exceeds capacity.
	for _, ns := range handles {
		if err := vectorQuery(ctx, ns, vecPool[0], cfg); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}

	// Run both concurrent phases under a live dashboard (on a TTY) or plain
	// progress lines (otherwise). The dashboard reads the shared phase counters;
	// an abort (ctrl+c) cancels loadCtx so in-flight queries stop.
	haveText := cfg.textField != ""
	loadCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var vecDone, bm25Done atomic.Int64
	var vStats, tStats bench.Stats
	var vQPS, tQPS float64
	var vCache, tCache cache.CacheStats
	var loadErr error

	phases := []loadPhase{{name: "query-vec", total: int64(cfg.queries), done: &vecDone}}
	if haveText {
		phases = append(phases, loadPhase{name: "query-bm25", total: int64(cfg.queries), done: &bm25Done})
	}

	cacheBefore := store.Stats()
	start := time.Now()
	displayLoad("multi-tenant load", phases, store, cacheBefore, start, cancel, func() {
		vStats, vQPS, vCache, loadErr = concurrentLoad(loadCtx, "query-vec (concurrent)", cfg, store, &vecDone, func(r *rand.Rand) error {
			i := r.Intn(len(handles))
			tenantHits[i].Add(1)
			return vectorQuery(loadCtx, handles[i], vecPool[r.Intn(len(vecPool))], cfg)
		})
		if loadErr != nil || !haveText {
			return
		}
		tStats, tQPS, tCache, loadErr = concurrentLoad(loadCtx, "query-bm25 (concurrent)", cfg, store, &bm25Done, func(r *rand.Rand) error {
			i := r.Intn(len(handles))
			tenantHits[i].Add(1)
			return textQuery(loadCtx, handles[i], textPool[r.Intn(len(textPool))], cfg)
		})
	})
	if loadErr != nil {
		return fmt.Errorf("load: %w", loadErr)
	}

	stats := []bench.Stats{indexRec.Summarize(), vStats}
	if haveText {
		stats = append(stats, tStats)
	}
	if err := bench.WriteTable(out, stats); err != nil {
		return err
	}

	fmt.Fprintf(out, "\nachieved throughput (%d concurrent workers, wall-clock):\n", cfg.concurrency)
	fmt.Fprintf(out, "  query-vec  : %8.0f qps\n", vQPS)
	if haveText {
		fmt.Fprintf(out, "  query-bm25 : %8.0f qps\n", tQPS)
	}

	fmt.Fprintf(out, "\nper-tenant breakdown (%d namespaces):\n", cfg.namespaces)
	if err := printTenantTable(ctx, out, names, handles, tenantHits); err != nil {
		return err
	}

	cacheTitle := "shared DRAM cache"
	if cfg.nvmeEnabled() {
		cacheTitle = "shared DRAM+NVMe cache"
	}
	fmt.Fprintf(out, "\n%s under concurrent load (working set = %d tenants):\n", cacheTitle, cfg.namespaces)
	printCacheFull(out, "query-vec  load", vCache)
	if haveText {
		printCacheFull(out, "query-bm25 load", tCache)
	}
	switch {
	case cfg.nvmeEnabled():
		fmt.Fprintf(out, "  (DRAM capped at %s, NVMe ring = %d slots; DRAM evictions that would be S3-cold reads are absorbed as NVMe-hits — only the first touch of each epoch object is truly S3-cold)\n", capDesc, cfg.nvmeSlots)
	case cfg.cacheObj > 0:
		fmt.Fprintf(out, "  (cache capped at %d objects; misses+evictions are cold-start recurrence under tenant churn — add --nvme-dir to absorb them in the warm tier)\n", cfg.cacheObj)
	default:
		fmt.Fprintf(out, "  (cache unbounded; once warm every tenant stays resident — set --cache-objects to force eviction)\n")
	}

	if cfg.backend == "s3" {
		fmt.Fprintf(out, "\n%d namespaces %q-t0..t%d persist in the bucket; delete them from the MinIO console to reclaim space.\n",
			cfg.namespaces, cfg.namespace, cfg.namespaces-1)
	}
	return nil
}

// concurrentLoad runs cfg.queries query closures spread across cfg.concurrency
// worker goroutines, returning aggregate latency stats, achieved wall-clock
// throughput, and the cache delta over the load. Each worker keeps its own
// Recorder and PRNG (neither is safe for concurrent use); the per-worker
// recorders are merged once all workers finish.
func concurrentLoad(ctx context.Context, name string, cfg config, store *cache.Store, counter *atomic.Int64, do func(r *rand.Rand) error) (bench.Stats, float64, cache.CacheStats, error) {
	recs := make([]*bench.Recorder, cfg.concurrency)
	errs := make([]error, cfg.concurrency)
	base, rem := cfg.queries/cfg.concurrency, cfg.queries%cfg.concurrency

	cacheBefore := store.Stats()
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < cfg.concurrency; w++ {
		recs[w] = bench.NewRecorder("")
		n := base
		if w < rem {
			n++
		}
		wg.Add(1)
		go func(w, n int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(cfg.seed + int64(w) + 1))
			rec := recs[w]
			for i := 0; i < n; i++ {
				if err := rec.Time(func() error { return do(r) }); err != nil {
					errs[w] = err
					return
				}
				counter.Add(1) // shared progress counter read by the live dashboard
			}
		}(w, n)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for _, e := range errs {
		if e != nil {
			return bench.Stats{}, 0, cache.CacheStats{}, e
		}
	}
	merged := bench.Combine(name, recs...).Summarize()
	qps := 0.0
	if elapsed > 0 {
		qps = float64(merged.Count) / elapsed.Seconds()
	}
	return merged, qps, store.Stats().Sub(cacheBefore), nil
}

// runColdStart is the controlled cold-vs-hot experiment. It builds one indexed
// namespace, then for each trial wraps the SAME backend in a fresh, empty cache
// and times a query twice: the first call is cold (the index objects must be
// fetched from object storage), the immediately-repeated identical call is hot
// (those objects are now resident in DRAM). Aggregating both gives a direct
// latency comparison plus proof of the mechanism: the cold pass records cache
// misses, the hot pass records none.
//
// Note the manifest is read uncached on EVERY query (correctness rule 2), so even
// the hot call makes one object-store round-trip; the cold/hot delta isolates the
// index-object fetches the cache eliminates, not the whole query.
func runColdStart(ctx context.Context, cfg config, out io.Writer) error {
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}

	// Build the data once; the cache used here is irrelevant (thrown away).
	setup := engine.Open(cache.New(backend), cfg.namespace)
	if err := setup.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: cfg.textField}); err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}
	rng := rand.New(rand.NewSource(cfg.seed))
	docs := makeDocs(rng, cfg.docs, cfg.dim, cfg.textField)
	for start := 0; start < len(docs); start += cfg.batch {
		end := min(start+cfg.batch, len(docs))
		if err := setup.Upsert(ctx, docs[start:end]); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}
	if err := setup.Index(ctx); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	tiers := "empty cache → query once COLD → repeat same query HOT"
	if cfg.nvmeEnabled() {
		tiers = "empty cache → COLD (S3) → fresh DRAM over warm ring → WARM (NVMe) → repeat HOT (DRAM)"
	}
	fmt.Fprintf(out, "tpuf-bench  backend=%s  COLD-vs-HOT (fresh empty cache per trial)\n", cfg.backend)
	fmt.Fprintf(out, "trials=%d dim=%d docs=%d metric=%s text-field=%q top-k=%d n-probe=%d seed=%d nvme=%v\n",
		cfg.coldTrials, cfg.dim, cfg.docs, cfg.metric, cfg.textField, cfg.topK, cfg.nProbe, cfg.seed, cfg.nvmeEnabled())
	fmt.Fprintf(out, "(each trial: %s; manifest is read uncached every time)\n\n", tiers)

	vecs := makeQueryVectors(rng, cfg.coldTrials, cfg.dim)
	texts := makeQueryTexts(rng, cfg.coldTrials)

	vecCold := bench.NewRecorder("query-vec  COLD (cache empty)")
	vecWarm := bench.NewRecorder("query-vec  WARM (NVMe, DRAM cold)")
	vecHot := bench.NewRecorder("query-vec  HOT  (cache warm)")
	vRes, err := coldHotPairs(ctx, cfg, backend, "vec", vecCold, vecWarm, vecHot, func(ctx context.Context, ns *engine.Namespace, i int) error {
		return vectorQuery(ctx, ns, vecs[i], cfg)
	})
	if err != nil {
		return fmt.Errorf("vector cold/hot: %w", err)
	}

	stats := []bench.Stats{vecCold.Summarize()}
	if cfg.nvmeEnabled() {
		stats = append(stats, vecWarm.Summarize())
	}
	stats = append(stats, vecHot.Summarize())

	var bm25Cold, bm25Warm, bm25Hot *bench.Recorder
	var bRes coldHotResult
	if cfg.textField != "" {
		bm25Cold = bench.NewRecorder("query-bm25 COLD (cache empty)")
		bm25Warm = bench.NewRecorder("query-bm25 WARM (NVMe, DRAM cold)")
		bm25Hot = bench.NewRecorder("query-bm25 HOT  (cache warm)")
		bRes, err = coldHotPairs(ctx, cfg, backend, "bm25", bm25Cold, bm25Warm, bm25Hot, func(ctx context.Context, ns *engine.Namespace, i int) error {
			return textQuery(ctx, ns, texts[i], cfg)
		})
		if err != nil {
			return fmt.Errorf("bm25 cold/hot: %w", err)
		}
		stats = append(stats, bm25Cold.Summarize())
		if cfg.nvmeEnabled() {
			stats = append(stats, bm25Warm.Summarize())
		}
		stats = append(stats, bm25Hot.Summarize())
	}

	if err := bench.WriteTable(out, stats); err != nil {
		return err
	}

	fmt.Fprintf(out, "\ncold → hot (same query, second call served from DRAM):\n")
	printColdHot(out, "query-vec ", vecCold.Summarize(), vecHot.Summarize())
	printColdCache(out, cfg, vRes)
	if cfg.textField != "" {
		printColdHot(out, "query-bm25", bm25Cold.Summarize(), bm25Hot.Summarize())
		printColdCache(out, cfg, bRes)
	}
	if cfg.backend == "memory" {
		fmt.Fprintf(out, "\n(memory backend: every tier reads from RAM, so the latency gaps are small — the\n cache's payoff is avoiding the NETWORK, which only the s3 backend exposes. The tier\n COUNTERS below the latency are still the proof the promotion mechanism works.)\n")
	}
	return nil
}

// printColdCache reports the cache outcome of a cold/hot run: always the cold
// pass's S3-cold count and the hot pass's miss count (0 ⇒ fully DRAM-served), and
// — when the NVMe tier is on — the warm pass's NVMe-hit count proving the middle
// tier served the DRAM-evicted objects from disk with no network.
func printColdCache(out io.Writer, cfg config, r coldHotResult) {
	if cfg.nvmeEnabled() {
		fmt.Fprintf(out, "    cache: cold pass = %d S3-cold, warm pass = %d NVMe-hits / %d S3-cold (0 ⇒ disk served it), hot pass = %d S3-cold (0 ⇒ DRAM)\n",
			r.coldMiss, r.warmNVMe, r.warmMiss, r.hotMiss)
		return
	}
	fmt.Fprintf(out, "    cache: cold pass = %d misses total, hot pass = %d misses (0 ⇒ fully served from DRAM)\n", r.coldMiss, r.hotMiss)
}

// runGroupCommit demonstrates the group-commit extension's win
// (docs/extensions/group-commit.md): it fires --docs concurrent single-document
// upserts at one namespace TWICE — once through the default stateless
// Namespace.Upsert (one WAL segment per caller) and once through an opt-in
// engine.Committer (concurrent writes coalesced into shared WAL segments) — and
// reports the WAL segment count each way by listing the wal/ prefix. The segment
// reduction is the headline: many concurrent writers, far fewer durable PUTs.
//
// turbopuffer reports its WAL batches "concurrent writes to the same namespace
// into the same entry" at "1 WAL entry/sec/namespace" — THEIR figure; this demo
// only shows the same coalescing shape, with no artificial timer (we batch
// whatever is already queued when the previous flush finishes).
func runGroupCommit(ctx context.Context, cfg config, out io.Writer) error {
	concurrency := cfg.concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	// fire builds a fresh namespace, asks newWriter for the per-doc upsert
	// function bound to that namespace (plus a cleanup the committer uses to
	// Close/drain), runs --docs single-doc upserts across `concurrency`
	// goroutines, then lists the wal/ prefix so both runs are scored on the same
	// ground-truth metric: how many durable WAL segment objects were written.
	fire := func(name string, newWriter func(ns *engine.Namespace) (write func(context.Context, engine.Document) error, cleanup func())) (int, error) {
		backend, err := newBackend(cfg.backend)
		if err != nil {
			return 0, err
		}
		store := cache.New(backend)
		nsName := fmt.Sprintf("%s-%s", cfg.namespace, name)
		ns := engine.Open(store, nsName)
		if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: cfg.textField}); err != nil {
			return 0, fmt.Errorf("creating namespace: %w", err)
		}
		write, cleanup := newWriter(ns)

		rng := rand.New(rand.NewSource(cfg.seed))
		docs := makeDocs(rng, cfg.docs, cfg.dim, cfg.textField)

		var wg sync.WaitGroup
		sem := make(chan struct{}, concurrency)
		errs := make([]error, len(docs))
		start := time.Now()
		for i := range docs {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				errs[i] = write(ctx, docs[i])
			}(i)
		}
		wg.Wait()
		if cleanup != nil {
			cleanup() // committer: Close drains any pending batch before we list.
		}
		elapsed := time.Since(start)

		// Count failures rather than aborting: the default path genuinely
		// contends under high concurrency (the AppendWAL probe loop can exhaust
		// its 64-attempt budget when many writers collide on the same seq), and
		// surfacing that contention IS the motivation for group commit — which,
		// owning the only writer goroutine, never collides intra-process.
		failed := 0
		for _, err := range errs {
			if err != nil {
				failed++
			}
		}

		keys, err := store.List(ctx, nsName+"/wal/")
		if err != nil {
			return 0, fmt.Errorf("%s listing wal: %w", name, err)
		}
		note := ""
		if failed > 0 {
			note = fmt.Sprintf("  [%d upserts FAILED on WAL-append contention]", failed)
		}
		fmt.Fprintf(out, "  %-14s %6d upserts → %6d WAL segments  (%.2fs wall)%s\n",
			name+":", len(docs), len(keys), elapsed.Seconds(), note)
		return len(keys), nil
	}

	fmt.Fprintf(out, "tpuf-bench group-commit demo  backend=%s  ns=%s\n", cfg.backend, cfg.namespace)
	fmt.Fprintf(out, "%d concurrent single-doc upserts, concurrency=%d\n\n", cfg.docs, concurrency)

	// Baseline: the default stateless path — one WAL PUT + manifest CAS per caller.
	direct, err := fire("direct", func(ns *engine.Namespace) (func(context.Context, engine.Document) error, func()) {
		return func(ctx context.Context, doc engine.Document) error {
			return ns.Upsert(ctx, []engine.Document{doc})
		}, nil
	})
	if err != nil {
		return err
	}

	// Group commit: the same load, coalesced through one committer goroutine.
	grouped, err := fire("group-commit", func(ns *engine.Namespace) (func(context.Context, engine.Document) error, func()) {
		c := engine.NewCommitter(ns)
		return func(ctx context.Context, doc engine.Document) error {
			return c.Upsert(ctx, []engine.Document{doc})
		}, c.Close
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(out, "\nWAL segments: %d (direct) → %d (group-commit)", direct, grouped)
	if grouped > 0 && grouped < direct {
		fmt.Fprintf(out, "  =  %.1fx fewer durable PUTs for the same writes\n", float64(direct)/float64(grouped))
	} else {
		fmt.Fprintln(out)
	}
	return nil
}

// runFilterPlan demonstrates the bitmap filter planner's win
// (docs/extensions/bitmap-attribute-indexes.md): the same filtered vector query is
// far cheaper when the filter is SELECTIVE, because the planner flips to
// filter-first — it scores only the handful of documents the filter's bitmap
// selects and fetches only the clusters they live in, instead of probing the
// nearest clusters and testing every candidate. It builds one indexed namespace
// whose "sel" attribute partitions the corpus into selectivity bands, then for
// each band times a vector query filtered to that band, reporting per-query
// latency plus the cold-cache index-object fetch count (the work each plan does).
//
// The headline is the contrast: the most selective band runs filter-first and
// touches the fewest objects; the broad band falls back to a pruned search-first
// scan that looks like the unfiltered baseline. Same answers either way (the
// engine still applies Filter.Match) — only the work differs.
func runFilterPlan(ctx context.Context, cfg config, out io.Writer) error {
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}
	store := cache.New(backend)
	ns := engine.Open(store, cfg.namespace)
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: ""}); err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}

	// Selectivity bands: each doc gets a "sel" value whose share of the corpus is
	// the band's selectivity. "rare" is ~0.2%, "broad" is the bulk — spanning the
	// filter-first/search-first decision.
	bands := []struct {
		name string
		frac float64
	}{
		{"rare", 0.002},
		{"narrow", 0.02},
		{"mid", 0.1},
		{"broad", 0.5},
	}
	pick := func(r *rand.Rand) int {
		x := r.Float64()
		cum := 0.0
		for i, b := range bands {
			cum += b.frac
			if x < cum {
				return i
			}
		}
		return len(bands) - 1
	}

	// Give each band its own region of vector space (a distinct centroid the band's
	// vectors cluster around). This models the realistic case the feature targets —
	// where a categorical attribute correlates with location in vector space (all
	// "premium" items look alike) — so a band's documents concentrate in a few IVF
	// clusters and the cluster-level prune actually skips fetches. With fully random
	// vectors a selective filter is spread thin across every cluster, the doc's
	// explicit worst case for any partition-based ANN index under filtering.
	rng := rand.New(rand.NewSource(cfg.seed))
	centers := make([][]float32, len(bands))
	for b := range centers {
		c := make([]float32, cfg.dim)
		for j := range c {
			c[j] = rng.Float32() * 10 // well-separated band centroids
		}
		centers[b] = c
	}

	docs := make([]engine.Document, cfg.docs)
	for i := range docs {
		b := pick(rng)
		v := make([]float32, cfg.dim)
		for j := range v {
			v[j] = centers[b][j] + (rng.Float32()*2-1)*0.3 // tight jitter around the band center
		}
		docs[i] = engine.Document{ID: fmt.Sprintf("doc-%d", i), Vector: v, Attributes: map[string]any{"sel": bands[b].name}}
	}
	for start := 0; start < len(docs); start += cfg.batch {
		end := min(start+cfg.batch, len(docs))
		if err := ns.Upsert(ctx, docs[start:end]); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}
	if err := ns.Index(ctx); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	fmt.Fprintf(out, "tpuf-bench  backend=%s  FILTER-PLAN (bitmap attribute index)\n", cfg.backend)
	fmt.Fprintf(out, "docs=%d dim=%d metric=%s queries=%d top-k=%d n-probe=%d seed=%d\n",
		cfg.docs, cfg.dim, cfg.metric, cfg.queries, cfg.topK, cfg.nProbe, cfg.seed)
	fmt.Fprintf(out, "(same vector query, filtered to each selectivity band; selective ⇒ filter-first, broad ⇒ search-first prune)\n\n")

	queryVecs := makeQueryVectors(rng, cfg.queries, cfg.dim)

	// timeFilter runs the measured queries under one filter, returning the latency
	// stats and the average cold-cache index-object fetch count (a fresh empty
	// cache per query isolates exactly the objects that plan touched).
	timeFilter := func(label string, f engine.Filter) (bench.Stats, float64, error) {
		rec := bench.NewRecorder(label)
		var totalMiss uint64
		for _, v := range queryVecs {
			cold := cache.New(backend)
			err := rec.Time(func() error {
				_, e := engine.Open(cold, cfg.namespace).Query(ctx, engine.QueryParams{
					RankBy: engine.RankBy{Vector: v}, Filter: f, TopK: cfg.topK, NProbe: cfg.nProbe,
				})
				return e
			})
			if err != nil {
				return bench.Stats{}, 0, err
			}
			totalMiss += cold.Stats().Misses
		}
		return rec.Summarize(), float64(totalMiss) / float64(len(queryVecs)), nil
	}

	var stats []bench.Stats
	fetches := map[string]float64{}

	base, baseFetch, err := timeFilter("unfiltered (baseline)", engine.Filter{})
	if err != nil {
		return fmt.Errorf("unfiltered: %w", err)
	}
	stats = append(stats, base)
	fetches["unfiltered (baseline)"] = baseFetch

	for _, b := range bands {
		label := fmt.Sprintf("filter sel=%s", b.name)
		s, fetch, err := timeFilter(label, engine.Filter{Op: "eq", Field: "sel", Value: b.name})
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		stats = append(stats, s)
		fetches[label] = fetch
	}

	if err := bench.WriteTable(out, stats); err != nil {
		return err
	}

	fmt.Fprintf(out, "\ncold-cache index objects fetched per query (lower ⇒ less work; filter-first prunes hardest):\n")
	fmt.Fprintf(out, "  %-26s %6.1f objects\n", "unfiltered (baseline)", fetches["unfiltered (baseline)"])
	for _, b := range bands {
		label := fmt.Sprintf("filter sel=%s", b.name)
		fmt.Fprintf(out, "  %-26s %6.1f objects\n", label, fetches[label])
	}
	fmt.Fprintf(out, "\nthe most selective bands run filter-first (score only the bitmap's matches, fetch only their\nclusters); broad filters fall back to a pruned search-first scan. The answer set is identical to\nthe per-candidate path in every band — Filter.Match still decides membership.\n")
	if cfg.backend == "s3" {
		fmt.Fprintf(out, "\nnamespace %q persists in the bucket; delete it from the MinIO console to reclaim space.\n", cfg.namespace)
	}
	return nil
}

// runHybrid is the controlled recall experiment that proves hybrid fusion beats
// either single retrieval mode. It builds a LABELED synthetic corpus where, for
// each query, exactly one document is the planted ground-truth answer that is
// moderately strong on BOTH signals — close (but not closest) in vector space
// and a partial (not perfect) BM25 match. Alongside it sit two decoys per query:
// a vector decoy that is the literal nearest vector but shares no query terms,
// and a text decoy that is a perfect keyword match but far in vector space. So
// vector-only retrieval ranks the vector decoy first, BM25-only ranks the text
// decoy first, and only RRF — which rewards consistency across both lists —
// floats the planted answer to the top. We then report recall@TopK of the
// planted answer for vector-only, bm25-only, and hybrid.
//
// This is pure query-side measurement: it builds one indexed namespace and reads
// it three ways, touching no manifest/WAL/epoch write path beyond the normal
// upsert+index setup.
func runHybrid(ctx context.Context, cfg config, out io.Writer) error {
	if cfg.textField == "" {
		return fmt.Errorf("hybrid experiment needs a text field; pass --text-field (default body) and do not set it empty")
	}
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}
	store := cache.New(backend)
	ns := engine.Open(store, cfg.namespace)
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric, TextField: cfg.textField}); err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}

	rng := rand.New(rand.NewSource(cfg.seed))
	docs, labels := makeLabeledHybridSet(rng, cfg)

	for start := 0; start < len(docs); start += cfg.batch {
		end := min(start+cfg.batch, len(docs))
		if err := ns.Upsert(ctx, docs[start:end]); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}
	if err := ns.Index(ctx); err != nil {
		return fmt.Errorf("index: %w", err)
	}

	fmt.Fprintf(out, "tpuf-bench  backend=%s  HYBRID-FUSION recall (RRF k=60)\n", cfg.backend)
	fmt.Fprintf(out, "queries=%d docs=%d dim=%d metric=%s text-field=%q top-k=%d n-probe=%d seed=%d\n",
		len(labels), len(docs), cfg.dim, cfg.metric, cfg.textField, cfg.topK, cfg.nProbe, cfg.seed)
	fmt.Fprintf(out, "(each query has one planted answer that is moderate on BOTH signals, plus a vector-only and a text-only decoy)\n\n")

	var vecHits, textHits, hybridHits int
	for _, lbl := range labels {
		vr, err := ns.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Vector: lbl.vector}, TopK: cfg.topK, NProbe: cfg.nProbe})
		if err != nil {
			return fmt.Errorf("vector query: %w", err)
		}
		tr, err := ns.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Text: lbl.text}, TopK: cfg.topK})
		if err != nil {
			return fmt.Errorf("bm25 query: %w", err)
		}
		hr, err := ns.Query(ctx, engine.QueryParams{RankBy: engine.RankBy{Vector: lbl.vector, Text: lbl.text}, TopK: cfg.topK, NProbe: cfg.nProbe})
		if err != nil {
			return fmt.Errorf("hybrid query: %w", err)
		}
		if containsID(vr, lbl.answer) {
			vecHits++
		}
		if containsID(tr, lbl.answer) {
			textHits++
		}
		if containsID(hr, lbl.answer) {
			hybridHits++
		}
	}

	n := float64(len(labels))
	fmt.Fprintf(out, "recall@%d of the planted answer:\n", cfg.topK)
	fmt.Fprintf(out, "  vector-only : %5.1f%%  (%d/%d)\n", 100*float64(vecHits)/n, vecHits, len(labels))
	fmt.Fprintf(out, "  bm25-only   : %5.1f%%  (%d/%d)\n", 100*float64(textHits)/n, textHits, len(labels))
	fmt.Fprintf(out, "  hybrid RRF  : %5.1f%%  (%d/%d)\n", 100*float64(hybridHits)/n, hybridHits, len(labels))
	fmt.Fprintf(out, "\nhybrid fuses two independent rankings (1/(60+rank) per list); a doc consistently near the\ntop of BOTH beats one that is #1 in only one — so recall rises above either mode alone.\n")
	if cfg.topK > 1 {
		fmt.Fprintf(out, "(re-run with --top-k 1 to see the gap most sharply: each single mode's top slot is taken by its decoy.)\n")
	}
	if cfg.backend == "s3" {
		fmt.Fprintf(out, "\nnamespace %q persists in the bucket; delete it from the MinIO console to reclaim space.\n", cfg.namespace)
	}
	return nil
}

// runRaBitQ is the controlled experiment proving the True RaBitQ win: at a fixed
// recall of the exact nearest neighbor, the True RaBitQ prefilter needs a SMALLER
// rerank shortlist than the lite sign-bit code (docs/extensions/true-rabitq.md).
//
// It builds one indexed namespace of Gaussian-spread vectors (so nearest
// neighbors are well separated and the prefilter's ranking quality, not the data,
// decides recall), brute-forces each query's true nearest neighbor, then sweeps
// the shortlist size. At each size it asks engine.PrefilterShortlist — the exact
// scoring the live query path uses — how often the true neighbor survives the
// binary scan, scored both by the True RaBitQ unbiased estimator and by the lite
// agreement. Recall is reported over the reachable queries (those whose true
// neighbor lands in a probed cluster) so the IVF probe-coverage factor does not
// muddy the prefilter comparison.
//
// This is pure query-side measurement over a normal upsert+index setup; it touches
// no manifest/WAL/epoch write path beyond building the one namespace.
func runRaBitQ(ctx context.Context, cfg config, out io.Writer) error {
	backend, err := newBackend(cfg.backend)
	if err != nil {
		return err
	}
	store := cache.New(backend)
	ns := engine.Open(store, cfg.namespace)
	if err := ns.Create(ctx, engine.NamespaceConfig{Dimension: cfg.dim, Metric: cfg.metric}); err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}

	rng := rand.New(rand.NewSource(cfg.seed))
	vecs := make([][]float32, cfg.docs)
	docs := make([]engine.Document, cfg.docs)
	for i := range docs {
		v := gaussVec(rng, cfg.dim)
		vecs[i] = v
		docs[i] = engine.Document{ID: fmt.Sprintf("doc-%d", i), Vector: v}
	}
	queries := make([][]float32, cfg.queries)
	for i := range queries {
		queries[i] = gaussVec(rng, cfg.dim)
	}

	for start := 0; start < len(docs); start += cfg.batch {
		end := min(start+cfg.batch, len(docs))
		if err := ns.Upsert(ctx, docs[start:end]); err != nil {
			return fmt.Errorf("upsert: %w", err)
		}
	}
	if err := ns.Index(ctx); err != nil {
		return fmt.Errorf("index: %w", err)
	}
	m, err := ns.Info(ctx)
	if err != nil {
		return fmt.Errorf("info: %w", err)
	}

	// Brute-force ground truth: the exact nearest neighbor per query.
	truth := make([]string, len(queries))
	for qi, q := range queries {
		best, bestD := "", math.Inf(1)
		for i, v := range vecs {
			if d := engine.Distance(cfg.metric, q, v); d < bestD {
				best, bestD = fmt.Sprintf("doc-%d", i), d
			}
		}
		truth[qi] = best
	}

	// nProbe is generous so most queries are reachable; the prefilter quality is
	// then the only differentiator between the two scorers.
	nProbe := cfg.nProbe
	if nProbe < 8 {
		nProbe = 8
	}
	reachable := 0
	for qi, q := range queries {
		hit, err := engine.PrefilterShortlist(ctx, store, cfg.namespace, m, q, nProbe, cfg.docs, true)
		if err != nil {
			return fmt.Errorf("prefilter (reachability): %w", err)
		}
		if contains(hit, truth[qi]) {
			reachable++
		}
	}

	fmt.Fprintf(out, "tpuf-bench  backend=%s  TRUE RaBitQ vs lite — shortlist recall\n", cfg.backend)
	fmt.Fprintf(out, "docs=%d dim=%d metric=%s queries=%d n-probe=%d seed=%d\n", cfg.docs, cfg.dim, cfg.metric, cfg.queries, nProbe, cfg.seed)
	fmt.Fprintf(out, "reachable queries (true NN in a probed cluster): %d/%d\n\n", reachable, len(queries))
	if reachable == 0 {
		return fmt.Errorf("no reachable queries; raise --n-probe or --docs")
	}

	fmt.Fprintf(out, "  shortlist   True RaBitQ recall   lite recall\n")
	fmt.Fprintf(out, "  ---------   ------------------   -----------\n")
	shortlists := []int{1, 2, 5, 10, 20, 50}
	rabitqRec := make(map[int]float64)
	liteRec := make(map[int]float64)
	for _, sl := range shortlists {
		rHits, lHits := 0, 0
		for qi, q := range queries {
			rShort, err := engine.PrefilterShortlist(ctx, store, cfg.namespace, m, q, nProbe, sl, true)
			if err != nil {
				return fmt.Errorf("prefilter (rabitq): %w", err)
			}
			lShort, err := engine.PrefilterShortlist(ctx, store, cfg.namespace, m, q, nProbe, sl, false)
			if err != nil {
				return fmt.Errorf("prefilter (lite): %w", err)
			}
			if contains(rShort, truth[qi]) {
				rHits++
			}
			if contains(lShort, truth[qi]) {
				lHits++
			}
		}
		rabitqRec[sl] = float64(rHits) / float64(reachable)
		liteRec[sl] = float64(lHits) / float64(reachable)
		fmt.Fprintf(out, "  %9d   %17.1f%%   %10.1f%%\n", sl, 100*rabitqRec[sl], 100*liteRec[sl])
	}

	// Headline: the shortlist each scorer needs to reach a target recall. True
	// RaBitQ's smaller number is the win — fewer exact reranks for the same recall.
	target := 0.90
	rNeed := neededShortlist(shortlists, rabitqRec, target)
	lNeed := neededShortlist(shortlists, liteRec, target)
	fmt.Fprintf(out, "\nshortlist needed for >=%.0f%% recall:  True RaBitQ %s   vs   lite %s\n",
		100*target, shortlistLabel(rNeed), shortlistLabel(lNeed))
	fmt.Fprintf(out, "True RaBitQ ranks candidates by the paper's unbiased estimator (O(1/√D) error), so a\nfar smaller shortlist keeps the true neighbor — that is the smaller-shortlist-at-fixed-recall win.\n")
	if cfg.backend == "s3" {
		fmt.Fprintf(out, "\nnamespace %q persists in the bucket; delete it from the MinIO console to reclaim space.\n", cfg.namespace)
	}
	return nil
}

// gaussVec draws a vector of i.i.d. standard Gaussians, spreading directions over
// the sphere so the recall experiment has well-defined nearest neighbors.
func gaussVec(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

// contains reports whether ids holds target.
func contains(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// neededShortlist returns the smallest swept shortlist whose recall reaches target,
// or -1 if none in the sweep does.
func neededShortlist(shortlists []int, recall map[int]float64, target float64) int {
	for _, sl := range shortlists {
		if recall[sl] >= target {
			return sl
		}
	}
	return -1
}

// shortlistLabel renders a needed-shortlist value, marking the not-reached case.
func shortlistLabel(sl int) string {
	if sl < 0 {
		return ">swept-max"
	}
	return fmt.Sprintf("%d", sl)
}

// hybridLabel is one labeled query: a vector and a text that both point at the
// same planted ground-truth document id.
type hybridLabel struct {
	vector []float32
	text   string
	answer string
}

// makeLabeledHybridSet builds the corpus and the per-query labels for the hybrid
// experiment. For query q it plants three documents:
//
//   - answer-q : vector = query + moderate noise (close, rarely closest); text =
//     two of the query's terms among filler (a partial BM25 match).
//   - vdecoy-q : vector = query + tiny noise (the literal nearest neighbor); text
//     = filler only, sharing no query term (invisible to BM25).
//   - tdecoy-q : vector = random/far; text = all the query's terms repeated (the
//     BM25 runaway #1), so it dominates the keyword list but loses on vectors.
//
// Vector-only thus tops out at vdecoy, BM25-only at tdecoy, and only RRF — which
// rewards a doc that is high in BOTH lists — surfaces the planted answer.
func makeLabeledHybridSet(rng *rand.Rand, cfg config) ([]engine.Document, []hybridLabel) {
	var docs []engine.Document
	var labels []hybridLabel

	for q := 0; q < cfg.queries; q++ {
		// Two rare query terms unique to this query keep BM25 selective; filler is
		// drawn from the shared vocab so the index has realistic term frequencies.
		t1 := fmt.Sprintf("qterm%da", q)
		t2 := fmt.Sprintf("qterm%db", q)
		queryText := t1 + " " + t2

		base := makeQueryVectors(rng, 1, cfg.dim)[0]

		answerVec := jitter(rng, base, cfg.dim, 0.15) // moderately close
		answerText := strings.Join([]string{t1, filler(rng, 6)}, " ")

		vdecoyVec := jitter(rng, base, cfg.dim, 0.02) // the literal nearest neighbor
		vdecoyText := filler(rng, 8)                  // no query term → BM25-invisible

		tdecoyVec := makeQueryVectors(rng, 1, cfg.dim)[0]         // far in vector space
		tdecoyText := strings.Join([]string{t1, t2, t1, t2}, " ") // BM25 runaway #1

		answerID := fmt.Sprintf("answer-%d", q)
		docs = append(docs,
			engine.Document{ID: answerID, Vector: answerVec, Attributes: map[string]any{cfg.textField: answerText}},
			engine.Document{ID: fmt.Sprintf("vdecoy-%d", q), Vector: vdecoyVec, Attributes: map[string]any{cfg.textField: vdecoyText}},
			engine.Document{ID: fmt.Sprintf("tdecoy-%d", q), Vector: tdecoyVec, Attributes: map[string]any{cfg.textField: tdecoyText}},
		)
		labels = append(labels, hybridLabel{vector: base, text: queryText, answer: answerID})
	}
	return docs, labels
}

// jitter returns v perturbed by uniform noise of the given magnitude per
// component, leaving it near v in vector space.
func jitter(rng *rand.Rand, v []float32, dim int, mag float32) []float32 {
	out := make([]float32, dim)
	for i := 0; i < dim; i++ {
		out[i] = v[i] + (rng.Float32()*2-1)*mag
	}
	return out
}

// filler joins n random words from the shared vocab, used as non-matching body
// text so a document carries realistic length without matching any query term.
func filler(rng *rand.Rand, n int) string {
	words := make([]string, n)
	for i := range words {
		words[i] = vocab[rng.Intn(len(vocab))]
	}
	return strings.Join(words, " ")
}

// containsID reports whether any result carries the given id.
func containsID(results []engine.QueryResult, id string) bool {
	for _, r := range results {
		if r.ID == id {
			return true
		}
	}
	return false
}

// coldHotResult accumulates the cache outcomes a cold/hot run observed, so the
// report can prove the mechanism: the cold pass is all S3-cold misses, the warm
// (NVMe) pass — present only when the disk tier is on — is all NVMe-hits, and the
// hot pass is all DRAM-hits with no backend traffic.
type coldHotResult struct {
	coldMiss uint64 // S3-cold reads during the cold pass
	warmNVMe uint64 // NVMe-hits during the warm pass (DRAM evicted, disk warm)
	warmMiss uint64 // any S3-cold reads leaking into the warm pass (want 0)
	hotMiss  uint64 // S3-cold reads during the hot pass (want 0)
}

// coldHotPairs runs cfg.coldTrials trials and times the same query in two (or,
// with the NVMe tier on, three) cache states:
//
//   - COLD: a brand-new empty cache (DRAM, and a fresh empty ring if enabled) —
//     every index object is fetched from object storage.
//   - WARM (NVMe only): a fresh empty DRAM map over the now-warm ring from the
//     cold pass — the index objects are served from local disk, no network.
//   - HOT: the same warm cache again — served from DRAM.
//
// Without the NVMe tier the WARM pass is skipped and warm is the empty recorder.
// Each trial gets its own ring directory (scoped by tag) so trials do not warm
// each other.
func coldHotPairs(ctx context.Context, cfg config, backend storage.ObjectStore, tag string, cold, warm, hot *bench.Recorder, do func(context.Context, *engine.Namespace, int) error) (coldHotResult, error) {
	var res coldHotResult
	for i := 0; i < cfg.coldTrials; i++ {
		// Fresh empty cache for the cold pass. With NVMe on, the ring lives in a
		// per-trial subdir so it starts empty here and is reused warm below.
		coldStore, err := newCacheStore(cfg, backend, 0, fmt.Sprintf("coldstart/%s-%d", tag, i))
		if err != nil {
			return coldHotResult{}, err
		}
		coldNS := engine.Open(coldStore, cfg.namespace)

		before := coldStore.Stats()
		if err := cold.Time(func() error { return do(ctx, coldNS, i) }); err != nil {
			return coldHotResult{}, err
		}
		res.coldMiss += coldStore.Stats().Sub(before).Misses

		if cfg.nvmeEnabled() {
			// WARM pass: a brand-new DRAM map over the SAME warm ring directory,
			// so the index objects miss DRAM but hit the disk ring — the warm
			// tier's whole reason to exist.
			warmStore, err := newCacheStore(cfg, backend, 0, fmt.Sprintf("coldstart/%s-%d", tag, i))
			if err != nil {
				return coldHotResult{}, err
			}
			warmNS := engine.Open(warmStore, cfg.namespace)
			wb := warmStore.Stats()
			if err := warm.Time(func() error { return do(ctx, warmNS, i) }); err != nil {
				return coldHotResult{}, err
			}
			wd := warmStore.Stats().Sub(wb)
			res.warmNVMe += wd.NVMeHits
			res.warmMiss += wd.Misses

			// HOT pass: repeat on the warm store; now resident in DRAM.
			hb := warmStore.Stats()
			if err := hot.Time(func() error { return do(ctx, warmNS, i) }); err != nil {
				return coldHotResult{}, err
			}
			res.hotMiss += warmStore.Stats().Sub(hb).Misses
			continue
		}

		// No NVMe tier: HOT is the immediate repeat on the cold store (DRAM warm).
		mid := coldStore.Stats()
		if err := hot.Time(func() error { return do(ctx, coldNS, i) }); err != nil {
			return coldHotResult{}, err
		}
		res.hotMiss += coldStore.Stats().Sub(mid).Misses
	}
	return res, nil
}

// printColdHot reports the p50/p99 cold→hot improvement with a speedup factor.
func printColdHot(out io.Writer, label string, cold, hot bench.Stats) {
	fmt.Fprintf(out, "  %s : p50 %v → %v (%.1fx faster)   p99 %v → %v (%.1fx faster)\n",
		label, dur(cold.P50), dur(hot.P50), speedup(cold.P50, hot.P50),
		dur(cold.P99), dur(hot.P99), speedup(cold.P99, hot.P99))
}

// speedup is cold/hot as a ratio; 0 if hot is non-positive.
func speedup(cold, hot time.Duration) float64 {
	if hot <= 0 {
		return 0
	}
	return float64(cold) / float64(hot)
}

// dur formats a duration the same way the bench table does (re-exported helper).
func dur(x time.Duration) string { return bench.FormatDuration(x) }

// printTenantTable lists every namespace with its doc count, live index epoch,
// and how many queries it served during the concurrent load — so the report
// names the tenants and shows how evenly the random load spread across them.
func printTenantTable(ctx context.Context, out io.Writer, names []string, handles []*engine.Namespace, hits []atomic.Int64) error {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tDOCS\tEPOCH\tQUERIES")
	for i, ns := range handles {
		info, err := ns.Info(ctx)
		if err != nil {
			return fmt.Errorf("info %s: %w", names[i], err)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\n", names[i], info.DocCount, info.IndexEpoch, hits[i].Load())
	}
	return tw.Flush()
}

// printCacheLine renders one cache hit/miss line with its hot-hit rate. With the
// NVMe tier off this is the familiar DRAM-hit / miss split; with it on it breaks
// the fast hits into DRAM vs NVMe so a "miss" means only the true S3-cold reads.
func printCacheLine(out io.Writer, label string, s cache.CacheStats) {
	if s.NVMeHits > 0 || s.DRAMHits != s.Hits {
		fmt.Fprintf(out, "  %s : %6d DRAM-hits  %6d NVMe-hits  %4d S3-cold  → %5.1f%% warm\n",
			label, s.DRAMHits, s.NVMeHits, s.Misses, s.HitRate()*100)
		return
	}
	fmt.Fprintf(out, "  %s : %6d hits  %4d misses  → %5.1f%% hot\n", label, s.Hits, s.Misses, s.HitRate()*100)
}

// printCacheFull renders a cache line including evictions, for the multi-tenant
// load where capacity pressure makes evictions the interesting number. When the
// NVMe tier is engaged it shows the full three-way DRAM/NVMe/S3 split, so the
// payoff is visible: DRAM evictions that used to become S3-cold reads now land
// as NVMe-hits instead.
func printCacheFull(out io.Writer, label string, s cache.CacheStats) {
	if s.NVMeHits > 0 {
		fmt.Fprintf(out, "  %s : %8d DRAM-hits  %8d NVMe-hits  %7d S3-cold  %7d evictions  → %5.1f%% warm\n",
			label, s.DRAMHits, s.NVMeHits, s.Misses, s.Evictions, s.HitRate()*100)
		return
	}
	fmt.Fprintf(out, "  %s : %8d hits  %7d misses  %7d evictions  → %5.1f%% hot\n",
		label, s.Hits, s.Misses, s.Evictions, s.HitRate()*100)
}

// printIndexSummary reads the epoch's centroids file (uncached, so it does not
// perturb the cache counters) and reports how k-means partitioned the vectors:
// K ≈ √N clusters and their size spread. A text-only namespace has no centroids
// file, which is reported rather than treated as an error.
func printIndexSummary(ctx context.Context, out io.Writer, store *cache.Store, ns string, epoch int64) error {
	key := fmt.Sprintf("%s/index/v%d/centroids.json", ns, epoch)
	body, _, err := store.Get(ctx, key)
	if err != nil {
		fmt.Fprintf(out, "index: no centroid file (text-only namespace)\n")
		return nil
	}
	var cf engine.CentroidsFile
	if err := json.Unmarshal(body, &cf); err != nil {
		return fmt.Errorf("decoding centroids: %w", err)
	}
	sizes := append([]int(nil), cf.Sizes...)
	sort.Ints(sizes)
	total := 0
	for _, s := range sizes {
		total += s
	}
	lo, mid, hi := 0, 0, 0
	if len(sizes) > 0 {
		lo, mid, hi = sizes[0], sizes[len(sizes)/2], sizes[len(sizes)-1]
	}
	fmt.Fprintf(out, "index: epoch=%d  K=%d clusters (≈√%d) over %d vectors  cluster size min/median/max=%d/%d/%d\n",
		epoch, cf.K, total, total, lo, mid, hi)
	return nil
}

// vectorQuery runs a single vector query, discarding the results — the benchmark
// cares about latency, not hits.
func vectorQuery(ctx context.Context, ns *engine.Namespace, v []float32, cfg config) error {
	_, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Vector: v},
		TopK:   cfg.topK,
		NProbe: cfg.nProbe,
	})
	return err
}

// textQuery runs a single BM25 query, discarding the results.
func textQuery(ctx context.Context, ns *engine.Namespace, text string, cfg config) error {
	_, err := ns.Query(ctx, engine.QueryParams{
		RankBy: engine.RankBy{Text: text},
		TopK:   cfg.topK,
	})
	return err
}

// warmVector replays vecs without timing, to warm caches and amortize first-call
// costs before the measured run.
func warmVector(ctx context.Context, ns *engine.Namespace, vecs [][]float32, cfg config) error {
	for _, v := range vecs {
		if err := vectorQuery(ctx, ns, v, cfg); err != nil {
			return err
		}
	}
	return nil
}

// measureVector times one query per vector into rec.
func measureVector(ctx context.Context, ns *engine.Namespace, rec *bench.Recorder, vecs [][]float32, cfg config) error {
	for _, v := range vecs {
		if err := rec.Time(func() error { return vectorQuery(ctx, ns, v, cfg) }); err != nil {
			return err
		}
	}
	return nil
}

// warmText replays texts without timing.
func warmText(ctx context.Context, ns *engine.Namespace, texts []string, cfg config) error {
	for _, text := range texts {
		if err := textQuery(ctx, ns, text, cfg); err != nil {
			return err
		}
	}
	return nil
}

// measureText times one BM25 query per string into rec.
func measureText(ctx context.Context, ns *engine.Namespace, rec *bench.Recorder, texts []string, cfg config) error {
	for _, text := range texts {
		if err := rec.Time(func() error { return textQuery(ctx, ns, text, cfg) }); err != nil {
			return err
		}
	}
	return nil
}

// vocab is a small word pool for synthetic document bodies and BM25 queries; a
// fixed list keeps every term in the index so queries reliably match something.
var vocab = []string{
	"walrus", "quick", "ice", "storage", "object", "vector", "search", "index",
	"latency", "cache", "cluster", "query", "engine", "durable", "manifest",
	"epoch", "segment", "tail", "probe", "centroid",
}

// makeDocs builds n synthetic documents with random dim-length vectors and, when
// textField is set, a body of random words drawn from vocab.
func makeDocs(rng *rand.Rand, n, dim int, textField string) []engine.Document {
	docs := make([]engine.Document, n)
	for i := range docs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		attrs := map[string]any{"lang": "en"}
		if textField != "" {
			words := make([]string, 8)
			for k := range words {
				words[k] = vocab[rng.Intn(len(vocab))]
			}
			attrs[textField] = strings.Join(words, " ")
		}
		docs[i] = engine.Document{ID: fmt.Sprintf("doc-%d", i), Vector: v, Attributes: attrs}
	}
	return docs
}

// makeQueryVectors returns n random dim-length query vectors.
func makeQueryVectors(rng *rand.Rand, n, dim int) [][]float32 {
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		vecs[i] = v
	}
	return vecs
}

// makeQueryTexts returns n two-word query strings drawn from vocab.
func makeQueryTexts(rng *rand.Rand, n int) []string {
	texts := make([]string, n)
	for i := range texts {
		texts[i] = vocab[rng.Intn(len(vocab))] + " " + vocab[rng.Intn(len(vocab))]
	}
	return texts
}

// parseFlags builds a config from args, writing usage to out on -h/--help.
func parseFlags(args []string, out io.Writer) (config, error) {
	fs := flag.NewFlagSet("tpuf-bench", flag.ContinueOnError)
	fs.SetOutput(out)

	cfg := config{}
	fs.StringVar(&cfg.backend, "backend", envOr("TPUF_BACKEND", "memory"), `backend: "memory" (no infra) or "s3" (MinIO)`)
	fs.StringVar(&cfg.namespace, "namespace", "", "namespace name (default: a unique bench-<ns> name)")
	fs.IntVar(&cfg.dim, "dim", 128, "vector dimension")
	fs.StringVar(&cfg.metric, "metric", "cosine", `distance metric: "cosine" or "euclidean"`)
	fs.StringVar(&cfg.textField, "text-field", "body", `attribute to index for BM25 ("" disables BM25)`)
	fs.IntVar(&cfg.docs, "docs", 1000, "number of documents to upsert")
	fs.IntVar(&cfg.batch, "batch", 100, "documents per upsert (each batch is one WAL segment)")
	fs.IntVar(&cfg.queries, "queries", 200, "measured queries per phase")
	fs.IntVar(&cfg.warmup, "warmup", 20, "warmup queries discarded before measuring")
	fs.IntVar(&cfg.topK, "top-k", 10, "results per query")
	fs.IntVar(&cfg.nProbe, "n-probe", 3, "clusters probed per vector query")
	fs.Int64Var(&cfg.seed, "seed", 1, "PRNG seed for reproducible data")
	fs.IntVar(&cfg.namespaces, "namespaces", 1, "tenant count; >1 runs the multi-tenant concurrent benchmark")
	fs.IntVar(&cfg.concurrency, "concurrency", 1, "worker goroutines issuing queries (multi-tenant mode)")
	fs.IntVar(&cfg.cacheObj, "cache-objects", 0, "DRAM cache capacity in objects (0 = unbounded); set below the working set to force eviction")
	fs.StringVar(&cfg.nvmeDir, "nvme-dir", "", "if set, enable the NVMe ring-buffer cache tier (a FIFO of index objects on local disk) rooted at this directory")
	fs.IntVar(&cfg.nvmeSlots, "nvme-slots", 4096, "NVMe ring capacity in objects (only used with --nvme-dir); make it larger than --cache-objects to see DRAM evictions become NVMe hits")
	fs.IntVar(&cfg.coldTrials, "coldstart-trials", 0, "if >0, run the cold-vs-hot experiment: time each query with an empty cache (cold) then again warm (hot), over this many trials")
	fs.BoolVar(&cfg.hybrid, "hybrid", false, "run the hybrid-fusion recall experiment: build a labeled set and compare vector-only / bm25-only / hybrid (RRF) recall")
	fs.BoolVar(&cfg.groupCommit, "group-commit", false, "run the group-commit demo: fire --docs concurrent single-doc upserts via the default path and via a Committer, comparing WAL segment counts (docs/extensions/group-commit.md)")
	fs.BoolVar(&cfg.filterPlan, "filter-plan", false, "run the bitmap filter-plan demo: time the same filtered vector query at high (filter-first) vs low (search-first) selectivity (docs/extensions/bitmap-attribute-indexes.md)")
	fs.BoolVar(&cfg.rabitq, "rabitq", false, "run the True RaBitQ demo: sweep the prefilter shortlist size and compare True RaBitQ vs lite recall of the exact nearest neighbor (docs/extensions/true-rabitq.md)")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.namespace == "" {
		cfg.namespace = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	return cfg, nil
}

// validate rejects configurations that would produce meaningless measurements.
func (c config) validate() error {
	switch {
	case c.backend != "memory" && c.backend != "s3":
		return fmt.Errorf("unknown backend %q (want \"memory\" or \"s3\")", c.backend)
	case c.dim <= 0:
		return fmt.Errorf("dim must be positive, got %d", c.dim)
	case c.docs <= 0:
		return fmt.Errorf("docs must be positive, got %d", c.docs)
	case c.batch <= 0:
		return fmt.Errorf("batch must be positive, got %d", c.batch)
	case c.queries <= 0:
		return fmt.Errorf("queries must be positive, got %d", c.queries)
	case c.warmup < 0:
		return fmt.Errorf("warmup must not be negative, got %d", c.warmup)
	case c.topK <= 0:
		return fmt.Errorf("top-k must be positive, got %d", c.topK)
	case c.namespaces < 1:
		return fmt.Errorf("namespaces must be >= 1, got %d", c.namespaces)
	case c.concurrency < 1:
		return fmt.Errorf("concurrency must be >= 1, got %d", c.concurrency)
	case c.cacheObj < 0:
		return fmt.Errorf("cache-objects must not be negative, got %d", c.cacheObj)
	case c.nvmeEnabled() && c.nvmeSlots <= 0:
		return fmt.Errorf("nvme-slots must be positive when --nvme-dir is set, got %d", c.nvmeSlots)
	case c.coldTrials < 0:
		return fmt.Errorf("coldstart-trials must not be negative, got %d", c.coldTrials)
	}
	return nil
}

// newBackend constructs the chosen ObjectStore. The s3 path reads the TPUF_S3_*
// and TPUF_BUCKET env vars (see internal/storage/s3.go).
func newBackend(backend string) (storage.ObjectStore, error) {
	switch backend {
	case "memory":
		return storage.New(), nil
	case "s3":
		store, err := storage.NewS3StoreFromEnv()
		if err != nil {
			return nil, fmt.Errorf("connecting to s3 backend: %w", err)
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unknown backend %q (want \"memory\" or \"s3\")", backend)
	}
}

// newCacheStore builds the cache for a benchmark run: the DRAM tier capped at
// dramCap (0 = unbounded), plus the optional NVMe ring-buffer tier when
// --nvme-dir is set. subdir scopes the ring's files within the configured
// directory so independent caches in one run (e.g. the per-trial caches of the
// cold-start experiment) do not collide on disk.
func newCacheStore(cfg config, backend storage.ObjectStore, dramCap int, subdir string) (*cache.Store, error) {
	if !cfg.nvmeEnabled() {
		return cache.NewWithCapacity(backend, dramCap), nil
	}
	dir := filepath.Join(cfg.nvmeDir, subdir)
	store, err := cache.NewWithNVMe(backend, dramCap, dir, cfg.nvmeSlots)
	if err != nil {
		return nil, fmt.Errorf("enabling nvme cache tier: %w", err)
	}
	return store, nil
}

// envOr returns the value of key, or def if it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
