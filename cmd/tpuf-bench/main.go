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
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
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
	namespaces  int // >1 switches to multi-tenant concurrent mode
	concurrency int // worker goroutines in multi-tenant mode
	cacheObj    int // DRAM cache capacity in objects; 0 = unbounded
	coldTrials  int // >0 runs the cold-vs-hot same-query experiment
}

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
	store := cache.NewWithCapacity(backend, cfg.cacheObj)
	rng := rand.New(rand.NewSource(cfg.seed))

	capDesc := "unbounded"
	if cfg.cacheObj > 0 {
		capDesc = fmt.Sprintf("%d objects", cfg.cacheObj)
	}
	fmt.Fprintf(out, "tpuf-bench  backend=%s  MULTI-TENANT\n", cfg.backend)
	fmt.Fprintf(out, "namespaces=%d concurrency=%d cache=%s | per-tenant: dim=%d docs=%d batch=%d | queries=%d top-k=%d n-probe=%d seed=%d\n\n",
		cfg.namespaces, cfg.concurrency, capDesc, cfg.dim, cfg.docs, cfg.batch, cfg.queries, cfg.topK, cfg.nProbe, cfg.seed)

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

	stats := []bench.Stats{indexRec.Summarize()}

	vStats, vQPS, vCache, err := concurrentLoad(ctx, "query-vec (concurrent)", cfg, store, func(r *rand.Rand) error {
		i := r.Intn(len(handles))
		tenantHits[i].Add(1)
		return vectorQuery(ctx, handles[i], vecPool[r.Intn(len(vecPool))], cfg)
	})
	if err != nil {
		return fmt.Errorf("vector load: %w", err)
	}
	stats = append(stats, vStats)

	var tQPS float64
	var tCache cache.CacheStats
	haveText := cfg.textField != ""
	if haveText {
		tStats, qps, tc, err := concurrentLoad(ctx, "query-bm25 (concurrent)", cfg, store, func(r *rand.Rand) error {
			i := r.Intn(len(handles))
			tenantHits[i].Add(1)
			return textQuery(ctx, handles[i], textPool[r.Intn(len(textPool))], cfg)
		})
		if err != nil {
			return fmt.Errorf("bm25 load: %w", err)
		}
		stats = append(stats, tStats)
		tQPS, tCache = qps, tc
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

	fmt.Fprintf(out, "\nshared DRAM cache under concurrent load (working set = %d tenants):\n", cfg.namespaces)
	printCacheFull(out, "query-vec  load", vCache)
	if haveText {
		printCacheFull(out, "query-bm25 load", tCache)
	}
	if cfg.cacheObj > 0 {
		fmt.Fprintf(out, "  (cache capped at %d objects; misses+evictions are cold-start recurrence under tenant churn)\n", cfg.cacheObj)
	} else {
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
func concurrentLoad(ctx context.Context, name string, cfg config, store *cache.Store, do func(r *rand.Rand) error) (bench.Stats, float64, cache.CacheStats, error) {
	recs := make([]*bench.Recorder, cfg.concurrency)
	errs := make([]error, cfg.concurrency)
	base, rem := cfg.queries/cfg.concurrency, cfg.queries%cfg.concurrency

	cacheBefore := store.Stats()
	var done atomic.Int64 // completed queries, for live progress
	var wg sync.WaitGroup
	start := time.Now()

	// Progress ticker → stderr (so it never pollutes the stdout report). It only
	// prints if the load runs longer than the interval, so fast runs (and tests)
	// stay silent. Stopped via the channel once all workers finish.
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				d := done.Load()
				el := time.Since(start).Seconds()
				c := store.Stats().Sub(cacheBefore)
				fmt.Fprintf(os.Stderr, "  …%s %d/%d queries (%.0f qps, cache %.0f%% hot)\n",
					name, d, cfg.queries, float64(d)/el, c.HitRate()*100)
			}
		}
	}()

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
				done.Add(1)
			}
		}(w, n)
	}
	wg.Wait()
	close(stop)
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

	fmt.Fprintf(out, "tpuf-bench  backend=%s  COLD-vs-HOT (fresh empty cache per trial)\n", cfg.backend)
	fmt.Fprintf(out, "trials=%d dim=%d docs=%d metric=%s text-field=%q top-k=%d n-probe=%d seed=%d\n",
		cfg.coldTrials, cfg.dim, cfg.docs, cfg.metric, cfg.textField, cfg.topK, cfg.nProbe, cfg.seed)
	fmt.Fprintf(out, "(each trial: empty cache → query once COLD → repeat same query HOT; manifest is read uncached both times)\n\n")

	vecs := makeQueryVectors(rng, cfg.coldTrials, cfg.dim)
	texts := makeQueryTexts(rng, cfg.coldTrials)

	vecCold := bench.NewRecorder("query-vec  COLD (cache empty)")
	vecHot := bench.NewRecorder("query-vec  HOT  (cache warm)")
	vColdMiss, vHotMiss, err := coldHotPairs(ctx, cfg, backend, vecCold, vecHot, func(ctx context.Context, ns *engine.Namespace, i int) error {
		return vectorQuery(ctx, ns, vecs[i], cfg)
	})
	if err != nil {
		return fmt.Errorf("vector cold/hot: %w", err)
	}

	stats := []bench.Stats{vecCold.Summarize(), vecHot.Summarize()}
	var bm25Cold, bm25Hot *bench.Recorder
	var bColdMiss, bHotMiss uint64
	if cfg.textField != "" {
		bm25Cold = bench.NewRecorder("query-bm25 COLD (cache empty)")
		bm25Hot = bench.NewRecorder("query-bm25 HOT  (cache warm)")
		bColdMiss, bHotMiss, err = coldHotPairs(ctx, cfg, backend, bm25Cold, bm25Hot, func(ctx context.Context, ns *engine.Namespace, i int) error {
			return textQuery(ctx, ns, texts[i], cfg)
		})
		if err != nil {
			return fmt.Errorf("bm25 cold/hot: %w", err)
		}
		stats = append(stats, bm25Cold.Summarize(), bm25Hot.Summarize())
	}

	if err := bench.WriteTable(out, stats); err != nil {
		return err
	}

	fmt.Fprintf(out, "\ncold → hot (same query, second call served from DRAM):\n")
	printColdHot(out, "query-vec ", vecCold.Summarize(), vecHot.Summarize())
	fmt.Fprintf(out, "    cache: cold pass = %d misses total, hot pass = %d misses (0 ⇒ fully served from DRAM)\n", vColdMiss, vHotMiss)
	if cfg.textField != "" {
		printColdHot(out, "query-bm25", bm25Cold.Summarize(), bm25Hot.Summarize())
		fmt.Fprintf(out, "    cache: cold pass = %d misses total, hot pass = %d misses (0 ⇒ fully served from DRAM)\n", bColdMiss, bHotMiss)
	}
	if cfg.backend == "memory" {
		fmt.Fprintf(out, "\n(memory backend: both cold and hot read from RAM, so the gap is small — the cache's\n payoff is avoiding the NETWORK, which only the s3 backend exposes.)\n")
	}
	return nil
}

// coldHotPairs runs cfg.coldTrials trials. Each trial uses a brand-new empty
// cache over the shared backend, times do() once (cold) then again (hot), and
// accumulates the cache misses observed in each pass. It returns the total cold
// and hot miss counts.
func coldHotPairs(ctx context.Context, cfg config, backend storage.ObjectStore, cold, hot *bench.Recorder, do func(context.Context, *engine.Namespace, int) error) (coldMiss, hotMiss uint64, err error) {
	for i := 0; i < cfg.coldTrials; i++ {
		store := cache.New(backend) // fresh, empty DRAM each trial
		ns := engine.Open(store, cfg.namespace)

		before := store.Stats()
		if err := cold.Time(func() error { return do(ctx, ns, i) }); err != nil {
			return 0, 0, err
		}
		mid := store.Stats()
		if err := hot.Time(func() error { return do(ctx, ns, i) }); err != nil {
			return 0, 0, err
		}
		after := store.Stats()

		coldMiss += mid.Sub(before).Misses
		hotMiss += after.Sub(mid).Misses
	}
	return coldMiss, hotMiss, nil
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

// printCacheLine renders one cache hit/miss line with its hot-hit rate.
func printCacheLine(out io.Writer, label string, s cache.CacheStats) {
	fmt.Fprintf(out, "  %s : %6d hits  %4d misses  → %5.1f%% hot\n", label, s.Hits, s.Misses, s.HitRate()*100)
}

// printCacheFull renders a cache line including evictions, for the multi-tenant
// load where capacity pressure makes evictions the interesting number.
func printCacheFull(out io.Writer, label string, s cache.CacheStats) {
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
	fs.IntVar(&cfg.coldTrials, "coldstart-trials", 0, "if >0, run the cold-vs-hot experiment: time each query with an empty cache (cold) then again warm (hot), over this many trials")

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

// envOr returns the value of key, or def if it is unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
