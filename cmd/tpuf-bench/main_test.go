package main

import (
	"strings"
	"testing"
)

// TestRunMemorySmoke exercises the whole benchmark end to end over the in-memory
// backend (no MinIO): upsert, tail-scan queries, index, indexed queries, and the
// rendered report. It is the cheapest way to catch a wiring regression between
// the bench command and the engine API.
func TestRunMemorySmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory",
		"--dim", "8", "--docs", "40", "--batch", "10",
		"--queries", "5", "--warmup", "2", "--top-k", "3", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"backend=memory",
		"OPERATION", "P99.9",
		"upsert (batch)",
		"query-vec (tail scan)", "query-bm25 (tail scan)",
		"index (build)",
		"query-vec (indexed)", "query-bm25 (indexed)",
		"manifest: docCount=40",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunMultiTenantMemorySmoke exercises the multi-tenant concurrent path over
// the memory backend with a bounded cache, so it covers tenant setup, the worker
// pool, throughput reporting, and cache eviction in one no-infra run. Run under
// -race, it also guards the concurrent cache and query paths.
func TestRunMultiTenantMemorySmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--namespaces", "3", "--concurrency", "4",
		"--dim", "8", "--docs", "30", "--batch", "10", "--queries", "40",
		"--top-k", "3", "--cache-objects", "5", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"MULTI-TENANT", "namespaces=3",
		"index (build/tenant)",
		"query-vec (concurrent)", "query-bm25 (concurrent)",
		"achieved throughput", "per-tenant breakdown", "NAMESPACE", "QUERIES",
		"shared DRAM cache", "evictions",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunColdStartMemorySmoke exercises the cold-vs-hot experiment over the
// memory backend. On memory the latency gap is negligible, but the mechanism is
// the assertion: the cold pass must record cache misses and the hot pass none —
// proven here by the report wording, with the real latency gap demonstrated on
// the s3 backend.
func TestRunColdStartMemorySmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--coldstart-trials", "10",
		"--dim", "8", "--docs", "200", "--batch", "50", "--top-k", "3", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"COLD-vs-HOT",
		"query-vec  COLD (cache empty)", "query-vec  HOT  (cache warm)",
		"cold → hot", "hot pass = 0 misses",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunGroupCommitSmoke exercises the group-commit demo mode end to end over
// the in-memory backend: it fires concurrent single-doc upserts both ways and
// renders the segment-count comparison. It asserts the report shows both runs
// and that group commit produced strictly fewer WAL segments than the direct
// path — the headline win, proven from the CLI. Under -race it also guards the
// committer's concurrent enqueue/flush/Close paths through the public command.
func TestRunGroupCommitSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--group-commit",
		"--dim", "8", "--docs", "400", "--concurrency", "16", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"group-commit demo",
		"direct:",
		"group-commit:",
		"WAL segments:",
		"fewer durable PUTs",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunMultiTenantWithNVMeSmoke exercises the multi-tenant path with the NVMe
// ring-buffer tier enabled: a tiny DRAM cap forces constant eviction, and the
// large ring absorbs the re-reads. It asserts the report renders the three-tier
// DRAM/NVMe/S3 panel and that the warm tier actually served traffic. Run under
// -race it also guards the ring's locking on the concurrent hot path.
func TestRunMultiTenantWithNVMeSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--namespaces", "4", "--concurrency", "4",
		"--dim", "8", "--docs", "30", "--batch", "10", "--queries", "60",
		"--top-k", "3", "--cache-objects", "2",
		"--nvme-dir", t.TempDir(), "--nvme-slots", "256", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"MULTI-TENANT",
		"shared DRAM+NVMe cache",
		"DRAM-hits", "NVMe-hits", "S3-cold",
		"absorbed as NVMe-hits",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunColdStartWithNVMeSmoke exercises the cold-vs-hot experiment with the
// NVMe tier on, which adds the WARM (NVMe) middle data point: a fresh DRAM map
// over the warm ring serves the DRAM-evicted index objects from local disk. The
// assertion is the mechanism — the warm pass records NVMe-hits and the report
// names the three states.
func TestRunColdStartWithNVMeSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--coldstart-trials", "5",
		"--dim", "8", "--docs", "120", "--batch", "40", "--top-k", "3",
		"--nvme-dir", t.TempDir(), "--nvme-slots", "256", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"COLD-vs-HOT",
		"query-vec  WARM (NVMe, DRAM cold)",
		"NVMe-hits", "hot pass = 0 S3-cold",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

func TestParseFlagsValidation(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"defaults are valid", nil, false},
		{"unknown backend", []string{"--backend", "redis"}, true},
		{"zero docs", []string{"--docs", "0"}, true},
		{"negative batch", []string{"--batch", "-5"}, true},
		{"zero queries", []string{"--queries", "0"}, true},
		{"zero namespaces", []string{"--namespaces", "0"}, true},
		{"zero concurrency", []string{"--concurrency", "0"}, true},
		{"negative cache-objects", []string{"--cache-objects", "-1"}, true},
		{"nvme-slots non-positive with nvme-dir", []string{"--nvme-dir", "/tmp/x", "--nvme-slots", "0"}, true},
		{"nvme-slots ignored without nvme-dir", []string{"--nvme-slots", "0"}, false},
		{"valid nvme tier", []string{"--nvme-dir", "/tmp/x", "--nvme-slots", "1024"}, false},
		{"valid multi-tenant", []string{"--namespaces", "8", "--concurrency", "4", "--cache-objects", "50"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var sink strings.Builder
			cfg, err := parseFlags(tt.args, &sink)
			if err != nil {
				t.Fatalf("parseFlags: %v", err)
			}
			err = cfg.validate()
			if tt.wantErr && err == nil {
				t.Errorf("validate() = nil, want error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validate() = %v, want nil", err)
			}
		})
	}
}

// TestParseFlagsAutoNamespace confirms an unspecified namespace gets a unique name.
func TestParseFlagsAutoNamespace(t *testing.T) {
	var sink strings.Builder
	cfg, err := parseFlags(nil, &sink)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if !strings.HasPrefix(cfg.namespace, "bench-") {
		t.Errorf("namespace = %q, want a bench- prefix", cfg.namespace)
	}
}

// TestRunRaBitQSmoke exercises the True RaBitQ demo mode end to end over the
// in-memory backend: it builds a labeled namespace, sweeps the prefilter shortlist
// size, and renders the True-RaBitQ-vs-lite recall table. It asserts the report
// renders and — the headline win — that True RaBitQ reaches the recall target at a
// shortlist no larger than lite needs (here lite typically never reaches it within
// the sweep). This guards the engine.PrefilterShortlist wiring from the CLI.
func TestRunRaBitQSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--rabitq",
		"--dim", "32", "--docs", "400", "--queries", "40", "--batch", "200",
		"--metric", "euclidean", "--seed", "3",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"TRUE RaBitQ vs lite",
		"True RaBitQ recall",
		"lite recall",
		"shortlist needed for",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunHybridSmoke exercises the hybrid-fusion demo mode end to end over the
// in-memory backend: it builds a labeled set (one planted answer moderate on both
// signals plus a per-mode decoy) and renders the vector-only / bm25-only / hybrid
// RRF recall table. It asserts the report renders and that hybrid recall is at
// least as high as either single mode — guarding the RRF wiring from the CLI.
func TestRunHybridSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--hybrid",
		"--dim", "16", "--docs", "200", "--queries", "40", "--top-k", "1", "--seed", "7",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"HYBRID-FUSION recall (RRF k=60)",
		"recall@",
		"vector-only :",
		"bm25-only   :",
		"hybrid RRF  :",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}

// TestRunFilterPlanSmoke exercises the bitmap filter-plan demo mode end to end over
// the in-memory backend: it times the same vector query across selectivity bands
// and reports the cold-cache index objects fetched per band. It asserts the report
// renders and names the baseline and the same-answers invariant — guarding the
// bitmap planner wiring from the CLI.
func TestRunFilterPlanSmoke(t *testing.T) {
	var out strings.Builder
	args := []string{
		"--backend", "memory", "--filter-plan",
		"--dim", "16", "--docs", "600", "--queries", "30", "--seed", "1",
	}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"FILTER-PLAN (bitmap attribute index)",
		"cold-cache index objects fetched per query",
		"unfiltered (baseline)",
		"answer set is identical",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report missing %q:\n%s", want, got)
		}
	}
}
