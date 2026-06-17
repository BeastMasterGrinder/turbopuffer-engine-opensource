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
