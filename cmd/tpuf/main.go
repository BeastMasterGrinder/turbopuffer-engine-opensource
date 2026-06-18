// Command tpuf is the CLI for the tpuf engine: a small, educational clone of
// turbopuffer doing centroid/IVF vector search and BM25 full-text search over
// object storage. It is the one consumer of the engine package.
//
// Subcommands:
//
//	tpuf create <ns> --dim N --metric cosine|euclidean [--text-field F]
//	tpuf branch <parent> <child>
//	tpuf upsert <ns> --file docs.json
//	tpuf index  <ns>
//	tpuf query  <ns> (--vector "f,f,..." and/or --bm25 "text") [--top-k K] [--n-probe P] [--filter '<json>']
//	tpuf info   <ns>
//
// branch makes <child> a copy-on-write fork of <parent>: it shares the parent's
// immutable WAL and index objects by reference (zero bytes copied, O(1)) and
// diverges only as new writes land on either side
// (docs/extensions/branches-copy-on-write.md).
//
// Passing both --vector and --bm25 runs a hybrid query: the vector and BM25
// rankings are fused with Reciprocal Rank Fusion (docs/extensions/hybrid-fusion.md).
//
// The backend is chosen with TPUF_BACKEND (default "s3"):
//
//	s3      — MinIO/S3, configured via TPUF_S3_ENDPOINT, TPUF_S3_ACCESS_KEY,
//	          TPUF_S3_SECRET_KEY, TPUF_BUCKET
//	memory  — in-process store; data does NOT persist across processes, so it is
//	          only useful for a single-invocation demo (every run starts empty)
//
// Setting TPUF_NVME_DIR enables the optional NVMe ring-buffer cache tier (a
// fixed-size FIFO of cached index objects on local disk; TPUF_NVME_SLOTS sets its
// object capacity, default 1024). It is off unless TPUF_NVME_DIR is set, so the
// default behavior is the plain DRAM-over-object-storage cache.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/farjad/turbopuffer-clone/internal/cache"
	"github.com/farjad/turbopuffer-clone/internal/engine"
	"github.com/farjad/turbopuffer-clone/internal/storage"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "tpuf: %v\n", err)
		os.Exit(1)
	}
}

// run dispatches to the subcommand named by args[0] and returns any error so
// main can report it and set the exit code. It is split out from main to keep
// the os.Exit at a single, testable boundary.
func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		usage(os.Stderr)
		return errors.New("a subcommand is required")
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "create":
		return runCreate(ctx, rest)
	case "branch":
		return runBranch(ctx, rest)
	case "upsert":
		return runUpsert(ctx, rest)
	case "index":
		return runIndex(ctx, rest)
	case "query":
		return runQuery(ctx, rest)
	case "info":
		return runInfo(ctx, rest)
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown subcommand %q", cmd)
	}
}

// runCreate handles: create <ns> --dim N --metric M [--text-field F].
func runCreate(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	dim := fs.Int("dim", 0, "vector dimension (required, > 0)")
	metric := fs.String("metric", "cosine", `distance metric: "cosine" or "euclidean"`)
	textField := fs.String("text-field", "", "attribute name to index for BM25 (empty = no full-text)")
	name, err := parseNamespaceFlags(fs, args, "create <ns> --dim N --metric cosine|euclidean [--text-field F]")
	if err != nil {
		return err
	}
	if *dim <= 0 {
		return errors.New("--dim must be a positive integer")
	}
	if *metric != "cosine" && *metric != "euclidean" {
		return fmt.Errorf("--metric must be \"cosine\" or \"euclidean\", got %q", *metric)
	}

	ns, err := openNamespace(ctx, name)
	if err != nil {
		return err
	}
	if err := ns.Create(ctx, engine.NamespaceConfig{
		Dimension: *dim,
		Metric:    *metric,
		TextField: *textField,
	}); err != nil {
		return err
	}
	fmt.Printf("created namespace %q (dim=%d metric=%s text-field=%q)\n", name, *dim, *metric, *textField)
	return nil
}

// runBranch handles: branch <parent> <child>. It creates a copy-on-write fork of
// <parent> named <child> at the parent's current head — a single manifest PUT,
// zero data objects copied (docs/extensions/branches-copy-on-write.md). Both
// names are positional; there are no flags, so the parsing is intentionally
// simpler than the flag-bearing subcommands.
func runBranch(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return errors.New("usage: tpuf branch <parent> <child>")
	}
	parent, child := args[0], args[1]
	if strings.HasPrefix(parent, "-") || strings.HasPrefix(child, "-") {
		return errors.New("usage: tpuf branch <parent> <child> (names must not start with '-')")
	}

	ns, err := openNamespace(ctx, parent)
	if err != nil {
		return err
	}
	if err := ns.Branch(ctx, child); err != nil {
		return err
	}
	fmt.Printf("branched %q from %q (copy-on-write; shares parent objects, diverges on write)\n", child, parent)
	return nil
}

// runUpsert handles: upsert <ns> --file docs.json. The file is a JSON array of
// Documents ({"id","vector","attributes","deleted"}).
func runUpsert(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("upsert", flag.ContinueOnError)
	file := fs.String("file", "", "path to a JSON array of documents (required)")
	name, err := parseNamespaceFlags(fs, args, "upsert <ns> --file docs.json")
	if err != nil {
		return err
	}
	if *file == "" {
		return errors.New("--file is required")
	}

	data, err := os.ReadFile(*file)
	if err != nil {
		return fmt.Errorf("reading documents file: %w", err)
	}
	var docs []engine.Document
	if err := json.Unmarshal(data, &docs); err != nil {
		return fmt.Errorf("parsing documents file %q: %w", *file, err)
	}

	ns, err := openNamespace(ctx, name)
	if err != nil {
		return err
	}
	if err := ns.Upsert(ctx, docs); err != nil {
		return err
	}
	fmt.Printf("upserted %d document(s) into %q\n", len(docs), name)
	return nil
}

// runIndex handles: index <ns>. It folds the durable WAL into a fresh epoch.
func runIndex(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("index", flag.ContinueOnError)
	name, err := parseNamespaceFlags(fs, args, "index <ns>")
	if err != nil {
		return err
	}

	ns, err := openNamespace(ctx, name)
	if err != nil {
		return err
	}
	if err := ns.Index(ctx); err != nil {
		return err
	}
	fmt.Printf("indexed namespace %q\n", name)
	return nil
}

// runQuery handles: query <ns> (--vector "f,f,..." | --bm25 "text") with
// optional --top-k, --n-probe, and --filter. Set --vector for a vector query,
// --bm25 for a BM25 query, or BOTH for a hybrid query whose two rankings are
// fused with Reciprocal Rank Fusion (docs/extensions/hybrid-fusion.md).
func runQuery(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	vector := fs.String("vector", "", `comma-separated query vector, e.g. "0.1,0.2,0.3"`)
	text := fs.String("bm25", "", "BM25 query text")
	topK := fs.Int("top-k", 10, "number of results to return")
	nProbe := fs.Int("n-probe", 3, "number of IVF clusters to probe (vector mode)")
	filterJSON := fs.String("filter", "", `attribute filter as JSON, e.g. '{"op":"eq","field":"lang","value":"en"}'`)
	name, err := parseNamespaceFlags(fs, args, `query <ns> (--vector "f,f,..." and/or --bm25 "text") [--top-k K] [--n-probe P] [--filter '<json>']`)
	if err != nil {
		return err
	}

	hasVector := *vector != ""
	hasText := *text != ""
	if !hasVector && !hasText {
		return errors.New("specify --vector, --bm25, or both (both ⇒ hybrid RRF fusion)")
	}

	var rankBy engine.RankBy
	if hasVector {
		vec, err := parseVector(*vector)
		if err != nil {
			return err
		}
		rankBy.Vector = vec
	}
	if hasText {
		rankBy.Text = *text
	}

	var filter engine.Filter
	if *filterJSON != "" {
		if err := json.Unmarshal([]byte(*filterJSON), &filter); err != nil {
			return fmt.Errorf("parsing --filter: %w", err)
		}
	}

	ns, err := openNamespace(ctx, name)
	if err != nil {
		return err
	}
	results, err := ns.Query(ctx, engine.QueryParams{
		RankBy: rankBy,
		Filter: filter,
		TopK:   *topK,
		NProbe: *nProbe,
	})
	if err != nil {
		return err
	}
	return printResults(results)
}

// runInfo handles: info <ns>. It prints the namespace manifest as JSON.
func runInfo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	name, err := parseNamespaceFlags(fs, args, "info <ns>")
	if err != nil {
		return err
	}

	ns, err := openNamespace(ctx, name)
	if err != nil {
		return err
	}
	m, err := ns.Info(ctx)
	if err != nil {
		return err
	}
	return printJSON(m)
}

// parseNamespaceFlags parses fs against args, where the namespace name is the
// single positional argument and all flags follow it (Go's flag package stops
// at the first non-flag token, so we pull the leading positional out first).
// usageLine is shown on a parse error. It returns the namespace name.
func parseNamespaceFlags(fs *flag.FlagSet, args []string, usageLine string) (string, error) {
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: tpuf %s\n", usageLine)
		fs.PrintDefaults()
	}
	if len(args) == 0 {
		fs.Usage()
		return "", errors.New("a namespace name is required")
	}

	name := args[0]
	if strings.HasPrefix(name, "-") {
		fs.Usage()
		return "", errors.New("the namespace name must come before any flags")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return "", err
	}
	if fs.NArg() > 0 {
		return "", fmt.Errorf("unexpected extra argument %q", fs.Arg(0))
	}
	return name, nil
}

// parseVector parses a comma-separated list of float32s, e.g. "0.1,0.2,0.3".
func parseVector(s string) ([]float32, error) {
	parts := strings.Split(s, ",")
	vec := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("parsing vector component %q: %w", p, err)
		}
		vec[i] = float32(f)
	}
	return vec, nil
}

// openNamespace builds the configured ObjectStore backend, wraps it in the cache
// tiers, and returns an engine handle to the named namespace. The DRAM tier is
// always on; the NVMe ring-buffer tier (docs/extensions/nvme-ring-buffer-cache.md)
// is optional, enabled by setting TPUF_NVME_DIR (and, optionally,
// TPUF_NVME_SLOTS — the ring's object capacity, default 1024). With it on, a DRAM
// miss is served from the local-disk ring before paying the object-storage
// round-trip, and the warm data survives across CLI invocations.
func openNamespace(ctx context.Context, name string) (*engine.Namespace, error) {
	backend, err := newBackend(ctx)
	if err != nil {
		return nil, err
	}
	store, err := newCache(backend)
	if err != nil {
		return nil, err
	}
	return engine.Open(store, name), nil
}

// newCache wraps backend in the DRAM tier, plus the optional NVMe ring-buffer
// tier when TPUF_NVME_DIR is set. The DRAM cache is unbounded (the right default
// for the CLI's single-namespace use); the NVMe ring is sized by
// TPUF_NVME_SLOTS (objects, default 1024).
func newCache(backend storage.ObjectStore) (*cache.Store, error) {
	dir := os.Getenv("TPUF_NVME_DIR")
	if dir == "" {
		return cache.New(backend), nil
	}
	slots := envIntOr("TPUF_NVME_SLOTS", 1024)
	store, err := cache.NewWithNVMe(backend, 0, dir, slots)
	if err != nil {
		return nil, fmt.Errorf("enabling nvme cache tier at %q: %w", dir, err)
	}
	return store, nil
}

// newBackend selects the ObjectStore implementation from TPUF_BACKEND (default
// "s3"). The "memory" backend is in-process and does not persist across runs.
func newBackend(ctx context.Context) (storage.ObjectStore, error) {
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

// printResults writes query hits as a JSON array. The $dist/$score tags come
// from the QueryResult struct's json tags, so vector and BM25 modes each emit
// only their relevant field.
func printResults(results []engine.QueryResult) error {
	if results == nil {
		results = []engine.QueryResult{}
	}
	return printJSON(results)
}

// printJSON writes v to stdout as indented JSON followed by a newline.
func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encoding output: %w", err)
	}
	return nil
}

// envOr returns the value of the environment variable key, or def if it is
// unset or empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envIntOr returns the integer value of the environment variable key, or def if
// it is unset, empty, or not a valid positive integer.
func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

// usage writes the top-level help text to w.
func usage(w io.Writer) {
	fmt.Fprint(w, `tpuf — an educational vector + full-text search engine over object storage

usage:
  tpuf create <ns> --dim N --metric cosine|euclidean [--text-field F]
  tpuf branch <parent> <child>
  tpuf upsert <ns> --file docs.json
  tpuf index  <ns>
  tpuf query  <ns> (--vector "f,f,..." and/or --bm25 "text") [--top-k K] [--n-probe P] [--filter '<json>']
  tpuf info   <ns>

  (branch is a copy-on-write fork: shares parent objects, diverges on write)

  (passing both --vector and --bm25 runs a hybrid query fused with RRF)

backend (env TPUF_BACKEND, default "s3"):
  s3      MinIO/S3 via TPUF_S3_ENDPOINT, TPUF_S3_ACCESS_KEY, TPUF_S3_SECRET_KEY, TPUF_BUCKET
  memory  in-process store; does NOT persist across processes (single-run demos only)
`)
}
