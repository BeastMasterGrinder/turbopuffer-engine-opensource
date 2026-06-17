# 07 — Project layout & how we unit-test it (the Go-idiomatic way)

This doc pins down the **directory layout** and the **testing strategy**, grounded in the official Go
guidance (`go.dev/doc/modules/layout` and the `go.dev/wiki/TableDrivenTests`), not blog opinion.

## Official Go layout guidance (what go.dev actually says)

Go's official module-layout doc gives three shapes; you pick the smallest that fits:

1. **Flat** — everything in one package at the root. For tiny libraries.
   ```
   modname/  go.mod  modname.go  modname_test.go
   ```
2. **Single command + supporting packages** — `package main` at root (or in `cmd/`), private helpers
   under `internal/`:
   ```
   modname/
     go.mod
     internal/auth/auth.go  internal/auth/auth_test.go
     cmd/tpuf/main.go
   ```
3. **Command *and* library (mixed repo)** — importable packages at the root / named dirs, commands under
   `cmd/`, private shared code under `internal/`.

> On `internal/`, the official doc is explicit: *"this prevents other modules from depending on packages
> we don't necessarily want to expose… we're free to refactor its API and generally move things around
> without breaking external users."*

### Our choice: **single command + supporting packages** (shape #2)

`tpuf` is fundamentally **a CLI built on an engine**, and this is an *educational clone* with **no API
stability promise** — so `internal/` is exactly right: it keeps us free to refactor, and the official doc
shows this precise shape for "one command + helpers." Learners can still read every line; `internal/`
only blocks *external module imports*, which is irrelevant for studying the code.

```
turbo-puffer-clone/
├── go.mod  go.sum
├── README.md  docker-compose.yml  .env.example
├── examples/sample.json
├── docs/                         # 01–07 + papers/
├── cmd/tpuf/
│   └── main.go                   # the CLI (package main) — the one consumer
└── internal/
    ├── storage/
    │   ├── storage.go            # ObjectStore interface + sentinel errors
    │   ├── s3.go                 # S3/MinIO impl (the ONLY file importing the AWS SDK)
    │   ├── s3_test.go            # integration test — //go:build integration (real MinIO)
    │   ├── memory.go             # in-memory ObjectStore w/ real CAS — infra-free tests & no-Docker demo
    │   └── memory_test.go        # unit: CAS / PutIfAbsent / 412 semantics
    ├── cache/
    │   ├── cache.go
    │   └── cache_test.go
    └── engine/
        ├── types.go      types_test.go      # Filter.Match (table-driven)
        ├── vector.go     vector_test.go     # distances, KMeans, RaBitQ-lite (table-driven, zero infra)
        ├── bm25.go       bm25_test.go       # Tokenize, BM25 scoring
        ├── manifest.go   manifest_test.go   # CAS loop (over memory store)
        ├── wal.go        wal_test.go        # MaterializeLive: last-writer-wins, deletes
        ├── indexer.go    indexer_test.go    # full build over memory store
        ├── query.go      query_test.go      # both modes + WAL-tail merge + filters
        └── namespace.go  namespace_test.go  # public API, external (package engine_test)
```

## There is NO "separate test module" in Go — and that's a feature

A common cross-language instinct is a top-level `tests/` directory or a separate test project. **Go
doesn't work that way, on purpose:**

- A unit test is a file named `*_test.go` **in the same directory/package** as the code it tests.
- The `go` tool only compiles `_test.go` files during `go test` — they never bloat the built binary.
- Two flavors, both valid, often mixed within one package:
  - **White-box** `package engine` — can test **unexported** helpers (`KMeans`, `ResidualCode`,
    `Tokenize`). Use for the math-heavy internals.
  - **Black-box** `package engine_test` — can only see the **exported** API. Use for `namespace_test.go`
    to prove the public surface is ergonomic and self-sufficient.
- A package's tests are discovered automatically by name; you never register them. `go test ./...` runs
  everything in the module.

So "a unit test module" for us = **a `_test.go` beside every `.go`**, plus the in-memory store below.

## The key enabler: an in-memory `ObjectStore`

The engine only ever talks to the `storage.ObjectStore` **interface** — never the S3 SDK directly. That
single design choice means we can supply a tiny in-memory implementation and unit-test the *entire*
engine (manifest CAS, concurrent-upsert retry, indexer, query merge) **with no Docker, no MinIO, fully
deterministic**:

```go
// internal/storage/memory.go — abridged
type MemStore struct {
    mu   sync.Mutex
    objs map[string]memObj // key -> {body, etag}
    seq  int64             // monotonic etag generator
}
func NewMemory() *MemStore { ... }

func (m *MemStore) PutCAS(ctx context.Context, key string, body []byte, ifMatch string) (string, error) {
    m.mu.Lock(); defer m.mu.Unlock()
    cur, ok := m.objs[key]
    if !ok || cur.etag != ifMatch {       // <-- exact 412 semantics: ETag must match
        return "", ErrPreconditionFailed
    }
    return m.put(key, body), nil
}
func (m *MemStore) PutIfAbsent(ctx context.Context, key string, body []byte) (string, error) {
    m.mu.Lock(); defer m.mu.Unlock()
    if _, ok := m.objs[key]; ok { return "", ErrPreconditionFailed } // already exists
    return m.put(key, body), nil
}
```

Because `MemStore` honors the **same `If-Match`/`If-None-Match` → 412 contract** as MinIO, the CAS retry
loop, the WAL `PutIfAbsent` race, and the indexer epoch-swap are all testable against it. It also doubles
as a **no-infrastructure run mode** for the CLI (e.g. `TPUF_BACKEND=memory`) — handy for demos.

> The real-MinIO path (`s3.go`) gets **one** integration test (`s3_test.go`, behind `//go:build
> integration`) that asserts MinIO really returns 412 — so we verify the contract once at the boundary,
> then trust `MemStore` for the fast feedback loop.

## How we write the tests: table-driven + subtests (the official pattern)

The Go wiki's recommended shape is a **slice (or map) of case structs**, iterated with `t.Run` subtests:

```go
func TestCosineDistance(t *testing.T) {
    tests := []struct {
        name string
        a, b []float32
        want float64
    }{
        {"identical", []float32{1, 0}, []float32{1, 0}, 0},
        {"orthogonal", []float32{1, 0}, []float32{0, 1}, 1},
        {"opposite",   []float32{1, 0}, []float32{-1, 0}, 2},
        {"zero-norm",  []float32{0, 0}, []float32{1, 0}, 1}, // guard: no NaN
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            got := CosineDistance(tt.a, tt.b)
            if math.Abs(got-tt.want) > 1e-6 {
                t.Errorf("CosineDistance(%v,%v) = %v, want %v", tt.a, tt.b, got, tt.want)
            }
        })
    }
}
```

Rules we follow (all from the official wiki):
- **Name every subtest** (`t.Run(tt.name, ...)`) so failures pinpoint the case.
- **`got`/`want` in every failure message** — `t.Errorf("X(%v) = %v, want %v", in, got, want)`.
- **`t.Errorf` (continue), not `t.Fatalf` (abort)** inside table loops — reveals whether a bug is
  universal or input-specific. Use `Fatalf` only when continuing is pointless (e.g. setup failed).
- **`t.Parallel()`** for independent cases (the pure-math tests qualify).
- A **map of cases** when you want name-keyed cases and order-independence.

## What each package tests (concrete checklist)

| Package | Tests (no infra unless noted) |
|---|---|
| `engine` types | `Filter.Match`: eq / and / or / match-all / missing-attr (false) / numeric coercion (5 vs 5.0) |
| `engine` vector | distances (incl. zero-norm), `Normalize`, `KMeans` (K=1, N=1, empty-cluster reseed), `ResidualCode`/`Hamming`/`Agreement` round-trip |
| `engine` bm25 | `Tokenize` edge cases, `BuildBM25` stats (N, avgdl, df), `Score` ranking order, `ScoreDoc` consistency with index stats |
| `engine` manifest | CAS loop **success**; **412 → reload → retry** (use a `MemStore` pre-seeded to force one conflict); `CreateManifest` exists-guard |
| `engine` wal | `MaterializeLive` last-writer-wins across segments; delete tombstones removed; `from/to` windowing |
| `engine` indexer | build over `MemStore` → assert centroids.json/cluster files/bm25.json/docs.json written; `IndexedUpTo` = start snapshot |
| `engine` query | vector + bm25; **query-before-index** (tail only); indexed+tail merge (newer wins); tombstone subtraction; filters; topK |
| `engine` namespace | (`package engine_test`) full Create→Upsert→Query→Index→Query→Info over `MemStore` |
| `storage` memory | CAS/PutIfAbsent 412 semantics, ETag changes on write, List prefix, NotFound |
| `storage` s3 | **integration** (`-tags=integration`): real MinIO returns 412 on If-Match mismatch |

## Commands

```bash
go test ./...                       # all unit tests (fast, no infra — thanks to MemStore)
go test ./internal/engine/... -v    # verbose, see every subtest name
go test ./... -race                 # race detector (validates the CAS/concurrent-upsert paths)
go test ./... -cover                # coverage; add -coverprofile=c.out && go tool cover -html=c.out
go test ./internal/engine -run TestQuery/before_index   # one subtest by name
docker compose up -d && go test ./internal/storage -tags=integration   # real-MinIO contract test
```

## Why this is the right testing shape for this repo

- **Fast feedback, no Docker** for ~95% of the suite (the engine is pure logic over an interface).
- **The CAS/concurrency story is actually tested**, not hand-waved — `MemStore` reproduces 412 and
  `-race` exercises concurrent upserts.
- **The S3 boundary is verified once** against real MinIO, where it matters.
- It's all **standard library `testing`** — no testify, no mock generators — matching the "minimal deps"
  spirit of the whole clone.
