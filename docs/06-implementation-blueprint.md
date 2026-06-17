# 06 — Implementation blueprint (the code-ready plan)

This is the concrete, file-by-file plan to build `tpuf` (Go core engine + CLI on MinIO). It's the
bridge from the conceptual docs (01–05) to actual code: every type, signature, algorithm, edge case,
build order, and verification step. Produced by a planning pass over the approved scope; SDK facts were
re-confirmed against the module cache.

> Module: `github.com/farjad/turbopuffer-clone`, Go 1.26. One external dep:
> `aws-sdk-go-v2/service/s3`. Everything else is hand-written stdlib.

## SDK facts (confirmed — build on these)

- `s3.PutObjectInput` has `IfMatch *string` and `IfNoneMatch *string`; `GetObjectInput` has
  `IfNoneMatch`; `GetObjectOutput`/`PutObjectOutput` return `ETag *string`.
- `s3.Options` has `BaseEndpoint *string` and `UsePathStyle bool` (MinIO needs both).
- **412 detection:** `errors.As` against a local interface `{ HTTPStatusCode() int }` — this matches the
  smithy `*ResponseError` (embedded in `aws/transport/http.ResponseError`) regardless of wrapper layer.
  String fallback: `Contains(err, "PreconditionFailed"|"412")`. Same pattern for 404 → `NoSuchKey`.
- aws deps are `// indirect` until first imported — run `go mod tidy` after writing `storage.go`.

## Five correctness rules that are easy to get wrong (lock these in)

1. **WAL segments use `PutIfAbsent`, not unconditional `Put`.** Two concurrent upserts both reading
   `WALSeq=5` must not clobber `wal/...05.json`. The loser gets 412, reloads (now `WALSeq=6`), rewrites
   at seq 6, then does the manifest CAS. This is what makes concurrent writes correct *and* demonstrates
   the 412 retry path.
2. **Never cache the manifest.** CAS correctness depends on a *fresh* GET each loop iteration. Index
   objects (immutable under their epoch key) are cacheable; the manifest and WAL are read uncached.
3. **`IndexedUpTo` must be the `WALSeq` snapshot taken at index *start*** — not the live `WALSeq` at swap
   time — or writes that arrive during the build are silently dropped from both index and tail.
4. **Atomic index publish = one CAS write.** All `index/v{epoch}/*` objects are written first under a
   fresh epoch prefix (write-once keys, no CAS). The index goes live only when the single
   `manifest.json` CAS flips `IndexEpoch`. Until then, queries serve the old epoch. Old epochs are
   GC-able (not built here).
5. **Last-writer-wins, end to end.** The query applies the WAL tail *after* the indexed path (newer
   vector/text overwrites), and subtracts tombstones (`MaterializeLiveAndDeleted`) so a delete in the
   tail removes an id even if the indexed epoch still has it.

## Type definitions (`internal/engine/types.go`)

```go
type Document struct {
    ID         string         `json:"id"`
    Vector     []float32      `json:"vector,omitempty"`
    Attributes map[string]any `json:"attributes,omitempty"`
    Deleted    bool           `json:"deleted,omitempty"` // tombstone in WAL
}

type WALSegment struct { Seq int64 `json:"seq"`; Ops []Document `json:"ops"` }

type Manifest struct {
    Version     int64  `json:"version"`     // informational; ETag is the real CAS token
    Dimension   int    `json:"dimension"`
    Metric      string `json:"metric"`      // "cosine" | "euclidean"
    TextField   string `json:"textField"`   // "" = no BM25
    WALSeq      int64  `json:"walSeq"`       // next seq; segments [0,WALSeq) exist
    IndexedUpTo int64  `json:"indexedUpTo"`  // [0,IndexedUpTo) folded into index
    IndexEpoch  int64  `json:"indexEpoch"`   // live index/v{epoch}/; 0 = none
    DocCount    int    `json:"docCount"`
}

type NamespaceConfig struct { Dimension int; Metric, TextField string }

type RankBy struct { Vector []float32; Text string } // exactly one set
func (r RankBy) IsVector() bool { return r.Vector != nil }
func (r RankBy) IsText()   bool { return r.Text != "" }

// Tagged-union filter — JSON-serializable, recursive, no interface dispatch.
type Filter struct {
    Op    string   `json:"op"`              // "eq"|"and"|"or"|"" (match-all)
    Field string   `json:"field,omitempty"`
    Value any      `json:"value,omitempty"`
    Sub   []Filter `json:"sub,omitempty"`
}
func (f Filter) Match(attrs map[string]any) bool

type QueryResult struct {
    ID    string  `json:"id"`
    Dist  float64 `json:"$dist,omitempty"`  // vector mode (lower = closer)
    Score float64 `json:"$score,omitempty"` // bm25 mode (higher = better)
    Attributes map[string]any `json:"attributes,omitempty"`
}
```

### On-disk index shapes (in `indexer.go`)

```go
type CentroidsFile struct { Metric string; Dimension, K int; Centroids [][]float32; Sizes []int }
type ClusterFile   struct { Cluster int; Centroid []float32; Members []ClusterEntry }
type ClusterEntry  struct { ID string; Vector []float32; Code []uint64; Attrs map[string]any }
type BM25File      struct { N int; AvgDL float64; DocLen map[string]int; Index map[string][]Posting }
type Posting       struct { ID string; TF int }
type DocsFile      struct { Docs map[string]map[string]any }
```
`ClusterEntry.Attrs` duplicates attributes so vector hits filter+return without a second fetch (mirrors
turbopuffer co-locating payloads with retrieved data); BM25 hits read attrs from `docs.json`.

## The `ObjectStore` interface (`internal/storage/storage.go`)

```go
type ObjectStore interface {
    Get(ctx, key) (body []byte, etag string, err error)        // ErrNotFound on 404
    PutCAS(ctx, key, body, ifMatchETag) (newETag string, err error)  // ErrPreconditionFailed on 412
    PutIfAbsent(ctx, key, body) (newETag string, err error)    // If-None-Match:"*"
    Put(ctx, key, body) (newETag string, err error)            // unconditional (index files)
    List(ctx, prefix) (keys []string, err error)
}
var ErrPreconditionFailed = errors.New("storage: precondition failed (412)")
var ErrNotFound          = errors.New("storage: not found (404)")
```
S3 client: `s3.NewFromConfig(cfg, func(o){ o.BaseEndpoint=&endpoint; o.UsePathStyle=true })` with static
creds + placeholder region. `PutCAS` sets `IfMatch`; `PutIfAbsent` sets `IfNoneMatch:"*"`; both map 412
→ `ErrPreconditionFailed` via the `statusCoder` interface check.

## Engine API (`internal/engine/namespace.go`)

```go
func Open(store *cache.Store, name string) *Namespace
func (n *Namespace) Create(ctx, NamespaceConfig) error             // CreateManifest → PutIfAbsent
func (n *Namespace) Upsert(ctx, docs []Document) error             // WAL PutIfAbsent + manifest CAS
func (n *Namespace) Index(ctx) error                               // BuildIndex
func (n *Namespace) Query(ctx, QueryParams) ([]QueryResult, error)
func (n *Namespace) Info(ctx) (Manifest, error)
```

### CAS manifest save loop (`manifest.go`)
```
for attempt < 10:
    m, etag := LoadManifest()        # fresh GET every iteration
    mutate(&m); m.Version++
    err := PutCAS(manifest, body, etag)
    if ok: return m
    if 412: log "CAS conflict, retrying"; continue
    else: return err
```

### Indexer (`indexer.go`)
```
m := LoadManifest(); walUpTo := m.WALSeq; epoch := m.IndexEpoch+1
live := MaterializeLive(0, walUpTo)             # last-writer-wins, deletes applied
if vectors exist:
    k := max(1, round(sqrt(N)))                 # ChooseK
    centroids, assign := KMeans(points, k, metric, 25)
    per doc: code := ResidualCode(v, centroids[c])  # RaBitQ-lite sign bits
    write centroids.json + cluster-{i}.json under index/v{epoch}/
if textField != "": write bm25.json (BuildBM25(live))
write docs.json
SaveManifestCAS: IndexEpoch=epoch; IndexedUpTo=walUpTo; DocCount=len(live)   # atomic swap
```

### Query (`query.go`) — both modes
```
VECTOR:
  if IndexEpoch>0: load centroids → top nProbe (default 3) clusters →
      RaBitQ-lite agreement prefilter (top M) → exact rerank → candidates
  WAL TAIL [IndexedUpTo, WALSeq): exact-scan every vector doc → overwrite (newer wins)
  subtract deletedInTail → Filter.Match → sort asc by $dist → topK

BM25:
  if IndexEpoch>0 && textField: load bm25.json → Score(terms); attrs from docs.json
  WAL TAIL: score new docs with same global stats (ScoreDoc) → overwrite
  subtract deletedInTail → Filter.Match → sort desc by $score → topK
```
`MaterializeLiveAndDeleted(from,to)` returns both the live map and the set of tombstoned ids so the
query can shadow indexed docs. Query-before-index (`IndexEpoch==0`) serves purely from the tail
`[0, WALSeq)` — the headline demo that unindexed data is searchable.

## Edge cases (each handled explicitly)
Empty namespace (WALSeq 0 → empty, not error) · query-before-index (tail-only) · K=1/tiny N (clamp,
reseed empty centroids) · deletes (tombstone → removed at materialize + subtracted in query) · dimension
mismatch (reject on upsert & query with both numbers) · zero-norm cosine (return dist 1.0, no NaN) ·
duplicate ids across segments (materialize order resolves) · concurrent upsert (WAL PutIfAbsent retry +
manifest CAS retry) · both/neither rank mode (CLI error) · text-only doc in vector scan (skip) ·
`--bm25` with no text field (clear error) · filter on missing attr (false, no panic) · numeric `eq`
coercion (JSON numbers are float64 — coerce both sides).

## Build order (compiles & tests incrementally)
1. `types.go` (pure + `Filter.Match`) → unit-test Match
2. `vector.go` (distances, Normalize, KMeans, RaBitQ-lite) → **highest-value unit tests, no infra**
3. `bm25.go` (Tokenize, BuildBM25, Score, ScoreDoc) → unit tests
4. `storage/storage.go` (interface + errors) + `storage/memory.go` (in-memory store w/ real CAS) →
   unit-test the CAS/412 semantics. **`memory.go` unblocks testing the entire engine without MinIO.**
5. `storage/s3.go` (only SDK importer) → `go mod tidy`; its integration test is `//go:build integration`
6. `cache.go` → 7. `manifest.go` → 8. `wal.go` → 9. `indexer.go` (engine now compiles; all testable over `memory.go`)
10. `query.go` → 11. `namespace.go` → 12. `cmd/tpuf/main.go` → `go build ./...`
13. `docker-compose.yml`, `.env.example`, `examples/sample.json`, `README.md`

Stages 1–4 are fully testable with **zero infrastructure** (pure math + the in-memory store) — get the
logic right before touching MinIO. Full layout + testing strategy: [`07-project-layout-and-testing.md`](./07-project-layout-and-testing.md).

## End-to-end verification
```bash
docker compose up -d                        # MinIO + auto-create bucket `tpuf`
set -a; source .env.example; set +a
go mod tidy && go build ./... && go test ./internal/engine/...
go run ./cmd/tpuf create demo --dim 4 --metric cosine --text-field body
go run ./cmd/tpuf upsert demo --file examples/sample.json
go run ./cmd/tpuf query  demo --vector "0.1,0.2,0.3,0.4" --top-k 3   # BEFORE index → WAL scan
go run ./cmd/tpuf query  demo --bm25 "quick walrus" --top-k 3
go run ./cmd/tpuf index  demo                                        # epoch swap via CAS
go run ./cmd/tpuf query  demo --vector "0.1,0.2,0.3,0.4" --n-probe 3 # indexed path + tail
go run ./cmd/tpuf query  demo --bm25 "walrus" --filter '{"op":"eq","field":"lang","value":"en"}'
go run ./cmd/tpuf info   demo
# open http://localhost:9001 → see demo/manifest.json, demo/wal/..., demo/index/v1/...
```
Step "query before index" returning results with no index present is the proof that unindexed WAL data
is searchable; the MinIO console showing every object is the proof that object storage is the source of
truth.
