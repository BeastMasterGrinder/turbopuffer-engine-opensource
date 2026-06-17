# CLAUDE.md

`tpuf` — a small, **educational** clone of [turbopuffer](https://turbopuffer.com): a vector +
full-text search engine built on the bet that *object storage is the source of truth, and everything
else exists to hide its latency*. A core engine library plus a CLI (`create | upsert | index | query
| info`) doing **centroid/IVF vector search** and **BM25 full-text search** over **MinIO**
(S3-compatible) in Docker. Written in **readable Go**, faithful to the core architecture, not
production scale — when a choice is between clever and clear, choose clear.

**Status:** the engine + CLI are implemented under `cmd/tpuf/` and `internal/{storage,cache,engine}/`,
following the `docs/06` build order. `go test ./...` passes with no infra; the real-MinIO contract test
runs under `-tags=integration` once `docker compose up` is healthy. The `docs/06` e2e recipe works against
MinIO end to end.

## Project map

- `docs/` — the sourced knowledge base (01–07) + primary papers in `docs/papers/`. Start at `docs/README.md`.
- `go.mod` / `go.sum` — module `github.com/farjad/turbopuffer-clone`, Go 1.26; sole external dep is `aws-sdk-go-v2/service/s3`.
- `cmd/tpuf/` — the CLI (`create | upsert | index | query | info`); backend via `TPUF_BACKEND=s3|memory`.
- `cmd/tpuf-bench/` — latency benchmark (p50..p99.9 per op); `internal/bench/` is its percentile-stats helper.
- `cmd/tpuf-node/` + `deploy/` — optional demo: HTTP query nodes behind an nginx consistent-hash LB (compose `lb` profile); see `deploy/README.md`.
- `internal/{storage,cache,engine}/` — engine packages (layout per `docs/07`); co-located `*_test.go`.

This repo is **documentation-first**: the hard decisions are already made and sourced in `docs/`. Read
the relevant doc before building — don't reinvent them.

<important if="you are implementing engine code, choosing the project layout, or writing tests">

- `docs/06-implementation-blueprint.md` — the code-ready plan: every type, signature, build order, edge cases, and the **5 CAS correctness rules**. Start here when writing engine code.
- `docs/07-project-layout-and-testing.md` — directory layout and testing strategy.
- `docs/01`–`05` — architecture, the SPFresh/SPANN vector index, RaBitQ, BM25, and the honest "what we simplify vs turbopuffer" mapping.
</important>

<important if="you need to run commands to build, test, lint, or run the CLI">

```bash
gofmt -w . && go vet ./...          # format + vet — don't hand-police style
go build ./...                      # after the first AWS SDK import, run `go mod tidy` (deps are // indirect until then)
go test ./...                       # unit tests: fast, no infra (thanks to the in-memory store)
go test ./... -race                 # exercises the CAS / concurrent-upsert paths
docker compose up -d                # MinIO (:9000 API, :9001 console) + auto-created `tpuf` bucket
set -a; source .env.example; set +a # load TPUF_S3_* env
go test ./internal/storage -tags=integration   # real-MinIO 412 contract test
go run ./cmd/tpuf create demo --dim 4 --metric cosine --text-field body   # full e2e recipe in docs/06
```
</important>

<important if="you are adding a dependency, or hand-writing vector math, k-means, RaBitQ, BM25, or the CAS loop">

The **engine** (`internal/`) keeps exactly **one** external dependency: `aws-sdk-go-v2/service/s3`.
Everything conceptually interesting is hand-written stdlib — reaching for a library to implement a core
concept defeats the purpose of the clone. The only exception is the optional benchmark CLI
(`cmd/tpuf-bench`), which uses `charmbracelet/bubbletea`+`bubbles`+`lipgloss` for its live TUI — that's
dev tooling, not the engine, so the engine's single-dependency invariant stands.
</important>

<important if="you are writing or running tests">

Standard-library `testing` only — no testify/mockgen. Co-located, table-driven `*_test.go`. The
in-memory `ObjectStore` (`internal/storage/memory.go`) lets you test the whole engine with no Docker;
MinIO is needed only for the one `//go:build integration` test.
</important>

<important if="you are writing docs or describing how turbopuffer works internally">

The docs cite primary sources and explicitly flag turbopuffer numbers that aren't publicly confirmed.
Don't assert internals as fact unless sourced, and preserve those flags.
</important>

<important if="you are implementing or modifying the storage, WAL, manifest, indexer, or query paths">

A namespace's `manifest.json` is the CAS-coordinated source of truth: a conditional `If-Match` PUT
returning HTTP 412 on a stale write → reload + retry (no Raft/Kafka). `Upsert` is
durable-before-return (write the WAL segment, then CAS the manifest). `index` builds a new epoch under
`index/v{epoch}/` and makes it live with a single atomic manifest CAS. Queries scan the live index
**and** the unindexed WAL tail, so freshly written data is searchable before it's indexed. Full rules:
`docs/06`.
</important>
