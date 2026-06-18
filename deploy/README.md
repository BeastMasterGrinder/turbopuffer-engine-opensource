# Namespace-routing load balancer (demo)

turbopuffer fronts its query nodes with a load balancer that **consistent-hashes the
namespace to a node** — `hash("notion-acme-corp") → node 47`, the same node every time.
Because all state lives on object storage, **any node can serve any namespace**; pinning a
namespace to one node is purely a *cache-locality* optimization, so that node's DRAM/NVMe
cache stays warm for that tenant. ([turbopuffer architecture](https://turbopuffer.com/docs/architecture))

This directory reproduces that with **nginx + three identical Go query nodes** over the same MinIO.

## Pieces

- **`cmd/tpuf-node`** — a stateless HTTP server over the engine. `POST /v1/namespaces/{ns}/query`,
  `GET /v1/namespaces/{ns}/info`, `GET /stats`. Every response carries `X-Tpuf-Node` naming the node.
  Each node has its **own** in-process DRAM cache.
- **`deploy/nginx.conf`** — `hash $ns consistent;` (ketama) over `node0/1/2`, hashing the namespace
  captured from the URL path. Consistent hashing remaps only ~1/N of namespaces when a node is
  added/removed (vs. plain modulo, which reshuffles nearly everything).
- **`deploy/Dockerfile`** — builds the node as a small static binary.
- **`deploy/Dockerfile.nginx`** — bakes `nginx.conf` into the LB image (so the remap demo can
  reconfigure it via `docker exec`, instead of relying on single-file bind-mount propagation).
- **docker-compose `lb` profile** — `node0/1/2` (+ `node3` for the remap demo) + `lb` (nginx),
  reusing the project's MinIO.

## Run it

```bash
docker compose --profile lb up -d --build     # nodes + nginx (+ MinIO if not already up)
deploy/demo.sh                                  # create namespaces, then show routing
```

The LB listens on **host port 8088** (→ nginx 8080). Query through it directly:

```bash
curl -s -D- -XPOST localhost:8088/v1/namespaces/acme/query \
  -d '{"vector":[0.1,0.2,0.3,0.4],"topK":3}'        # see the X-Tpuf-Node response header
```

Inspect a node's cache (nodes aren't published on the host; reach them via the LB container):

```bash
docker compose exec lb wget -qO- http://node1:8080/stats
```

## What the demo shows

```
== routing: hash(namespace) -> node ==
  acme   -> node1     notion -> node0     linear -> node2   ...
== stickiness: 'acme' queried 5x always lands on the same node ==
  attempt 1..5 -> node1
== per-node cache ==
  node1: hits=15 misses=6  (0.71 hot)   # acme's repeated queries warmed THIS node
  node0: hits=0  misses=12              # its namespaces were each queried once -> all cold
  node2: hits=0  misses=3
```

The cache-hit asymmetry is the whole point: because a namespace always routes to the same node,
its repeated queries build a warm cache *there* instead of scattering across nodes. With only a
handful of namespaces the distribution looks lumpy (consistent hashing balances *statistically*,
at scale — turbopuffer runs 200M+ namespaces).

## Adding a node — why consistent hashing matters

`deploy/remap-demo.sh` routes 50 namespaces across 3 nodes, then adds `node3` to the nginx
upstream (written into the container via `docker exec` + `nginx -s reload`) and re-routes them:

```
phase 1 (3 nodes): node0=22  node1=15  node2=13
phase 2 (4 nodes): node0=17  node1=12  node2=11  node3=10
  10/50 namespaces remapped; 40/50 stayed on their node.
  10/10 of the moved ones landed on the new node3.
```

The guarantee: adding a node only *steals* its ~1/N share for the new node — it never reshuffles
assignments among the existing nodes. Plain `hash % N` would have moved ~37/50 (almost everything),
cold-flushing every node's cache. Consistent hashing moves only ~1/N, so most caches stay warm
through a scaling event. (Run it again to watch the same handful move; the assignment is stable.)

## Teardown

```bash
docker compose --profile lb down          # stop nodes + nginx (MinIO stays up)
docker compose --profile lb down -v       # also drop the MinIO data volume
```

---

# Async indexing: broker + indexers + `queue.json` (demo)

turbopuffer keeps indexing **off the query/write path**: writers commit to the WAL and return,
and a separate fleet of **indexer nodes** asynchronously folds that WAL into search indexes. The
two fleets are decoupled by an indexing job queue that is itself **a single JSON object on object
storage** (`queue.json`), coordinated by compare-and-swap and fronted by a **stateless broker**
that group-commits all queue mutations. ([object-storage queue blog](https://turbopuffer.com/blog/object-storage-queue);
design write-up: `docs/extensions/broker-indexer-queue.md`.)

Our clone normally indexes **inline** (`tpuf index <ns>`). This profile adds the async scheduling
*around* the unchanged engine — `tpuf index` still works exactly as before.

> **Flagged as our design:** turbopuffer publishes the *filename* `queue.json`, the CAS retry loop,
> group commit, heartbeats, FIFO, and at-least-once — but **not** the field layout inside the file
> or the heartbeat timeout. The `Job` schema (`internal/engine/queue.go`) and the 30 s default
> timeout are the clone's choices, not quotes.

## Pieces

- **`cmd/tpuf-broker`** — the stateless single-writer to `queue.json`. `POST /v1/enqueue`
  group-commits reindex notifications (one CAS write per batch); `GET /v1/queue` shows the jobs. It
  acks a request only after the group commit has landed — durability is on the WAL, the queue is
  purely a notification.
- **`cmd/tpuf-indexer`** — the daemon loop: `ClaimNextJob` (CAS ○→◐) → heartbeat while building →
  the engine's unchanged `BuildIndex` (one manifest CAS publish) → `CompleteJob` (CAS ◐→removed).
- **`internal/engine/queue.go`** — `queue.json` and its CAS helpers, the **exact** `If-Match`/412
  shape as the manifest (`SaveQueueCAS` mirrors `SaveManifestCAS`; never cached, fresh read each
  loop — correctness rule 2).
- **docker-compose `indexer` profile** — one `broker` + two identical `indexer0/1` over the shared
  MinIO, so a job is coordinated purely through `queue.json` CAS.

## Run it

```bash
docker compose --profile indexer up -d --build   # broker + 2 indexers (+ MinIO if not already up)
deploy/queue-demo.sh                              # upsert (no inline index), enqueue, watch the epoch advance
```

## What the demo shows

```
== create + upsert (NO inline index) ==
  async-demo: upserted; indexEpoch=0 (0 = unindexed, served by WAL-tail scan)
== notify the broker (group-committed to queue.json) ==
== watch the indexers claim + drain the job, epoch advancing on its own ==
  t= 1s  indexEpoch=0  queue={"count":1,"jobs":[{"namespace":"async-demo","state":"in_progress",...}]}
  t= 2s  indexEpoch=1  queue={"count":0,"jobs":[]}
  -> epoch advanced to 1 via the async indexer (no inline 'tpuf index' was run)
== which indexer did the work? ==
  indexer0: claimed "async-demo" (requestedUpTo=1)
  indexer0: indexed "async-demo" in 12ms, epoch advanced (no inline index call)
```

The point: the epoch advances with **no `tpuf index`** call, exactly **one** indexer claims the
job (the other's CAS loses and it polls on), and queries stay correct throughout — unindexed data
is searched via the WAL tail until the indexer catches up. The **primary correctness proof** is the
race test over the in-memory store (`go test ./internal/engine -race -run Queue`); this compose
profile is the live demo of the same mechanism.

## Teardown

```bash
docker compose --profile indexer down     # stop broker + indexers (MinIO stays up)
```

> Like the LB above, this is a deliberate **non-goal of the core clone** (the engine is a library +
> CLI). It lives here as a separate, optional demo of the indexing tier described in `docs/01`,
> `docs/05`, and `docs/extensions/broker-indexer-queue.md`.
