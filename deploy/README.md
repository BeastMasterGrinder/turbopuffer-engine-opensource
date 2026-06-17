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

> This is a deliberate **non-goal of the core clone** (the engine is a library + CLI). It lives
> here as a separate, optional demo of the routing tier described in `docs/01` and `docs/05`.
