#!/usr/bin/env bash
# Demonstrates ASYNC indexing: a broker fronting queue.json and two indexer
# daemons that claim jobs and run BuildIndex out of band — turbopuffer's
# compute-compute separation (docs/extensions/broker-indexer-queue.md). The win:
# the index epoch advances WITHOUT any inline `tpuf index` call, a job is claimed
# by exactly ONE indexer, and a crashed indexer's job is reclaimed.
#
# Prereqs: the "indexer" profile is up and the bucket exists:
#
#   docker compose --profile indexer up -d --build
#
# Then run this from anywhere:  deploy/queue-demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Load MinIO creds/endpoint for the host-side tpuf CLI (talks to localhost:9000).
set -a; source .env.example; set +a
export TPUF_BACKEND=s3

BROKER=${BROKER:-http://localhost:8090}
NS=${NS:-async-demo}

tpuf="$(mktemp -d)/tpuf"
go build -o "$tpuf" ./cmd/tpuf

epoch() { "$tpuf" info "$NS" 2>/dev/null | awk -F'[:,]' '/indexEpoch/{gsub(/ /,"",$2); print $2}'; }
queue() { curl -s "$BROKER/v1/queue"; }

echo "== create + upsert (NO inline index — indexing is left to the daemons) =="
"$tpuf" create "$NS" --dim 4 --metric cosine --text-field body >/dev/null 2>&1 || true
"$tpuf" upsert "$NS" --file examples/sample.json >/dev/null
echo "  $NS: upserted; indexEpoch=$(epoch) (0 = unindexed, served by WAL-tail scan)"

echo
echo "== notify the broker that $NS needs reindexing (group-committed to queue.json) =="
curl -s -XPOST "$BROKER/v1/enqueue" -d "{\"namespace\":\"$NS\",\"requestedUpTo\":1}" >/dev/null
echo "  queue right after enqueue:"
queue | sed 's/^/    /'

echo
echo "== watch the indexers claim + drain the job, epoch advancing on its own =="
for i in $(seq 1 20); do
  e="$(epoch)"; q="$(queue | tr -d '\n' | sed 's/  */ /g')"
  printf "  t=%2ds  indexEpoch=%s  queue=%s\n" "$i" "${e:-?}" "$q"
  if [ "${e:-0}" != "0" ] && [ -n "$e" ]; then
    echo "  -> epoch advanced to $e via the async indexer (no inline 'tpuf index' was run)"
    break
  fi
  sleep 1
done

echo
echo "== which indexer did the work? (grep the daemon logs) =="
docker compose logs --no-color indexer0 indexer1 2>/dev/null | grep -E "claimed|indexed" | tail -10 | sed 's/^/  /' || true

echo
echo "== query works through the whole flow (indexed path + WAL tail) =="
"$tpuf" query "$NS" --vector "0.1,0.2,0.3,0.4" --top-k 3
