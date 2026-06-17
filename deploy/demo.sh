#!/usr/bin/env bash
# Demonstrates namespace-based consistent-hash routing through the nginx load
# balancer. Prereqs: the "lb" profile is up and the bucket exists:
#
#   docker compose --profile lb up -d --build
#
# Then run this from anywhere:  deploy/demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# Load MinIO creds/endpoint for the host-side tpuf CLI (talks to localhost:9000).
set -a; source .env.example; set +a
export TPUF_BACKEND=s3

LB=${LB:-http://localhost:8088}
namespaces=(acme notion linear vercel stripe retool figma)

tpuf="$(mktemp -d)/tpuf"
go build -o "$tpuf" ./cmd/tpuf

route() { # $1=namespace -> prints the node that served the query
  curl -s -D- -o /dev/null -XPOST "$LB/v1/namespaces/$1/query" \
    -d '{"vector":[0.1,0.2,0.3,0.4],"topK":3}' | tr -d '\r' | awk -F': ' '/^X-Tpuf-Node/{print $2}'
}

echo "== creating + upserting + indexing ${#namespaces[@]} namespaces =="
for ns in "${namespaces[@]}"; do
  "$tpuf" create "$ns" --dim 4 --metric cosine --text-field body >/dev/null 2>&1 || true
  "$tpuf" upsert "$ns" --file examples/sample.json >/dev/null
  "$tpuf" index  "$ns" >/dev/null
  echo "  $ns ready"
done

echo
echo "== routing: hash(namespace) -> node (nginx consistent hash) =="
for ns in "${namespaces[@]}"; do
  printf "  %-8s -> %s\n" "$ns" "$(route "$ns")"
done

echo
echo "== stickiness: 'acme' queried 5x always lands on the same node =="
for i in $(seq 5); do printf "  attempt %d -> %s\n" "$i" "$(route acme)"; done

echo
echo "== per-node cache (the owning node is warm; others never saw that namespace) =="
for n in node0 node1 node2; do
  printf "  %s: %s\n" "$n" "$(docker compose exec -T lb wget -qO- "http://$n:8080/stats" | tr -d '\n' | sed 's/  */ /g')"
done
