#!/usr/bin/env bash
# Shows the consistent-hashing guarantee: ADDING a node remaps only ~1/N of
# namespaces (the rest stay on their current node), unlike plain modulo hashing
# which would reshuffle almost everything.
#
# It probes routing for many namespace NAMES (no data needed — the node sets the
# X-Tpuf-Node header before it even looks the namespace up), first with 3 nodes,
# then adds node3 to the nginx upstream and reloads, then compares.
#
# The reconfigure is done by writing the config INTO the lb container via
# `docker exec` and reloading (the config is baked into the image, not bind
# mounted), so it does not depend on bind-mount propagation.
#
# Prereqs: docker compose --profile lb up -d --build   (node0..node3 + lb running)
set -euo pipefail
cd "$(dirname "$0")/.."

LB=${LB:-http://localhost:8088}

# write_config <file> : load <file> as the lb's nginx.conf and reload
write_config() { docker compose exec -T lb sh -c 'cat > /etc/nginx/nginx.conf && nginx -t && nginx -s reload' >/dev/null 2>&1; }

# Always restore the committed 3-node config on exit.
trap 'write_config < deploy/nginx.conf || true' EXIT

route() { # $1=namespace -> the node nginx routed it to
  curl -s -D- -o /dev/null "$LB/v1/namespaces/$1/info" \
    | tr -d '\r' | awk -F': ' 'tolower($1)=="x-tpuf-node"{print $2}'
}
dist() { printf '%s\n' "$@" | sort | uniq -c | awk '{printf "%s=%s  ", $2, $1}'; echo; }

KEYS=(); for i in $(seq -w 0 49); do KEYS+=("ns-$i"); done
total=${#KEYS[@]}

# Start from a known 3-node config.
write_config < deploy/nginx.conf

echo "== phase 1: route $total namespaces across 3 nodes =="
declare -A before
for k in "${KEYS[@]}"; do before[$k]=$(route "$k"); done
printf '   distribution: '; dist "${before[@]}"

echo "== adding node3 to the nginx upstream and reloading =="
sed '/server node2:8080;/a\        server node3:8080;' deploy/nginx.conf | write_config
if [[ "$(docker compose exec -T lb grep -c 'server node3:8080;' /etc/nginx/nginx.conf | tr -d '\r')" != "1" ]]; then
  echo "ERROR: node3 did not reach the container config." >&2
  exit 1
fi

echo "== phase 2: route the same $total namespaces across 4 nodes =="
declare -A after
for k in "${KEYS[@]}"; do after[$k]=$(route "$k"); done
printf '   distribution: '; dist "${after[@]}"

moved=0; stayed=0; tonode3=0
echo "== who moved =="
for k in "${KEYS[@]}"; do
  if [[ "${before[$k]}" != "${after[$k]}" ]]; then
    moved=$((moved + 1))
    [[ "${after[$k]}" == "node3" ]] && tonode3=$((tonode3 + 1))
    printf '   %-6s %s -> %s\n' "$k" "${before[$k]}" "${after[$k]}"
  else
    stayed=$((stayed + 1))
  fi
done

echo
echo "   $moved/$total namespaces remapped; $stayed/$total stayed on their node."
echo "   $tonode3/$moved of the moved ones landed on the new node3."
echo "   consistent hashing moves ~1/N ≈ $((total / 4))/$total; plain modulo would move almost all $total."
