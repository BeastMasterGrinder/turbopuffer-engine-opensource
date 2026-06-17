#!/usr/bin/env bash
# Convenience runner for tpuf-bench.
#
#   benchmarks/run.sh [--backend memory|s3] [--mode single|multi|coldhot|all] [--save] [-- extra flags]
#
# Defaults: --backend memory --mode single  (fast, no infrastructure).
# Any flags after the recognized ones are passed straight to tpuf-bench and
# override the per-mode defaults (Go's flag package takes the last value), e.g.
#   benchmarks/run.sh --mode multi --dim 384 --queries 40000
#
# Saved reference runs live in benchmarks/RESULTS.txt.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

usage() {
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
}

backend=memory
mode=single
save=0
passthru=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend) backend="$2"; shift 2 ;;
    --mode)    mode="$2"; shift 2 ;;
    --save)    save=1; shift ;;
    --)        shift; passthru+=("$@"); break ;;
    -h|--help) usage; exit 0 ;;
    *)         passthru+=("$1"); shift ;;
  esac
done

case "$backend" in memory|s3) ;; *) echo "bad --backend: $backend (want memory|s3)" >&2; exit 1 ;; esac
case "$mode" in single|multi|coldhot|all) ;; *) echo "bad --mode: $mode (want single|multi|coldhot|all)" >&2; exit 1 ;; esac

echo "building tpuf-bench..."
BIN="$(mktemp -d)/tpuf-bench"
go build -o "$BIN" ./cmd/tpuf-bench

if [[ "$backend" == "s3" ]]; then
  set -a; source "$ROOT/.env.example"; set +a
  export TPUF_BACKEND=s3
  if ! curl -fsS "${TPUF_S3_ENDPOINT:-http://localhost:9000}/minio/health/live" >/dev/null 2>&1; then
    echo "ERROR: MinIO not reachable at ${TPUF_S3_ENDPOINT:-http://localhost:9000}." >&2
    echo "       Start it first:  docker compose up -d" >&2
    exit 1
  fi
fi

# Per-backend default sizes: memory can go big (pure CPU); s3 is kept smaller so
# wall-clock stays reasonable. These mirror benchmarks/RESULTS.txt.
if [[ "$backend" == "memory" ]]; then
  single=(--dim 128 --docs 2000 --batch 100 --queries 500 --warmup 50)
  multi=(--namespaces 20 --concurrency 16 --dim 256 --docs 2000 --batch 200 --queries 20000 --cache-objects 200)
  coldhot=(--coldstart-trials 300 --dim 128 --docs 2000 --batch 200)
else
  single=(--dim 64 --docs 500 --batch 50 --queries 3000 --warmup 100)
  multi=(--namespaces 12 --concurrency 12 --dim 256 --docs 1000 --batch 200 --queries 6000 --cache-objects 80)
  coldhot=(--coldstart-trials 300 --dim 128 --docs 3000 --batch 300)
fi

run() { # $1=label, rest=args; passthru overrides the defaults
  local label="$1"; shift
  echo
  echo "############################################################"
  echo "# ${label}  (backend=${backend})"
  echo "############################################################"
  "$BIN" --backend "$backend" "$@" ${passthru[@]+"${passthru[@]}"}
}

do_mode() {
  case "$mode" in
    single)  run "single-tenant"             "${single[@]}" ;;
    multi)   run "multi-tenant (concurrent)" "${multi[@]}" ;;
    coldhot) run "cold-vs-hot"               "${coldhot[@]}" ;;
    all)
      run "single-tenant"             "${single[@]}"
      run "multi-tenant (concurrent)" "${multi[@]}"
      run "cold-vs-hot"               "${coldhot[@]}"
      ;;
  esac
}

if [[ "$save" == "1" ]]; then
  out="$ROOT/benchmarks/run-${backend}-${mode}.txt"
  do_mode | tee "$out"
  echo
  echo "saved to ${out#"$ROOT"/}"
else
  do_mode
fi
