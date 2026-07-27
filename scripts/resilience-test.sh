#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"
namespace=${KUBERNETES_NAMESPACE:-orders-lab}
results_root=${K6_RESULTS_DIRECTORY:-"$project_directory/artifacts/k6"}
run_id=$(date -u +%Y%m%dT%H%M%SZ)
test_directory="$results_root/$run_id-resilience"
load_log="$test_directory/load.log"
load_pid=""

for command_name in curl docker jq kubectl; do
  command -v "$command_name" >/dev/null
done

mkdir -p "$test_directory"

query_order_count() {
  (
    cd "$compose_directory"
    docker compose exec -T postgres \
      psql \
      --username orders \
      --dbname orders \
      --tuples-only \
      --no-align \
      --command 'SELECT count(*) FROM orders;'
  )
}

cleanup() {
  if [[ -n "$load_pid" ]] && kill -0 "$load_pid" 2>/dev/null; then
    kill "$load_pid" 2>/dev/null || true
    wait "$load_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

orders_before=$(query_order_count)
worker_log_start_nanoseconds="$(date +%s)000000000"
api_revision_before=$(
  kubectl get deployment/orders-api \
    --namespace "$namespace" \
    --output jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}'
)
worker_revision_before=$(
  kubectl get deployment/orders-worker \
    --namespace "$namespace" \
    --output jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}'
)

(
  set -o pipefail
  "$script_directory/k6-run.sh" resilience 2>&1 | tee "$load_log"
) &
load_pid=$!

traffic_started=false
for attempt in $(seq 1 30); do
  orders_now=$(query_order_count)
  if (( orders_now >= orders_before + 5 )); then
    traffic_started=true
    break
  fi
  if ! kill -0 "$load_pid" 2>/dev/null; then
    break
  fi
  sleep 1
done

if [[ "$traffic_started" != true ]]; then
  echo "The resilience workload did not start generating orders." >&2
  exit 1
fi

printf 'Restarting Orders API while traffic is active\n'
kubectl rollout restart deployment/orders-api --namespace "$namespace"
kubectl rollout status deployment/orders-api --namespace "$namespace" --timeout=180s

printf 'Restarting Orders Worker while traffic is active\n'
kubectl rollout restart deployment/orders-worker --namespace "$namespace"
kubectl rollout status deployment/orders-worker --namespace "$namespace" --timeout=180s

set +e
wait "$load_pid"
load_exit_code=$?
set -e
load_pid=""

if (( load_exit_code != 0 )); then
  echo "The resilience workload failed." >&2
  exit "$load_exit_code"
fi

api_revision_after=$(
  kubectl get deployment/orders-api \
    --namespace "$namespace" \
    --output jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}'
)
worker_revision_after=$(
  kubectl get deployment/orders-worker \
    --namespace "$namespace" \
    --output jsonpath='{.metadata.annotations.deployment\.kubernetes\.io/revision}'
)

if (( api_revision_after <= api_revision_before )); then
  echo "Orders API revision did not advance." >&2
  exit 1
fi
if (( worker_revision_after <= worker_revision_before )); then
  echo "Orders Worker revision did not advance." >&2
  exit 1
fi

loki_query='{service_name="orders-worker"} |= "Orders worker is stopping gracefully"'
loki_response=$(
  cd "$compose_directory"
  docker compose exec -T grafana curl --fail --silent --get \
    --data-urlencode "query=$loki_query" \
    --data-urlencode "start=$worker_log_start_nanoseconds" \
    --data-urlencode "limit=20" \
    http://loki:3100/loki/api/v1/query_range
)

if ! jq --exit-status '.data.result | length > 0' <<<"$loki_response" >/dev/null; then
  echo "Loki did not receive the Worker graceful-shutdown log." >&2
  exit 1
fi

printf 'RESILIENCE_RESULT api_revision=%s->%s worker_revision=%s->%s graceful_shutdown=true\n' \
  "$api_revision_before" \
  "$api_revision_after" \
  "$worker_revision_before" \
  "$worker_revision_after" |
  tee "$test_directory/result.txt"
