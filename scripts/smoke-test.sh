#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"
orders_url=${ORDERS_URL:-http://127.0.0.1:8088}
run_id=$(date -u +%Y%m%dT%H%M%SZ)
temporary_directory=$(mktemp -d)
trap 'rm -rf -- "$temporary_directory"' EXIT

declare -A instances=()
declare -a order_ids=()
last_correlation_id=""

for index in $(seq 1 6); do
  correlation_id="smoke-$run_id-$index"
  headers_file="$temporary_directory/headers-$index.txt"
  body_file="$temporary_directory/body-$index.json"

  curl --fail-with-body --silent --show-error \
    --dump-header "$headers_file" \
    --output "$body_file" \
    --request POST \
    --header 'Content-Type: application/json' \
    --header "X-Correlation-ID: $correlation_id" \
    --data "{\"customerId\":\"smoke-customer-$index\",\"amount\":$index.50,\"currency\":\"brl\"}" \
    "$orders_url/orders"

  instance_id=$(awk 'BEGIN { IGNORECASE=1 } /^x-instance-id:/ { gsub("\\r", "", $2); print $2 }' "$headers_file")
  response_correlation_id=$(awk 'BEGIN { IGNORECASE=1 } /^x-correlation-id:/ { gsub("\\r", "", $2); print $2 }' "$headers_file")
  order_id=$(jq --raw-output '.id' "$body_file")

  test -n "$instance_id"
  test "$response_correlation_id" = "$correlation_id"
  test "$order_id" != "null"
  instances["$instance_id"]=1

  order_ids+=("$order_id")
  last_correlation_id="$correlation_id"
  printf 'Created order %s through %s with correlation %s\n' "$order_id" "$instance_id" "$correlation_id"
done

if (( ${#instances[@]} < 2 )); then
  echo "Expected both API replicas to serve requests." >&2
  exit 1
fi

for order_id in "${order_ids[@]}"; do
  curl --fail --silent --show-error "$orders_url/orders/$order_id" >/dev/null
done

worker_logged=false
for attempt in $(seq 1 15); do
  if (cd "$compose_directory" && docker compose logs --no-color orders-worker 2>&1 | grep --quiet --fixed-strings "$last_correlation_id"); then
    printf 'Worker consumed the final event with correlation %s\n' "$last_correlation_id"
    worker_logged=true
    break
  fi
  sleep 1
done

if [[ "$worker_logged" != true ]]; then
  echo "The worker did not log the final correlation within the timeout." >&2
  exit 1
fi

loki_query="{service_name=\"orders-worker\"} |= \"$last_correlation_id\""
for attempt in $(seq 1 15); do
  if ! loki_response=$(
    cd "$compose_directory" &&
      docker compose exec -T grafana curl --fail --silent --get \
        --data-urlencode "query=$loki_query" \
        --data-urlencode "limit=20" \
        http://loki:3100/loki/api/v1/query_range
  ); then
    sleep 1
    continue
  fi

  if jq --exit-status '.data.result | length > 0' <<<"$loki_response" >/dev/null; then
    printf 'Loki indexed the final worker log with correlation %s\n' "$last_correlation_id"
    printf 'Smoke test passed; observed API instances: %s\n' "${!instances[*]}"
    exit 0
  fi

  sleep 1
done

echo "Loki did not index the final correlated worker log within the timeout." >&2
exit 1
