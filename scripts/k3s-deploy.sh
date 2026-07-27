#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"
overlay_directory="$project_directory/kubernetes/overlays/local"
namespace=orders-lab

for command_name in docker jq kubectl; do
  command -v "$command_name" >/dev/null
done

connection_string=$(
  cd "$compose_directory" &&
    docker compose --profile compose-apps config --format json |
      jq --exit-status --raw-output '.services.migrations.environment.ConnectionStrings__Orders'
)

if [[ -z "$connection_string" || "$connection_string" == "null" ]]; then
  echo "The Orders connection string could not be resolved from Compose." >&2
  exit 1
fi

trap 'unset connection_string' EXIT

kubectl apply --filename "$project_directory/kubernetes/base/namespace.yaml"
printf '%s' "$connection_string" |
  kubectl create secret generic orders-runtime \
    --namespace "$namespace" \
    --from-file=orders-connection-string=/dev/stdin \
    --dry-run=client \
    --output yaml |
  kubectl apply --filename -

kubectl apply --kustomize "$overlay_directory"
kubectl wait \
  --namespace "$namespace" \
  --for=condition=complete \
  job/compose-connectivity-m6 \
  --timeout=120s
kubectl wait \
  --namespace "$namespace" \
  --for=condition=complete \
  job/orders-migrations-m6 \
  --timeout=180s
kubectl rollout status \
  --namespace "$namespace" \
  deployment/orders-api \
  --timeout=180s
kubectl rollout status \
  --namespace "$namespace" \
  deployment/orders-worker \
  --timeout=180s

cd "$compose_directory"
docker compose --profile compose-apps stop nginx orders-api-1 orders-api-2 orders-worker

kubectl get pods --namespace "$namespace" --output wide
kubectl get services,endpointslices --namespace "$namespace"
