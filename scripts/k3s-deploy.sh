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

# Apply any pending Compose infra config (e.g. new Kafka topics added to
# kafka-init's command) before deploying application code that depends on it.
# kafka-init is a one-shot container: it only reruns when its own config
# changes are applied via `up`, never automatically just because app code
# changed, so skipping this step can silently leave new topics uncreated.
(cd "$compose_directory" && docker compose up --detach --wait)

compose_config_json=$(
  cd "$compose_directory" &&
    docker compose --profile compose-apps config --format json
)
connection_string=$(jq --exit-status --raw-output '.services.migrations.environment.ConnectionStrings__Orders' <<<"$compose_config_json")
payments_connection_string=$(jq --exit-status --raw-output '.services["payments-service"].environment.ConnectionStrings__Payments' <<<"$compose_config_json")

if [[ -z "$connection_string" || "$connection_string" == "null" ]]; then
  echo "The Orders connection string could not be resolved from Compose." >&2
  exit 1
fi
if [[ -z "$payments_connection_string" || "$payments_connection_string" == "null" ]]; then
  echo "The Payments connection string could not be resolved from Compose." >&2
  exit 1
fi

secret_directory=$(mktemp -d)
chmod 700 "$secret_directory"
cleanup() {
  unset connection_string payments_connection_string compose_config_json
  rm -rf "$secret_directory"
}
trap cleanup EXIT

"$script_directory/init-payments-db.sh"

printf '%s' "$connection_string" >"$secret_directory/orders-connection-string"
printf '%s' "$payments_connection_string" >"$secret_directory/payments-connection-string"
chmod 600 "$secret_directory"/*

kubectl apply --filename "$project_directory/kubernetes/base/namespace.yaml"
kubectl create secret generic orders-runtime \
  --namespace "$namespace" \
  --from-file=orders-connection-string="$secret_directory/orders-connection-string" \
  --from-file=payments-connection-string="$secret_directory/payments-connection-string" \
  --dry-run=client \
  --output yaml |
  kubectl apply --filename -

# Kubernetes Jobs are immutable once created and never rerun just because the
# image they reference changed underneath the same tag - unlike Deployments,
# `kubectl apply` on an already-Completed Job is a silent no-op. Without this,
# a schema change added after a migration Job's name was last bumped would
# never actually apply; deleting first forces a fresh run every deploy, which
# is safe because EF Core migrations are themselves idempotent.
kubectl delete job orders-migrations-m7 payments-migrations-m12 \
  --namespace "$namespace" --ignore-not-found

kubectl apply --kustomize "$overlay_directory"

# Deployments use a static image tag with imagePullPolicy: IfNotPresent, so
# rebuilding an image under the same tag leaves the Pod template unchanged and
# `kubectl apply` is a no-op - the already-running Pod keeps serving the old
# image. Forcing a rollout restart every deploy ensures rebuilt code is always
# picked up, mirroring the Job-recreation fix above for the same class of bug.
# orders-api is an Argo Rollout (Milestone 15) and is normally reconciled by
# Argo CD rather than this script; "kubectl rollout restart" doesn't support
# its kind, so it's restarted via its own restartAt field instead.
kubectl rollout restart deployment/orders-worker deployment/payments-service \
  --namespace "$namespace"
kubectl patch rollout/orders-api --namespace "$namespace" --type merge \
  --patch "{\"spec\":{\"restartAt\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}"

kubectl wait \
  --namespace "$namespace" \
  --for=condition=complete \
  job/compose-connectivity-m6 \
  --timeout=120s
kubectl wait \
  --namespace "$namespace" \
  --for=condition=complete \
  job/orders-migrations-m7 \
  --timeout=180s
kubectl wait \
  --namespace "$namespace" \
  --for=condition=complete \
  job/payments-migrations-m12 \
  --timeout=180s
# orders-api is an Argo Rollout (Milestone 15), not a Deployment - it has no
# "rollout status" support and is normally reconciled by Argo CD, not this
# script. Poll for full availability rather than assuming a Deployment.
for _ in $(seq 1 90); do
  desired=$(kubectl get rollout orders-api --namespace "$namespace" --output jsonpath='{.spec.replicas}' 2>/dev/null)
  available=$(kubectl get rollout orders-api --namespace "$namespace" --output jsonpath='{.status.availableReplicas}' 2>/dev/null)
  if [[ -n "$desired" && -n "$available" && "$available" -ge "$desired" ]]; then
    break
  fi
  sleep 2
done
kubectl rollout status \
  --namespace "$namespace" \
  deployment/orders-worker \
  --timeout=180s
kubectl rollout status \
  --namespace "$namespace" \
  deployment/payments-service \
  --timeout=180s

cd "$compose_directory"
docker compose --profile compose-apps stop nginx orders-api-1 orders-api-2 orders-worker payments-service

kubectl get pods --namespace "$namespace" --output wide
kubectl get services,endpointslices --namespace "$namespace"
