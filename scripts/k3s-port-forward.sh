#!/usr/bin/env bash
set -euo pipefail

namespace=${KUBERNETES_NAMESPACE:-orders-lab}
local_port=${1:-8088}

exec kubectl port-forward \
  --namespace "$namespace" \
  --address 127.0.0.1 \
  service/orders-api \
  "$local_port:80"
