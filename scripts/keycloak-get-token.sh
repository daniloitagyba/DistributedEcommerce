#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"

# shellcheck disable=SC1091
source "$compose_directory/.env"

# Same host-published-port quirk as scripts/keycloak-configure-realm.sh -
# the k3s-bridge fixed IP is what's actually reachable from the host.
keycloak_url=${KEYCLOAK_URL:-http://172.30.0.17:8080}

curl --fail --silent --show-error \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_id=orders-api-clients" \
  --data-urlencode "client_secret=$KEYCLOAK_CLIENT_SECRET" \
  "$keycloak_url/realms/orders-lab/protocol/openid-connect/token" |
  jq --raw-output '.access_token'
