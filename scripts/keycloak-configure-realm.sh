#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"

# shellcheck disable=SC1091
source "$compose_directory/.env"

# The published host port (127.0.0.1:18081, matching KEYCLOAK_PORT) records
# a correct binding in `docker inspect` but doesn't actually get programmed
# on this host - the same class of quirk already hit with Alertmanager
# (Milestone 16) and Debezium's connector API (Milestone 21). The k3s-bridge
# network's fixed IP is host-routable and reliably reachable instead.
keycloak_url=${KEYCLOAK_URL:-http://172.30.0.17:8080}
realm_name=orders-lab
client_id=orders-api-clients

curl_json() {
  curl --fail --silent --show-error --header "Content-Type: application/json" "$@"
}

admin_token=$(
  curl --fail --silent --show-error \
    --data-urlencode "client_id=admin-cli" \
    --data-urlencode "username=admin" \
    --data-urlencode "password=$KEYCLOAK_ADMIN_PASSWORD" \
    --data-urlencode "grant_type=password" \
    "$keycloak_url/realms/master/protocol/openid-connect/token" |
    jq --raw-output '.access_token'
)
auth_header="Authorization: Bearer $admin_token"

if curl --fail --silent --header "$auth_header" "$keycloak_url/admin/realms/$realm_name" >/dev/null 2>&1; then
  printf 'Realm %s already exists.\n' "$realm_name"
else
  curl_json --header "$auth_header" \
    --data "{\"realm\":\"$realm_name\",\"enabled\":true,\"accessTokenLifespan\":900}" \
    "$keycloak_url/admin/realms"
  printf 'Created realm %s.\n' "$realm_name"
fi

for role_name in "orders:read" "orders:write"; do
  if curl --fail --silent --header "$auth_header" "$keycloak_url/admin/realms/$realm_name/roles/$role_name" >/dev/null 2>&1; then
    printf 'Role %s already exists.\n' "$role_name"
  else
    curl_json --header "$auth_header" \
      --data "{\"name\":\"$role_name\"}" \
      "$keycloak_url/admin/realms/$realm_name/roles"
    printf 'Created role %s.\n' "$role_name"
  fi
done

client_internal_id=$(
  curl --fail --silent --header "$auth_header" \
    "$keycloak_url/admin/realms/$realm_name/clients?clientId=$client_id" |
    jq --raw-output '.[0].id // empty'
)

if [[ -z "$client_internal_id" ]]; then
  # secret is fixed to KEYCLOAK_CLIENT_SECRET (from .env, same file
  # POSTGRES_PASSWORD and KEYCLOAK_ADMIN_PASSWORD already live in) rather
  # than left for Keycloak to generate - every script that needs a token
  # (smoke tests, k6) reads it from the same known source instead of
  # scraping this script's own output.
  curl_json --header "$auth_header" \
    --data "{\"clientId\":\"$client_id\",\"publicClient\":false,\"secret\":\"$KEYCLOAK_CLIENT_SECRET\",\"serviceAccountsEnabled\":true,\"standardFlowEnabled\":false,\"directAccessGrantsEnabled\":false}" \
    "$keycloak_url/admin/realms/$realm_name/clients"
  client_internal_id=$(
    curl --fail --silent --header "$auth_header" \
      "$keycloak_url/admin/realms/$realm_name/clients?clientId=$client_id" |
      jq --raw-output '.[0].id'
  )
  printf 'Created client %s.\n' "$client_id"
else
  printf 'Client %s already exists.\n' "$client_id"
fi

# client_credentials tokens default to "aud": "account" - a hardcoded
# audience mapper is what makes Orders.Api's Audience="orders-api" check
# meaningful, rather than accepting any token this realm ever issues.
existing_mapper_id=$(
  curl --fail --silent --header "$auth_header" \
    "$keycloak_url/admin/realms/$realm_name/clients/$client_internal_id/protocol-mappers/models" |
    jq --raw-output '.[] | select(.name == "orders-api-audience") | .id // empty'
)
if [[ -z "$existing_mapper_id" ]]; then
  curl_json --header "$auth_header" \
    --data '{"name":"orders-api-audience","protocol":"openid-connect","protocolMapper":"oidc-audience-mapper","config":{"included.custom.audience":"orders-api","access.token.claim":"true","id.token.claim":"false"}}' \
    "$keycloak_url/admin/realms/$realm_name/clients/$client_internal_id/protocol-mappers/models"
  printf 'Created orders-api audience mapper.\n'
else
  printf 'Audience mapper already exists.\n'
fi

service_account_user_id=$(
  curl --fail --silent --header "$auth_header" \
    "$keycloak_url/admin/realms/$realm_name/clients/$client_internal_id/service-account-user" |
    jq --raw-output '.id'
)

existing_role_names=$(
  curl --fail --silent --header "$auth_header" \
    "$keycloak_url/admin/realms/$realm_name/users/$service_account_user_id/role-mappings/realm" |
    jq --raw-output '.[].name'
)

roles_to_assign="[]"
for role_name in "orders:read" "orders:write"; do
  if grep --quiet --fixed-strings --line-regexp "$role_name" <<<"$existing_role_names"; then
    continue
  fi
  role_json=$(curl --fail --silent --header "$auth_header" "$keycloak_url/admin/realms/$realm_name/roles/$role_name")
  roles_to_assign=$(jq --argjson role "$role_json" '. + [$role]' <<<"$roles_to_assign")
done

if [[ "$(jq 'length' <<<"$roles_to_assign")" -gt 0 ]]; then
  curl_json --header "$auth_header" \
    --data "$roles_to_assign" \
    "$keycloak_url/admin/realms/$realm_name/users/$service_account_user_id/role-mappings/realm"
  printf 'Assigned roles to %s service account.\n' "$client_id"
else
  printf 'Service account already has both roles.\n'
fi

printf '\nRealm ready. Client ID: %s (secret is KEYCLOAK_CLIENT_SECRET in .env).\n' "$client_id"
