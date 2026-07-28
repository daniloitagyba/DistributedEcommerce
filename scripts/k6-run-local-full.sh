#!/usr/bin/env bash
set -uo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner="$script_directory/k6-run-local.sh"
base_url=${BASE_URL:-http://127.0.0.1:8088}

# kubectl port-forward is a single-stream debug proxy, not a load-bearing
# proxy: it handles bursts fine but breaks down under the sustained request
# volume that autoscale/overload/soak generate (verified - the exact same
# load succeeds when run on the server directly). Run those three via
# scripts/k6-run.sh on the server instead. Ordered cheapest-first.
profiles=(smoke baseline cache resilience stress chaos saga)

command -v k6 >/dev/null || {
  echo "k6 is not installed locally (brew install k6)." >&2
  exit 1
}

if ! curl --fail --silent --show-error --max-time 5 "$base_url/health/ready" >/dev/null; then
  printf \
    'Orders API is not reachable at %s - is the SSH tunnel / kubectl port-forward running?\n' \
    "$base_url" >&2
  exit 1
fi

# Parallel array to `profiles` (not associative - stays compatible with
# macOS's bundled bash 3.2, which has no `declare -A`).
results=()
overall_status=0
suite_started_at=$(date +%s)

for profile in "${profiles[@]}"; do
  printf '\n==== %s ====\n' "$profile"
  profile_started_at=$(date +%s)
  if BASE_URL="$base_url" "$runner" "$profile"; then
    results+=("PASS")
  else
    results+=("FAIL")
    overall_status=1
  fi
  profile_elapsed=$(( $(date +%s) - profile_started_at ))
  last_index=$(( ${#results[@]} - 1 ))
  printf '==== %s: %s (%ss) ====\n' "$profile" "${results[$last_index]}" "$profile_elapsed"
done

suite_elapsed=$(( $(date +%s) - suite_started_at ))

printf '\n===== Full suite summary (%ss total) =====\n' "$suite_elapsed"
for index in "${!profiles[@]}"; do
  printf '%-12s %s\n' "${profiles[$index]}" "${results[$index]}"
done

exit "$overall_status"
