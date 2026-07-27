#!/usr/bin/env bash
set -euo pipefail

script_directory=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_directory=$(cd -- "$script_directory/.." && pwd)
compose_directory="$project_directory/compose"

for command_name in docker; do
  command -v "$command_name" >/dev/null
done

cd "$compose_directory"
docker compose --profile compose-apps build migrations orders-worker

docker image inspect \
  local-distributed-lab/orders-api:milestone-7 \
  local-distributed-lab/orders-worker:milestone-7 >/dev/null

printf 'Importing milestone 7 images into the K3s containerd image store\n'
docker save \
  local-distributed-lab/orders-api:milestone-7 \
  local-distributed-lab/orders-worker:milestone-7 |
  docker run --rm --interactive \
    --network none \
    --entrypoint /bin/ctr \
    --volume /run/k3s/containerd/containerd.sock:/run/k3s/containerd/containerd.sock \
    rancher/k3s:v1.36.2-k3s1 \
    --address /run/k3s/containerd/containerd.sock \
    --namespace k8s.io images import -

docker run --rm \
  --network none \
  --entrypoint /bin/ctr \
  --volume /run/k3s/containerd/containerd.sock:/run/k3s/containerd/containerd.sock \
  rancher/k3s:v1.36.2-k3s1 \
  --address /run/k3s/containerd/containerd.sock \
  --namespace k8s.io images list --quiet |
  grep --extended-regexp 'local-distributed-lab/orders-(api|worker):milestone-7'
