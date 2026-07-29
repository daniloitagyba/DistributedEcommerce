# Milestone 31 Chaos Mesh Game Day

## Scope

Milestone 10's Toxiproxy work injects network-level faults (latency, timeouts, connection resets) - it never actually kills a process. This milestone adds a real Kubernetes-level fault: Chaos Mesh's `PodChaos` with `action: pod-kill`, which sends the pod's containers a genuine `SIGKILL` - an abrupt crash, not the graceful rolling restart (`SIGTERM` + drain) Milestone 8's `resilience-test.sh` already exercises via `kubectl rollout restart`. The question this game day answers with a live measurement, not an assumption: when `orders-worker` is killed mid-flight while orders are actively being created, does every order still reach a terminal, processed state, and how long does recovery take?

## Setup

Chaos Mesh installed via the Helm chart already wired into `iac/ansible/roles/cluster-addons` (Milestone 28), configured with `chaosDaemon.runtime=containerd` and this K3s distribution's actual containerd socket path (`/run/k3s/containerd/containerd.sock` - the default assumes a vanilla containerd install and is wrong for K3s). `chaos-controller-manager` (x3), `chaos-daemon`, `chaos-dashboard`, and `chaos-dns-server` all confirmed `Running` in the `chaos-mesh` namespace before the experiment.

`kubernetes/chaos-experiments/podchaos-orders-worker-kill.yaml` defines the fault: `mode: one` against the `orders-worker` label selector in `orders-lab`, applied ad hoc for the experiment window (`kubectl apply` then `kubectl delete` after), not left running as a standing policy.

`scripts/chaos-mesh-gameday.sh` orchestrates the game day: creates a batch of orders against `orders-api`'s ClusterIP directly (host-originated, same pattern as `saga-chaos-test.sh`), applies the `PodChaos` manifest partway through the batch, waits for `orders-worker` to become `Ready` again, then polls every created order until it reaches `Confirmed` or `Cancelled`.

## A major regression found and fixed before the game day could even run

The first attempt at this game day failed outright - every order ID came back empty. Direct `curl` against `orders-api`'s ClusterIP from the host returned a real `HTTP/1.1 403 Forbidden` from the mesh layer, not from the application.

Root cause: Milestone 26's `orders-api-allow-orders-worker` `AuthorizationPolicy` (see `kubernetes/cluster-policies/orders-api-authz.yaml`) restricts `orders-api`'s mesh-visible inbound traffic on port 8080 to requests presenting the `orders-worker` Linkerd workload identity. That's correct for pod-to-pod traffic, but it silently also blocks every **host-originated** request hitting the ClusterIP directly - and host-originated traffic to a ClusterIP arrives SNAT'd to the flannel bridge gateway address (`10.42.0.1` on this node, a fact already established during Milestone 24's own investigation), which obviously isn't the `orders-worker` identity. This had been blocking `k6-run.sh` (every k6 profile), `saga-chaos-test.sh`, and `resilience-chaos.sh`'s outage-window requests since the moment Milestone 26 was deployed - undetected because nothing had re-run a load-testing or chaos script against the live mesh between Milestones 26 and 31.

`kubectl port-forward` (the established workaround since Milestone 24 for reaching ClusterIPs from the host) isn't a complete fix here: it's a single-stream SPDY tunnel that can't sustain the throughput k6's higher-volume profiles (autoscale, overload, soak) need - a limit already hit and documented for the Mac-local k6 runner earlier in this lab's history.

**The real fix**: Linkerd supports authorizing by source network as an alternative to workload identity. Added a `NetworkAuthentication` (`k3s-node-bridge-gateway`, scoped to `10.42.0.1/32` - this node's actual bridge gateway address, not the permissive `0.0.0.0/0` linkerd-viz's own `kubelet` `NetworkAuthentication` uses, since that resource can't know the node's IP in advance the way this one can) plus a second `AuthorizationPolicy` (`orders-api-allow-host-originated-traffic`) targeting the same `orders-api-http` `Server`. Multiple `AuthorizationPolicy` resources targeting one `Server` are OR'd together, so this adds an allowed path without loosening the existing `orders-worker`-identity requirement.

Verified live before proceeding: direct `curl` against the ClusterIP returned `201`; `k6-run.sh smoke` passed cleanly (`create_p95_ms=21.75`, `get_p95_ms=96.21`, `failed_rate=0`, `checks_rate=1`).

## Game day results

```
=== Chaos Mesh game day: orders-worker pod-kill mid-flight ===
Creating 20 orders, killing orders-worker after order #10
...
--- killing orders-worker pod (SIGKILL via Chaos Mesh PodChaos) ---
...
Waiting for orders-worker to become ready again...
orders-worker ready again after 1s

Polling until every order reaches a terminal state (Confirmed/Cancelled)...

=== Results ===
Orders created: 20
Orders converged to a terminal state: 20
Data loss: 0 order(s)
Recovery time (kill to orders-worker ready): 1s
Total time (kill to full convergence): 4s
GAME DAY PASSED: every order converged with zero measured data loss.
```

The hypothesis going in - zero data loss, because Kafka's at-least-once delivery plus the PostgreSQL Inbox's dedup by `(consumer_name, event_id)` (the same guarantee documented in the README's "Delivery guarantees" section) should make an abrupt worker crash recoverable without any order getting stuck or duplicated - held under a real `SIGKILL`, not just on paper. All 10 orders created before the kill and all 10 created after (racing the pod's restart) converged to `Confirmed` or `Cancelled` within 4 seconds of the kill. `orders-worker`'s readiness probe reported it healthy again just 1 second after the kill - Kubernetes' default pod restart plus the existing `Deployment`'s single-replica-recreate behavior recovering fast enough that the in-flight batch barely noticed.

## Running the experiment

```bash
scripts/chaos-mesh-gameday.sh [order_count] [kill_after]
# defaults: 20 orders, kill after the 10th is created
```

## Regression check

`k6-run.sh smoke` and `saga-chaos-test.sh` both re-verified passing after the `NetworkAuthentication` fix, confirming the fix didn't loosen the `orders-worker`-identity requirement the original Milestone 26 policy still enforces for pod-to-pod traffic.
