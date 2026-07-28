# Milestone 24 Service Mesh (Linkerd) on K3s

## Scope

Install Linkerd on the K3s cluster and mesh the three app-tier workloads - `orders-api`, `orders-worker`, `payments-service` - the last of this session's eight architecture-broadening milestones. The goal: automatic mutual TLS between meshed workloads and free golden metrics (success rate, RPS, latency), without touching application code. Infra-only, installed the same way Milestone 17's sealed-secrets controller was: imperatively via the tool's own CLI/Helm charts, not through Argo CD (which in this repo manages application manifests, not cluster infrastructure).

## Design

- **Control plane + CRDs**: installed via `linkerd install --crds` / `linkerd install`, both piped to `kubectl apply`.
- **`linkerd viz`**: the observability extension - its own Prometheus, a metrics API, and `tap` for live request inspection. Kept separate from this project's own Milestone 16 Prometheus/Grafana stack deliberately; viz's Prometheus scrapes proxy-emitted golden metrics specifically, a different concern from the app-level SLO metrics the existing stack tracks.
- **CNI plugin, not the default init-container**: covered below under "what didn't work" - `orders-lab`'s namespace enforces the `restricted` Pod Security Standard, which the default per-pod `linkerd-init` container's `NET_ADMIN`/`NET_RAW` capabilities violate. The CNI plugin moves iptables setup into a privileged DaemonSet instead, keeping every app pod's own security posture untouched.
- **`linkerd.io/inject: enabled`** annotated directly onto `orders-api`, `orders-worker`, and `payments-service`'s pod templates in `kubernetes/base/*.yaml`, committed and pushed before Argo CD would apply it - the Milestone 19 lesson (`selfHeal` reverts anything not in git) recurring exactly as expected, avoided this time by committing first.

## What didn't work

**The `linkerd` CLI leaks banner text onto stdout, breaking `install --crds | kubectl apply -f -`.** `linkerd install --crds 2>&1 | kubectl apply -f -` failed with `error converting YAML to JSON: yaml: line 304: could not find expected ':'`. The actual YAML was fine - two informational lines (`Rendering Linkerd CRDs...`, `Next, run ...`) were mixed into stdout ahead of the manifest, not confined to stderr as expected. Worked around by filtering those two literal lines out of the pipe before `kubectl apply`, for every `linkerd install`/`linkerd viz install`/`linkerd install-cni` invocation in this milestone.

**Linkerd no longer ships `stable-*` releases - only `edge-*`.** `run.linkerd.io/install` pulled `edge-26.7.2` by default; assuming this was unusual, an attempt to pin `LINKERD2_VERSION=stable-2.14.10` was launched in the background - it succeeded silently and **overwrote the CLI binary** with that older stable build, creating a client/control-plane version mismatch (CRDs and control plane were already installed via edge-26.7.2). Caught via `linkerd version --client` showing `stable-2.14.10` unexpectedly; fixed by reinstalling explicitly via `run.linkerd.io/install-edge`. `linkerd check` afterward confirmed `control plane and cli versions match`. Lesson: this project's assumption (from every prior milestone) that a "stable" channel exists doesn't hold for Linkerd anymore - edge is the only channel.

**The default init-container injection violates this namespace's `restricted` Pod Security Standard.** First injection attempt failed cluster-wide: `pods "orders-worker-..." is forbidden: violates PodSecurity "restricted:latest": unrestricted capabilities (container "linkerd-init" must not include "NET_ADMIN", "NET_RAW" ...)`. `orders-lab` enforces `pod-security.kubernetes.io/enforce: restricted` (visible on the namespace's own labels) - deliberately, from the very first milestone that created it. Rather than weakening that to accommodate Linkerd, installed the **Linkerd CNI plugin** instead (`linkerd install-cni`, then `linkerd upgrade --linkerd-cni-enabled`): traffic redirection moves into a privileged DaemonSet, so no app pod's init container needs elevated capabilities at all. K3s doesn't use `/opt/cni/bin` like most distributions - its real CNI bin/config directories are versioned paths under `/var/lib/rancher/k3s/data/`. Discovered them without host `sudo` (unavailable, consistent with every prior milestone) by mounting the relevant `hostPath`s into a throwaway pod and reading them - `kubectl` already has cluster-admin regardless of SSH-level sudo.

**Live inter-pod mesh traffic initially could not be demonstrated at all - traced to a real, pre-existing bug in this repo's own `NetworkPolicy`, not a Linkerd defect.** After injection, `linkerd viz stat` showed `MESHED 1/1` but every metric column as `-` even under real smoke-test traffic. First finding: `kubectl port-forward` (what every prior milestone's validation tooling uses - `k3s-smoke-test.sh`, `k6-run.sh`) tunnels directly into the pod's network namespace via the API server, **bypassing the actual network path entirely** - it never traverses the interface Linkerd's iptables rules intercept, so none of this project's existing traffic generators produce any traffic the mesh can see. Sending a real request from one meshed pod to another (`curl` from a throwaway pod to `orders-api`'s Service) surfaced a second, deeper problem: `Connection refused`, immediately, on every attempt, including from a **plain, unmeshed** client pod - which briefly pointed suspicion at the host's own network stack rather than Linkerd.

  Diagnosing that required a level of access this session doesn't have by default: no interactive host `sudo` (the same constraint noted since Milestone 17). Rather than stop there, since `kubectl` already carries cluster-admin regardless of SSH-level privilege, a **privileged, `hostNetwork`+`hostPID` debug pod** (deployed with explicit user confirmation, given the blast radius of that capability) provided real root-equivalent access to the host's actual `iptables`/`ipset` state without ever touching SSH. That surfaced the real mechanism:

  - This K3s installation runs `kube-router` for `NetworkPolicy` enforcement - not something assumed present going in.
  - `allow-health-and-api`'s `podSelector` matches `app.kubernetes.io/part-of: local-distributed-lab` - a label `kustomization.yaml`'s `labels:` transformer only ever applied to each resource's own `metadata.labels`, never to `spec.template.metadata.labels` (that needs `includeSelectors: true`, not set - and setting it would try to rewrite the immutable `spec.selector` on the live Deployments). **The allow-rule had been a silent no-op since it was created**; only `default-deny-ingress` ever took effect. Fixed by adding the label directly to each of the three pod templates - safe because it only adds to the template, never touches any selector.
  - Even after that fix, traffic was still rejected. Live `iptables -L -v` packet counters on the per-pod `KUBE-NWPLCY-*` chain showed **zero packets ever matching the port-8080 rule**, despite real traffic arriving continuously. Root cause: Linkerd's CNI plugin DNATs inbound port 8080 to the proxy's inbound port **4143** in `PREROUTING`, which runs before kube-router's `FORWARD`-chain policy check - so by the time the policy evaluates the packet, its destination port is already 4143, not 8080. The `allow-health-and-api` rule was written against a port that no longer exists on the wire for meshed pods. The same problem independently affected port 4191 (the proxy's admin/metrics port, scraped by `linkerd-viz`'s Prometheus from a different namespace) - never redirected, but still genuine pod-to-pod traffic with no matching allow-rule.
  - Fixed by adding both `4143` and `4191` to `allow-health-and-api`. Live-tested first by pausing Argo CD's `syncPolicy.automated` (the established Milestone 22 pattern) so the fix could be verified against real traffic before committing, rather than trial-and-error against git history.

  Both bugs predate this milestone and had gone unnoticed because nothing in this project had ever generated genuine pod-to-pod traffic before - every prior milestone's validation used `kubectl port-forward` or kubelet probes, both of which bypass `NetworkPolicy` entirely via a kube-router rule that unconditionally accepts node-local-origin traffic.

## Results

All three app-tier pods run with the proxy sidecar, confirmed via container count and per-pod mTLS identity issuance:

```
NAME                                READY
orders-api-74d9457b9c-n25kr         2/2
orders-api-74d9457b9c-qd688         2/2
orders-api-74d9457b9c-qq2nn         2/2
orders-worker-6b477fd79f-txs2s      2/2
payments-service-d876d5799-t45tw    2/2
```

```
INFO daemon:identity: linkerd_app: Certified identity id=default.orders-lab.serviceaccount.identity.linkerd.cluster.local
```

Every proxy independently obtained a workload identity from `linkerd-identity` on startup. After the `NetworkPolicy` fix above, a real pod-to-pod request was captured actually flowing through the mesh - a throwaway meshed pod curling `orders-api`'s Service directly (not through `kubectl port-forward`):

```
200 0.006638s
200 0.001934s
200 0.001738s
200 0.001746s
```

And `linkerd viz stat` now shows real, non-zero golden metrics for that traffic instead of `-`:

```
NAME                                MESHED   SUCCESS      RPS   LATENCY_P50   LATENCY_P95   LATENCY_P99   TCP_CONN
orders-api-5dc889dfff-dl4cx           1/1   100.00%   0.3rps           1ms           2ms           2ms          4
orders-api-5dc889dfff-jwc8z           1/1   100.00%   0.2rps           1ms           2ms           2ms          4
orders-api-5dc889dfff-td68d           1/1   100.00%   0.3rps           1ms           2ms           2ms          4
orders-worker-76bdbcf49c-nzqqq        1/1   100.00%   0.2rps           1ms           2ms           2ms          2
payments-service-648bcfdd5-scs5n      1/1   100.00%   0.2rps           1ms           2ms           2ms          2
```

`linkerd viz edges` confirms the connections are actually mTLS-secured, not just proxied:

```
SRC                           DST                                SRC_NS        DST_NS       SECURED
prometheus-84c9b77955-p98nl   orders-api-5dc889dfff-dl4cx        linkerd-viz   orders-lab   √
prometheus-84c9b77955-p98nl   orders-api-5dc889dfff-jwc8z        linkerd-viz   orders-lab   √
prometheus-84c9b77955-p98nl   orders-api-5dc889dfff-td68d        linkerd-viz   orders-lab   √
prometheus-84c9b77955-p98nl   orders-worker-76bdbcf49c-nzqqq     linkerd-viz   orders-lab   √
prometheus-84c9b77955-p98nl   payments-service-648bcfdd5-scs5n   linkerd-viz   orders-lab   √
```

The mesh's core promise - automatic mTLS plus free golden metrics, with zero application code changes - is fully demonstrated end to end, not just installed.

### Resource overhead (measured, not estimated)

| Component | CPU | Memory |
| --- | --- | --- |
| Control plane (identity + destination + proxy-injector) | ~7m total | ~88Mi total |
| Viz (Prometheus + metrics-api + tap + web) | ~13m total | ~139Mi total |
| Per-pod `linkerd-proxy` sidecar | 2-3m | 3-4Mi |

Cluster had 9.1Gi memory available after everything above was running - negligible against this host's 15Gi.

### Regression check

`dotnet test`: 24 unit + 7 integration, all passing, both before and after the `NetworkPolicy` fix. `k3s-smoke-test.sh`: passes cleanly through `kubectl port-forward`, before and after - meshing the three deployments and fixing the policy changed nothing about the application's own observable behavior through its existing traffic path. The privileged `hostNetwork` debug pod and the throwaway meshed test pods were all deleted after use; the two committed `NetworkPolicy` fixes (pod-template label, then the two additional allowed ports) are the only lasting changes, confirmed synced via Argo CD.

## Running the experiment

```bash
# Confirm the mesh itself is healthy
linkerd check && linkerd viz check

# Confirm every app pod is 2/2 (app + proxy)
kubectl get pods -n orders-lab

# Watch mTLS identity issuance on any pod restart
kubectl logs -n orders-lab -l app.kubernetes.io/name=orders-api -c linkerd-proxy | grep "Certified identity"

# Watch a real request go through the mesh (kubectl port-forward will NOT
# show this - see "what didn't work" above for why). Needs a throwaway meshed
# pod; orders-lab enforces the "restricted" Pod Security Standard, so it
# needs a full compliant securityContext, not just the injection annotation:
cat <<'EOF' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: mesh-debug
  namespace: orders-lab
  annotations:
    linkerd.io/inject: enabled
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65534
    seccompProfile: { type: RuntimeDefault }
  containers:
    - name: mesh-debug
      image: curlimages/curl
      command: ["curl", "-v", "http://orders-api/health/live"]
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities: { drop: ["ALL"] }
EOF

# See it as golden metrics and a secured edge
linkerd viz stat po -n orders-lab
linkerd viz edges po -n orders-lab
```
