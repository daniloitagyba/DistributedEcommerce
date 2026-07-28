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

**Live inter-pod mesh traffic could not be demonstrated - a pre-existing gap on this host, not a Linkerd defect.** After injection, `linkerd viz stat` showed `MESHED 1/1` but every metric column as `-` even under real smoke-test traffic. Investigating: `kubectl port-forward` (what every prior milestone's validation tooling uses - `k3s-smoke-test.sh`, `k6-run.sh`) tunnels directly into the pod's network namespace via the API server, **bypassing the actual network path entirely** - it never traverses the interface Linkerd's iptables rules intercept, so none of this project's existing traffic generators produce any traffic the mesh can see. Sending a real request from one meshed pod to another (`curl` from a throwaway pod to `orders-api`'s Service) surfaced a second, deeper problem: `Connection refused`, immediately, on every attempt. The investigation, cleanly isolating the cause:
  - Ruled out Linkerd's own policy layer - `defaultInboundPolicy` is `all-unauthenticated` cluster-wide, and no `Server`/`AuthorizationPolicy` targets `orders-lab`.
  - Ruled out `NetworkPolicy` - removing `default-deny-ingress` entirely (temporarily) made no difference; the failure persisted identically.
  - Ruled out DNS - `orders-api.orders-lab.svc.cluster.local` resolved correctly via CoreDNS every time.
  - Ruled out Linkerd's CNI redirect specifically - a **plain, unmeshed** pod hitting `orders-api`'s pod IP directly (no proxy involved on either side) failed exactly the same way.
  - Isolated the actual boundary: a pod reaching the API server's ClusterIP (`kubernetes.default.svc`, backed by the host network) got a real `401` - proving TCP routing to *host-network-backed* ClusterIPs works. A pod reaching **any** other pod's network namespace - `orders-api`, or `traefik` in `kube-system`, completely unrelated to this milestone - failed or timed out every time.

  This narrows the fault to K3s/flannel's pod-to-pod bridge path on this specific host, independent of everything this milestone added. It was never noticed before because nothing in this project ever generated genuine pod-to-pod traffic: every prior milestone's validation used `kubectl port-forward`, kubelet health probes (host-network-originated, a different path), or the external services pointing at the Compose stack's host IP. Diagnosing further needs host root (`iptables`/`nft` state, conntrack) that isn't available in this environment - the same constraint noted since Milestone 17. Documented as a real, unresolved limitation rather than worked around or hidden.

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

Every proxy independently obtained a workload identity from `linkerd-identity` on startup - the mTLS machinery itself is live and working, even though no inter-pod request was captured to show it in flight.

### Resource overhead (measured, not estimated)

| Component | CPU | Memory |
| --- | --- | --- |
| Control plane (identity + destination + proxy-injector) | ~7m total | ~88Mi total |
| Viz (Prometheus + metrics-api + tap + web) | ~13m total | ~139Mi total |
| Per-pod `linkerd-proxy` sidecar | 2-3m | 3-4Mi |

Cluster had 9.1Gi memory available after everything above was running - negligible against this host's 15Gi.

### Regression check

`dotnet test`: 24 unit + 7 integration, all passing. `k3s-smoke-test.sh`: passes cleanly through `kubectl port-forward` exactly as before - meshing the three deployments changed nothing about the application's own observable behavior. The `NetworkPolicy` edits made while isolating the connectivity gap were never committed; a hard Argo CD refresh + sync during cleanup confirmed both policies are back to their exact original committed state.

## Running the experiment

```bash
# Confirm the mesh itself is healthy
linkerd check && linkerd viz check

# Confirm every app pod is 2/2 (app + proxy)
kubectl get pods -n orders-lab

# Watch mTLS identity issuance on any pod restart
kubectl logs -n orders-lab -l app.kubernetes.io/name=orders-api -c linkerd-proxy | grep "Certified identity"

# Reproduce the pod-to-pod finding (needs a throwaway meshed pod - kubectl port-forward
# will NOT reproduce this, see "what didn't work" above). orders-lab enforces the
# "restricted" Pod Security Standard, so the debug pod needs a full compliant
# securityContext, not just the injection annotation:
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
```
