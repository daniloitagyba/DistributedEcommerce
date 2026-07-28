# Milestone 17 Sealed Secrets for GitOps

## Scope

Milestone 15 made Argo CD the source of truth for every `kubernetes/overlays/local` resource except one: the `orders-runtime` Secret (the Orders and Payments Postgres connection strings) was still created imperatively by `scripts/k3s-deploy.sh` on every run, `kubectl apply`-ed straight into the cluster from a value resolved out of the local Compose config. That's the one remaining action that isn't `git push` — this milestone closes it with [sealed-secrets](https://github.com/bitnami/sealed-secrets), so the encrypted secret material can live in git like everything else, decrypted only in-cluster.

## Design

- **The sealed-secrets controller, installed via Helm** (`helm install sealed-secrets sealed-secrets/sealed-secrets --namespace kube-system`), matching this cluster's existing convention for infrastructure components (KEDA, Argo CD, Argo Rollouts are all Helm releases too). It holds the private key that can decrypt a `SealedSecret` back into a normal `Secret`; nothing else can.
- **`kubeseal` (the client-side sealing CLI) has no root-installable package on this box** without an interactive sudo password, so it's installed to `~/.local/bin` instead — a one-time, user-scoped install, not something any script depends on going forward.
- **The current live connection strings were sealed once**, using the exact same Compose-config resolution `k3s-deploy.sh` used to do at deploy time, producing `kubernetes/base/orders-runtime-sealed-secret.yaml`. That file is ciphertext — safe to commit — and by default is scoped to decrypt only as `orders-runtime` in the `orders-lab` namespace, so it can't be copy-pasted into a different Secret name or namespace even by someone with repo access.
- **`k3s-deploy.sh` no longer creates any Secret.** The `SealedSecret` is just another resource in `kubernetes/base`, applied by the same `kubectl apply --kustomize` step (and, in steady state, by Argo CD) as everything else. The script now only waits for the resulting `Secret` to materialize before the Jobs/Pods that mount it start.
- **A password rotation is a manual `kubeseal` + `git commit`, not automated.** This is the actual tradeoff GitOps makes here: convenience (any script can mint a fresh secret on demand) is traded for auditability (every secret change is a reviewable git commit, and the plaintext never touches the deploy script or its logs again).

## What didn't work

**The upstream Helm repo has moved and the old URL 404s.** `https://bitnami-labs.github.io/sealed-secrets` — the URL in most existing tutorials and even the project's own older docs — now 404s; the GitHub org itself redirects `bitnami-labs/sealed-secrets` → `bitnami/sealed-secrets`. The correct chart repo is `https://bitnami.github.io/sealed-secrets`. Same story for the `kubeseal` release binaries: they're at `github.com/bitnami/sealed-secrets/releases`, not the `-labs` org.

**No interactive sudo means no `/usr/local/bin` install.** Every other CLI on this box (`kubectl`, `helm`) lives in `/usr/local/bin`, owned by root; installing there needs a sudo password this non-interactive SSH session doesn't have (a known, already-documented constraint on this server from earlier work in this lab). `kubeseal` isn't needed by any automated script — only by a human sealing a value by hand — so it went to the already-existing, user-writable `~/.local/bin` instead rather than fighting for root.

## Results

### Decryption round-trip

| Step | Observed |
| --- | --- |
| Delete the live `orders-runtime` Secret, apply only the `SealedSecret` | Controller recreated the `Secret` in ~3s |
| Diff decrypted values against the pre-deletion original | `orders-connection-string` and `payments-connection-string` byte-for-byte identical |

### Full deploy with the imperative step removed

`scripts/k3s-deploy.sh` run end-to-end: `orders-migrations-m7` and `payments-migrations-m12` Jobs completed, `orders-api` (Rollout), `orders-worker`, and `payments-service` all rolled to `Running` — none of them ever saw a missing-Secret error, confirming the wait-for-materialization step is sufficient.

### Application-level validation

| Check | Result |
| --- | --- |
| `k3s-smoke-test.sh` | Passed — 6 orders created and consumed through two ready API replicas |
| `k6-run.sh saga` | `failed_rate=0`, `saga_correct_outcome_rate=99.47%` (377/379) — consistent with this lab's pre-existing, already-documented tail-latency flakiness in the saga convergence poll, not a regression from this change |

Both services read their respective connection string from the sealed-secret-sourced `Secret` correctly — Payments.Service approving/declining orders proves `payments-connection-string` resolved correctly, not just Orders.Api's half.

## Running the experiment

```bash
# One-time, per secret value, whenever POSTGRES_PASSWORD (or any other
# sealed value) changes:
kubectl create secret generic orders-runtime --namespace orders-lab \
  --from-file=orders-connection-string=... \
  --from-file=payments-connection-string=... \
  --dry-run=client --output yaml | \
  kubeseal --controller-name=sealed-secrets --controller-namespace=kube-system \
    --format yaml > kubernetes/base/orders-runtime-sealed-secret.yaml
git add kubernetes/base/orders-runtime-sealed-secret.yaml
git commit -m "..." && git push
```
