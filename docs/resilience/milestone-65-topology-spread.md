# Milestone 65: Topology Spread Constraints

## Scope

Five services (`cart-service`, `catalog-service`, `inventory-service`, `orders-api`, `storefront-service`) run multiple replicas and already carry a `PodDisruptionBudget`. A PDB protects against a *voluntary* disruption - it stops `kubectl drain`/a node upgrade from taking every replica down at once - but does nothing about where the scheduler *placed* those replicas in the first place. Nothing in this repo ever told the scheduler to spread a service's replicas across nodes, so an *involuntary* single-node failure could plausibly take out 100% of a service's replicas simultaneously, PDB or not.

## Design

Added a `topologySpreadConstraint` to all five services' pod template (`topologyKey: kubernetes.io/hostname`, `maxSkew: 1`, `whenUnsatisfiable: ScheduleAnyway`), scoped to each service's own `app.kubernetes.io/name` label so one service's spread never competes against another's:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: cart-service
```

**`ScheduleAnyway`, not `DoNotSchedule`, deliberately.** `DoNotSchedule` turns the constraint into a hard scheduling gate - on a cluster with fewer nodes than replicas (this lab's cluster included, see below), that risks a replica sitting `Pending` forever rather than degrading to "spread as well as it can." `ScheduleAnyway` expresses the same intent (prefer spread) without the same failure mode.

## What didn't work

**A manual `kubectl apply` got silently reverted within seconds - Argo CD's self-heal caught it exactly as designed.** Applied the change directly to the live cluster first to see real pod scheduling behavior before committing anything. It worked - new pods scheduled fine with the constraint active - but checking back moments later showed every one of the 5 deployments back to having no `topologySpreadConstraints` at all, and `orders-api`'s Rollout had an event trail reading `SkipSteps ... Rollback to stable ReplicaSets`. Argo CD compares the live cluster against what's in `main`, and a `kubectl apply` that never touched git is, correctly, treated as drift to be corrected - not a bug in Argo CD, a reminder that "verify live before committing" in a GitOps-managed cluster means the verification itself doesn't stick unless it's committed and pushed. Fixed by committing and pushing first, then forcing an Argo CD refresh (`kubectl patch application ... -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'`) rather than relying on the next poll cycle.

**This cluster is single-node, so the constraint cannot be proven to actually spread anything.** `kubectl get nodes` shows exactly one (`lab-local`). With one topology domain, every replica of every service lands there regardless of the constraint - there is no second node to demonstrate spreading *to*. What *is* honestly provable here: the constraint is syntactically valid, Argo CD/Kustomize renders it without error, and `ScheduleAnyway` does not block scheduling even in a topology where satisfying `maxSkew: 1` across hostnames is trivially impossible (there's only one hostname). That's the real, calibrated claim for this milestone - not "verified pods spread across nodes," which would be false on this cluster - and the reason `DoNotSchedule` was rejected outright rather than tried and found to work by luck.

## Results

After pushing and forcing a refresh, Argo CD converged cleanly:

```
Application: Synced Healthy
Rollout orders-api: Healthy, step 6/6

cart-service-59bfbcd5-*        2/2 Running (x2)
catalog-service-58d967c9c4-*   2/2 Running (x2)
inventory-service-95f86c78d-*  2/2 Running (x2)
storefront-service-85bc59899c-* 2/2 Running (x2)
orders-api-ccc8d9b4f-*         2/2 Running (x3)
```

All 5 confirmed carrying the constraint post-sync (`kubectl get deployment/rollout <name> -o jsonpath='{.spec.template.spec.topologySpreadConstraints}'`). No pod ever went `Pending` at any point, on this single-node cluster, confirming `ScheduleAnyway`'s no-op-when-unsatisfiable behavior in practice, not just in the docs. Full solution (132 tests, 9 projects) unaffected - this milestone touched only Kubernetes manifests, no application code.

## Running it

```bash
# Render and sanity-check before applying anywhere
kubectl kustomize kubernetes/overlays/local | grep -A6 topologySpreadConstraints

# Force Argo CD to pick up a push immediately instead of waiting for the next poll
kubectl patch application distributed-ecommerce -n argocd --type merge \
  -p '{"metadata":{"annotations":{"argocd.argoproj.io/refresh":"hard"}}}'

# Confirm it landed
kubectl get deployment cart-service -n orders-lab \
  -o jsonpath='{.spec.template.spec.topologySpreadConstraints}'
```
