# Milestone 36: Kubernetes Lease Leader Election

## Scope

The first Tier A gap identified in this lab's broader architecture review: no distributed-systems leader-election pattern existed anywhere, despite `orders-worker` running periodic singleton-style jobs (`SagaTimeoutSweeper`, Milestone 22) while being horizontally scalable via KEDA (Milestone 14). Investigating this surfaced a real, more fundamental problem than "does N replicas duplicate work" - it led to fixing the actual root cause rather than papering over it with election alone.

## What investigation found

`OutboxPublisher` (used by both `orders-api` and `payments-service`, both multi-replica) was already safe: its query uses Postgres's `FOR UPDATE SKIP LOCKED`, a proven concurrent-work-queue pattern - multiple replicas polling the same table claim disjoint rows automatically, no coordination needed. Not a leader-election candidate; nothing to fix there.

`SagaTimeoutSweeper`, however, read from `SagaOrchestrationTracker` - a plain in-memory `ConcurrentDictionary`, explicitly documented in its own source as "a deliberate scope choice over a dedicated database table" at the time Milestone 22 was written. The real problem this milestone found: **that state doesn't survive a pod restart or a KEDA scale-in of `orders-worker`** - exactly the kind of event this lab's own chaos testing (Milestone 8, Milestone 31) induces on purpose. A stuck saga tracked by a pod that gets killed or scaled away simply vanishes from tracking - its timeout would never fire, silently defeating the entire point of the sweeper.

Adding leader election on top of the in-memory tracker *without* fixing durability would have made this worse, not better: concentrating all saga-tracking state onto a single elected leader means losing *that one pod* loses *everything* being tracked, instead of only the fraction any one of several replicas happened to be holding. Leader election and durable state are both necessary here, not alternatives to each other.

## Design

- **Durable state**: `SagaOrchestrationState` (`Orders.Domain`) + a new `saga_orchestration_states` table (EF Core migration `AddSagaOrchestrationStates`) - EF Core owns schema only, exactly like the existing `OrderEvent`/`order_events` pattern. Runtime reads/writes go through `SagaOrchestrationStore` (raw Npgsql, matching `OrderEventStoreAppender`/`OrderProjectionStore`'s established style, not EF `SaveChanges`):
  - `TrackRequestedAsync` - `INSERT ... ON CONFLICT (order_id) DO NOTHING`
  - `TryCompleteRepliedAsync` - `DELETE ... RETURNING`, atomically completes and removes on a payment reply
  - `ClaimTimedOutAsync` - `DELETE ... WHERE order_id IN (SELECT ... FOR UPDATE SKIP LOCKED)`, atomically claims and removes timed-out rows. This is belt-and-suspenders, not the primary safety mechanism - leader election already ensures only one replica calls it at a time; `SKIP LOCKED` just protects the brief window during a leadership handoff.
- **Leader election**: `LeaderElectionService` wraps `k8s.LeaderElection.LeaderElector` (the `KubernetesClient` NuGet package's C# port of client-go's `leaderelection`) against a `Lease` object (`orders-worker-saga-sweeper`, `orders-lab` namespace). `SagaTimeoutSweeper` still runs its loop on every replica but checks `leaderElection.IsLeader` each tick and no-ops if it isn't currently the leader - the classic "one active worker, N standby" pattern, demonstrated against the real K8s API rather than reinvented.
- **RBAC trade-off**: `orders-worker`'s `automountServiceAccountToken` had been `false` since Milestone 26's hardening pass (it never needed to talk to the K8s API server before). Leader election needs the projected ServiceAccount token for `InClusterConfig()` to authenticate - flipped back to `true`, but scoped by a new `Role` granting *only* `get/list/watch/create/update/patch` on `leases.coordination.k8s.io` in this one namespace. It still cannot read Pods, Secrets, or anything else.

## Results

Live proof, not just "it compiled": scaled `orders-worker` to 2 replicas (via KEDA's `ScaledObject.spec.minReplicaCount`, temporarily raised from 1 to 2 to force it without needing to generate real Kafka lag) and inspected the `Lease` and both pods' logs directly.

**Only one of two replicas became leader:**
```
$ kubectl get lease orders-worker-saga-sweeper -n orders-lab -o jsonpath='{.spec.holderIdentity}'
orders-worker-678886c4b7-2vr2x

# 2vr2x's own log:
{"EventId":7001,"Message":"Instance orders-worker-678886c4b7-2vr2x became the leader"}

# m97w2 (the second replica) never logs EventId 7001 at all
```

**Killing the leader pod triggers real failover** - the surviving replica picked up leadership without any manual intervention:
```
$ kubectl delete pod orders-worker-678886c4b7-2vr2x -n orders-lab
pod "orders-worker-678886c4b7-2vr2x" deleted

# ~15-20s later, m97w2's log:
{"EventId":7001,"Message":"Instance orders-worker-678886c4b7-m97w2 became the leader"}

$ kubectl get lease orders-worker-saga-sweeper -n orders-lab -o jsonpath='{.spec.holderIdentity} transitions={.spec.leaseTransitions}'
orders-worker-678886c4b7-m97w2 transitions=2
```
`leaseTransitions=2` confirms the full sequence: no leader → `2vr2x` → `m97w2`.

**The orchestrated saga still works correctly against the new Postgres-backed store** - 3 orders created, all 3 completed:
```
OrchestratedSagaCompleted order 03aa4b90-... approved=True latencyMs=80.83
OrchestratedSagaCompleted order d65f25e4-... approved=True latencyMs=7.59
OrchestratedSagaCompleted order 4eaa47bf-... approved=True latencyMs=7.48
```

### Regression check

`dotnet test`: 28 unit tests unchanged, 3 new integration tests (`SagaOrchestrationStoreTests`, against a real Testcontainers Postgres) covering track→complete, an unknown-order lookup, and claim-only-past-cutoff-and-remove semantics - all passing. `scripts/k6-run.sh smoke` post-deploy: `failed_rate=0`, `checks_rate=1`, `flow_rate=1`.

## Running it

```bash
# Who's leading right now
kubectl get lease orders-worker-saga-sweeper -n orders-lab -o jsonpath='{.spec.holderIdentity}'

# Force a failover test (temporarily bypass KEDA to get >1 replica)
kubectl patch scaledobject orders-worker -n orders-lab --type merge -p '{"spec":{"minReplicaCount":2}}'
kubectl delete pod <current-leader-pod> -n orders-lab
kubectl patch scaledobject orders-worker -n orders-lab --type merge -p '{"spec":{"minReplicaCount":1}}'  # restore
```
