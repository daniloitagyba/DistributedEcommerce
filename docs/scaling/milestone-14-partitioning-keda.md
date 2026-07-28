# Milestone 14 Kafka Partitioning + KEDA Autoscaling

## Scope

Milestone 8 gave `orders-api` CPU-based horizontal scaling. `orders-worker` never had any — it ran as a single, fixed replica for every milestone through 13, including the CQRS projector added in Milestone 13. This milestone makes `orders-worker` horizontally scalable and drives that scaling from **Kafka consumer-group lag** instead of CPU, via [KEDA](https://keda.sh), contrasting directly with Milestone 8's CPU-based approach.

## Design

- **Partitioning was already in place.** `orders.created.v1` and `payments.result.v1` were both created with 3 partitions from Milestone 7 onward (`KAFKA_NUM_PARTITIONS: 3` in `kafka-init`), and both producers (`KafkaOrderEventPublisher`, `PaymentEventPublisher`) already key every message by `OrderId`. So partition-per-order ordering was already correct; the only missing piece was a consumer side able to actually use more than one partition at a time.
- **`orders-worker` becomes horizontally scalable.** Changed its Deployment `strategy` from `Recreate` to `RollingUpdate` (`maxUnavailable: 1, maxSurge: 0`) — `Recreate` assumed a singleton and would tear down every replica at once on any future deploy. No code changes were needed for correctness: all three consumer groups (`orders-worker`, `orders-worker-payments-result`, `orders-projector`) already rely on the database-backed `InboxStore` for dedup rather than any in-memory state, so Kafka's own consumer-group rebalancing safely distributes partitions across however many replicas exist.
- **KEDA installed via Helm** (`helm install keda kedacore/keda --namespace keda`) rather than vendoring raw manifests, since Helm was already available on the server.
- **One `ScaledObject` (`kubernetes/base/orders-worker-scaledobject.yaml`) with four Kafka triggers** — one per (consumer group, topic) pair the worker actually owns: `orders-worker`/`orders.created.v1`, `orders-worker-payments-result`/`payments.result.v1`, and two for `orders-projector` (it subscribes to both topics under one group). KEDA scales on the **max** across all triggers' computed replica counts, so a lag spike in any single group scales the whole Deployment — appropriate here since all three groups live in the same process. `lagThreshold: 50`, `maxReplicaCount: 3` (matching the partition count — more replicas than partitions would leave consumers permanently idle).
- **Scaling behavior is tuned explicitly** rather than left at Kubernetes' HPA defaults: scale-up has no stabilization window and can add all 3 replicas within one 15-second policy window (react fast to a burst); scale-down waits 60 seconds of low lag and then removes one replica per 30 seconds (react slowly to avoid flapping). The out-of-the-box HPA default for scale-down is a 5-minute stabilization window, which works but would have made every validation cycle in this milestone take 5+ minutes longer than necessary to observe.

## What didn't work

**The bootstrap address that works for every existing consumer doesn't work for KEDA — cross-namespace DNS.** Every application pod (`orders-api`, `orders-worker`, `payments-service`) lives in the `orders-lab` namespace and connects to Kafka via the short name `kafka:9092`, which resolves fine because Kubernetes' per-namespace DNS search path tries the caller's own namespace first. The KEDA operator, however, runs in its own `keda` namespace — `kafka` alone doesn't resolve there, only `kafka.orders-lab.svc.cluster.local` does. Simply using the FQDN in the `ScaledObject`'s `bootstrapServers` field only got partway: Kafka clients use the bootstrap address purely for the *first* connection, then switch to whatever address the broker's own metadata response *advertises* for subsequent requests — and `KAFKA_ADVERTISED_LISTENERS` was still just `kafka:9092`, so KEDA's scaler kept getting redirected back to a name it couldn't resolve.

The fix is Kafka's own multi-listener support, not a KEDA workaround: added a second listener, `PLAINTEXT_K8S`, on port 9094, advertised as `kafka.orders-lab.svc.cluster.local:9094` — resolvable from any namespace — while the original `PLAINTEXT` listener on 9092 keeps advertising the short name unchanged. Every existing consumer and producer still points at `kafka:9092` and was never touched; only the new `ScaledObject` (and any other future cross-namespace client) uses the 9094 listener. Confirmed the fix by running a throwaway debug pod inside `orders-lab` against `kafka.orders-lab.svc.cluster.local:9094` before trusting KEDA's own (initially stale, pre-fix) error events.

**HPA's default scale-down stabilization (5 minutes) is fine in production, unusable for iterating on a validation script.** Left unset, the shadow HPA KEDA creates inherits Kubernetes' own 5-minute default scale-down stabilization window regardless of KEDA's `cooldownPeriod` (which, it turns out, only applies when `minReplicaCount` or `idleReplicaCount` is 0 — irrelevant here since the floor is 1). Set an explicit `behavior.scaleDown.stabilizationWindowSeconds: 60` with a one-replica-per-30-seconds policy so a full scale-up/scale-down cycle could be observed and documented in a single test run instead of needing a 5+ minute wait after every load test.

## Results

Reused Milestone 8's unmodified `autoscale` profile (0 → 75 VUs over 15s, held for 60s, ramped down over 15s) as load — no new load shape needed, and this milestone's validation carries no regression risk to Milestone 8's own acceptance suite.

| Timestamp (relative to load start) | Aggregate Kafka lag across all 4 triggers | `orders-worker` replicas |
| --- | ---: | ---: |
| Before load (idle) | 0–7 | 1 |
| ~15s (ramp reaching peak) | 36 | 1 |
| ~30s | 150 (capped display; over threshold) | 3 |
| ~45s–90s (sustained peak) | ~50 (right at `lagThreshold`) | 3 |
| Load ends | 0 | 3 |
| +30s after lag hits 0 | 0 | 2 |
| +78s after lag hits 0 | 0 | 1 |

`orders-worker` scaled from 1 to the maximum 3 replicas within about 30 seconds of load ramping up, held 3 replicas through the entire sustained-peak phase (aggregate lag stayed pinned near the 50-message threshold rather than climbing unbounded, confirming the extra replicas were genuinely absorbing throughput), and scaled back down to 1 replica in three steps over about 78 seconds once load stopped — matching the configured one-replica-per-30-second scale-down policy exactly.

`failed_rate=0`, `checks_rate=100%`, `flow_rate=100%` on both validation runs of `autoscale` — the write-side acceptance criteria established in Milestone 8 are unaffected by giving the worker its own independent, lag-driven scaling path. Projection lag p95 during a KEDA-scaled run (`OrderCreated` ~500 ms, `PaymentDecided` ~1,371 ms) stayed comparable to Milestone 13's already-fixed single-instance baseline, which is the expected outcome: Milestone 13 fixed the *offset-commit* bottleneck; this milestone adds the capacity to keep absorbing load beyond what any single instance could handle, which the lag-vs-replica-count table above demonstrates directly.

## Running the experiment

```bash
cd /srv/local-distributed-lab
kubectl get scaledobject -n orders-lab
kubectl get hpa -n orders-lab           # keda-hpa-orders-worker is KEDA's shadow HPA
scripts/k6-run.sh autoscale &
watch -n5 'kubectl get hpa keda-hpa-orders-worker -n orders-lab; kubectl get pods -n orders-lab -l app.kubernetes.io/name=orders-worker'
```
