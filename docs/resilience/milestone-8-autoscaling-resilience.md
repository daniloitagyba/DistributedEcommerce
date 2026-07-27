# Milestone 8 Autoscaling and Resilience

## Scope

This milestone validates two operational behaviors in the local K3s application runtime:

1. The Orders API scales horizontally under measured CPU pressure and returns to its minimum replica count.
2. API and Worker rolling restarts complete while traffic remains active, without losing confirmed orders or producing client-visible failures.

PostgreSQL, Kafka, and the observability backends remain in Docker Compose. The experiments use Kubernetes rolling restarts only; they do not delete pods, workloads, persistent data, or infrastructure.

## Autoscaling policy

The Orders API uses an `autoscaling/v2` HorizontalPodAutoscaler:

| Setting | Value |
| --- | ---: |
| Minimum replicas | 2 |
| Maximum replicas | 4 |
| CPU target | 60% of the 200m request |
| Scale-up stabilization | 0 seconds |
| Scale-up maximum | 2 pods or 100% every 15 seconds |
| Scale-down stabilization | 60 seconds |
| Scale-down maximum | 1 pod or 50% every 60 seconds |

The minimum preserves service availability during a rollout. The bounded maximum and conservative scale-down policy fit the single-node, 16 GB laboratory server.

## Autoscaling experiment

The `autoscale` k6 profile ramps to 75 VUs over 15 seconds, holds for 60 seconds, and ramps down over 15 seconds. Each iteration creates and reads an order while preserving a unique correlation ID.

Final result:

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Maximum API replicas | 4 | At least 3 |
| Final API replicas | 2 | 2 |
| Orders created | 5,629 | Informational |
| HTTP requests | 11,258 | Informational |
| Failed HTTP requests | 0.00% | < 1% |
| Successful checks | 100.00% | > 99% |
| Successful order flows | 100.00% | > 99% |
| Create order p95 | 3.68 ms | < 750 ms |
| Create order p99 | 4.84 ms | < 1,500 ms |
| Get order p95 | 1.36 ms | < 500 ms |
| Get order p99 | 2.59 ms | < 1,000 ms |
| Inbox convergence | 5,629 / 5,629 | 100% |
| Pending Outbox messages | 0 | 0 |

The HPA progressed from two to three and then four replicas. After CPU pressure ended, it returned to two replicas without manual scaling.

The Worker needed more than the original 60-second convergence allowance to drain the autoscale workload. The runner now gives this high-volume profile 180 seconds, while still requiring zero pre-existing Kafka lag before a run and exact Inbox/Outbox convergence afterward.

## Rolling-restart experiment

The `resilience` profile holds five VUs for 75 seconds. After traffic begins, the test performs:

1. `kubectl rollout restart deployment/orders-api`
2. Wait for the API rollout to complete.
3. `kubectl rollout restart deployment/orders-worker`
4. Wait for the Worker rollout to complete.
5. Require the k6, database, Kafka pipeline, and Loki assertions to pass.

An initial run exposed two five-second POST timeouts when old API pods terminated. A 30-second termination grace period already existed, but Kubernetes sent SIGTERM immediately after starting termination. The API pod now uses a five-second `preStop` delay, allowing endpoint and connection state to converge before ASP.NET Core begins graceful shutdown.

The unchanged strict workload passed after that fix:

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| API Deployment revision | 6 to 7 | Revision advances |
| Worker Deployment revision | 6 to 7 | Revision advances |
| Orders created | 369 | Informational |
| HTTP requests | 738 | Informational |
| Failed HTTP requests | 0.00% | 0% |
| Successful checks | 100.00% | 100% |
| Successful order flows | 100.00% | 100% |
| Create order p95 | 22.43 ms | < 750 ms |
| Create order p99 | 531.76 ms | < 1,500 ms |
| Get order p95 | 2.45 ms | < 500 ms |
| Get order p99 | 142.79 ms | < 1,000 ms |
| Inbox convergence | 369 / 369 | 100% |
| Pending Outbox messages | 0 | 0 |
| Worker graceful-shutdown log in Loki | Present | Present |

The higher tail latency during the rollout is expected and remains within the explicit thresholds. No confirmed order was lost.

## Reproduce

Run these commands on the Ubuntu server:

```bash
cd /srv/local-distributed-lab
scripts/hpa-test.sh
scripts/resilience-test.sh
```

The tests create ignored raw evidence under `artifacts/k6/`, including k6 summaries, pod resource samples, HPA timelines, pipeline state, and concise reports. Only this reviewed summary is committed.

## Boundaries

These results demonstrate controlled behavior on one K3s node. They do not prove node-failure tolerance, multi-zone availability, or production capacity. PostgreSQL and Kafka are still single instances outside Kubernetes and remain deliberate single points of failure for later dependency-recovery exercises.
