# Milestone 7 Performance Baseline

## Scope

This baseline measures the local K3s Orders application through its ClusterIP. PostgreSQL, Kafka, and the observability backends remain in Docker Compose on the same Ubuntu server.

The result is a repeatable laboratory reference, not a production capacity claim. It excludes TLS, Internet latency, multi-node scheduling, cross-zone traffic, and durable-storage contention from Kubernetes StatefulSets.

## Environment

- Date: 2026-07-27
- Server: AMD Ryzen 5 4500, 16 GB RAM, NVMe storage
- Kubernetes: single-node K3s
- Load generator: k6 1.6.1 on the same Ubuntu server
- Workload target: Orders API ClusterIP
- API replicas: 2
- Worker replicas: 1
- PostgreSQL: 17 Alpine in Docker Compose
- Kafka: 4.1.1, one KRaft broker in Docker Compose

Final application resources:

| Workload | Replicas | CPU request | CPU limit | Memory request | Memory limit |
| --- | ---: | ---: | ---: | ---: | ---: |
| Orders API | 2 | 200m each | 500m each | 128Mi each | 256Mi each |
| Orders Worker | 1 | 100m | 500m | 96Mi | 256Mi |

## Workload

The `baseline` profile ran for 70 seconds:

1. Ramp from 0 to 5 VUs over 10 seconds.
2. Hold 5 VUs for 20 seconds.
3. Ramp from 5 to 10 VUs over 10 seconds.
4. Hold 10 VUs for 20 seconds.
5. Ramp down over 10 seconds.

Each iteration creates an order with a unique correlation ID, validates response headers, and reads the created order. The runner then waits for every created event to appear in the Worker Inbox and for the Outbox backlog to reach zero.

## Final result

All configured thresholds passed.

| Measurement | Result | Threshold |
| --- | ---: | ---: |
| Iterations / orders created | 439 | Informational |
| HTTP requests | 878 | Informational |
| Average HTTP request rate | 12.51 requests/s | Informational |
| Failed HTTP requests | 0.00% | < 1% |
| Successful checks | 100.00% | > 99% |
| Successful order flows | 100.00% | > 99% |
| Create order p95 | 24.73 ms | < 500 ms |
| Create order p99 | 50.22 ms | < 1,000 ms |
| Get order p95 | 2.25 ms | < 300 ms |
| Get order p99 | 41.33 ms | < 750 ms |
| Inbox convergence | 439 / 439 | 100% |
| Pending Outbox messages | 0 | 0 |

Kubernetes Service distribution:

| API pod | Orders created |
| --- | ---: |
| Replica 1 | 131 |
| Replica 2 | 308 |

The distribution is intentionally observed rather than required to be exactly even. Kubernetes balances connections, while each k6 VU reuses HTTP connections; both replicas must receive traffic.

Peak two-second resource samples:

| Workload | Peak CPU | Peak memory |
| --- | ---: | ---: |
| Orders API replica 1 | 162m | 97Mi |
| Orders API replica 2 | 163m | 96Mi |
| Orders Worker | 77m | 59Mi |

The final CPU requests cover the observed peaks with conservative headroom. Memory requests also remain above observed peaks, while 256Mi limits retain room for runtime variation.

## Reliability finding

An initial smoke run exposed a historical state mismatch: PostgreSQL retained Inbox source positions from before the Kafka topic was recreated, so the new topic reused lower offsets. A unique source-position constraint caused valid new events to be classified as duplicates.

Milestone 7 changes the source-position index to non-unique and keeps the Inbox idempotency key on stable event identity: `(consumer_name, event_id)`. The integration test now verifies that the same event is rejected as a duplicate while a different event at a reused Kafka position is accepted.

## Reproduce

```bash
cd /srv/local-distributed-lab
scripts/k6-run.sh smoke
scripts/k6-run.sh baseline
```

Optional higher-impact profiles:

```bash
scripts/k6-run.sh stress
scripts/k6-run.sh soak
```

Raw run artifacts are intentionally excluded from Git because they include timestamps, pod names, ClusterIPs, and high-volume samples. Commit reviewed summaries instead.
