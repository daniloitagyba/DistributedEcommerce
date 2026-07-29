# Milestone 37: Network Partition Game Day (Chaos Mesh NetworkChaos)

## Scope

A genuinely different fault from anything tried before in this lab. Toxiproxy (Milestone 10) injects latency/timeouts on a single proxied hop; Chaos Mesh's `PodChaos` (Milestone 31) kills a process outright. Neither is a real network partition: a node that stays up, keeps its other dependencies reachable, and is otherwise completely healthy, but loses connectivity to exactly one thing - the actual CAP-theorem scenario, and a meaningfully different failure mode than "this process is gone" or "this one call is slow."

Two experiments, both using Chaos Mesh's `NetworkChaos` with `action: partition` (real iptables `DROP` rules, both directions) against `externalTargets` - Kafka and Postgres aren't Kubernetes pods here, they run in the docker-compose stack bridged into K3s via fixed IPs, so pod-selector-based targeting doesn't apply; `externalTargets` (IP-based) does.

1. **`orders-worker` ↔ Kafka** (60s) - Postgres and Redis stay fully reachable.
2. **`orders-api` (all 3 replicas) ↔ Postgres** (45s) - Kafka and Redis stay fully reachable.

## Results

### Experiment 1: orders-worker ↔ Kafka

```
Created 5 orders before the partition
Partition applied at 22:51:31
orders-worker readiness 5s into the partition: true
Created 5 more orders during the partition (orders-api itself is unaffected)
orders-worker ready 3s after the partition healed

--- Experiment 1 results ---
Total orders (before + during partition): 10
Converged to a terminal state: 10
Data loss: 0 order(s)
Recovery time (partition healed to worker ready): 3s
Total time (partition applied to full convergence): 70s
```

Zero data loss, fully automatic recovery - the same Kafka at-least-once + Postgres Inbox dedup guarantee this lab has relied on since Milestone 7 held under a real partition, not just a killed process. The 5 orders created *during* the partition queued normally in `orders-api`'s outbox (unaffected - only `orders-worker`'s connectivity was cut) and were consumed the moment the consumer could reach Kafka again.

### Experiment 2: orders-api ↔ Postgres

```
orders-api ready Service endpoints before the partition: 3
Partition applied at 22:52:41
orders-api ready Service endpoints 10s into the partition: 3 (expected: 0)
POST /orders during the partition: 000curl_failed
orders-api ready 3s after the partition healed
POST /orders after recovery: order c53ffc67-... created successfully
```

`POST /orders` genuinely failed during the partition (`000`/connection failure - the request itself hit the cut connection, independent of what the readiness probe reported), and recovery was immediate and automatic once the partition healed.

## What the single point-in-time endpoint check got wrong (a real methodology finding, not a product bug)

The script's one-time check of ready Service endpoints (at the 10s mark) reported all 3 replicas still "ready" during the Postgres partition - which looked, at first, like the readiness probe wasn't detecting the fault. `kubectl get events` from the same window told a different story:

```
62s  Warning  Unhealthy  pod/orders-api-7d7bcf7b69-gcczr  Readiness probe failed: Get "http://.../health/ready": context deadline exceeded
59s  Warning  Unhealthy  pod/orders-api-7d7bcf7b69-s55zm  Readiness probe failed: Get "http://.../health/ready": context deadline exceeded
58s  Warning  Unhealthy  pod/orders-api-7d7bcf7b69-bnp5z  Readiness probe failed: Get "http://.../health/ready": context deadline exceeded
113s Warning  Unhealthy  pod/orders-worker.../health/ready: context deadline exceeded
```

The probes *were* failing, almost immediately (`PostgresHealthCheck`'s `SELECT 1` and `KafkaHealthCheck`'s metadata fetch both hang against a genuinely partitioned target until `timeoutSeconds: 2` is hit). What the single check missed is that Kubernetes doesn't remove a pod from Service endpoints on the first failed probe - `readinessProbe.failureThreshold: 3` at `periodSeconds: 5` means it takes **three consecutive failures, roughly 15-21 seconds**, before the condition actually flips and the pod leaves the endpoint list. The 10-second check in this script's first run landed inside that detection window, not after a health-check failure to detect the fault at all. A longer partition (this experiment ran 45s total) does have a real "zero ready endpoints" window later on - just not at the single moment sampled here.

Worth knowing generally, not just for this lab: readiness-based failure detection has a real, non-zero floor (`failureThreshold × periodSeconds`), and a chaos experiment's observation cadence needs to account for it or it will report false negatives about detection that's actually working correctly, just not instantaneously.

## Running it

```bash
scripts/network-partition-gameday.sh
```

Applies both `NetworkChaos` manifests in `kubernetes/chaos-experiments/` in sequence, measuring order convergence, Service endpoint counts, and recovery time for each.
