# Milestone 10 Resilience and Chaos Engineering

## Scope

This milestone adds explicit client-side resilience policies (timeout, retry, circuit breaker) around every external dependency the Orders API and Worker call — PostgreSQL, Kafka, and Redis — using Polly v8 via `Microsoft.Extensions.Resilience`. It then proves those policies actually work by injecting real network faults with [Toxiproxy](https://github.com/Shopify/toxiproxy) and observing the system's behavior end to end on the live K3s deployment.

Two fault modes are tested against both PostgreSQL and Kafka:

- **Latency**: the dependency is slow but reachable. The system should tolerate it — same success rate, just higher latency — without tripping the circuit breaker.
- **Outage**: the dependency is completely unreachable. The system should fail fast (bounded latency, no multi-second hangs) and fully recover once the dependency returns, with zero data loss or partial writes.

## Design

### Resilience pipelines (`BuildingBlocks/ResilienceExtensions.cs`)

Three named Polly pipelines, registered once and reused via `ResiliencePipelineProvider<string>`:

| Pipeline | Retry | Circuit breaker | Timeout |
| --- | --- | --- | --- |
| `postgres` | 2 attempts, 100 ms exponential backoff | 50% failure ratio, min. 4 calls / 10 s window, 5 s break | 2 s |
| `kafka-producer` | 1 attempt, 100 ms constant backoff | same shape | 3 s |
| `redis` | none | same shape | 150 ms |

Applied to:

- **Orders.Api** `CreateAsync` (the whole insert-order-and-outbox transaction) and `GetByIdAsync`'s PostgreSQL fallback query — both wrapped with the `postgres` pipeline; `BrokenCircuitException`/`TimeoutRejectedException` map to `503 Service Unavailable` with a `Retry-After` header instead of an unhandled 500 or an open-ended hang.
- **Orders.Api** `KafkaOrderEventPublisher.PublishAsync` — wrapped with `kafka-producer`, bounding how long the background Outbox publisher can be stuck on a single message during a Kafka outage.
- **Orders.Api** `RedisOrderCache` — wrapped with `redis`; on an infrastructure fault it does **not** fail the request. It bypasses the cache entirely and serves directly from PostgreSQL (`X-Cache: BYPASS`), recording an `orders.cache.bypassed` metric. This is graceful degradation, not fail-fast: losing Redis should never break order reads.
- **Orders.Worker** `InboxStore.TryRecordAsync` and `OrderStatusStore.TryConfirmAsync` — wrapped with `postgres`. The consumer's own retry loop (`OrderCreatedConsumer.ProcessWithRetriesAsync`) already special-cases `NpgsqlException` as an infrastructure failure (seek and retry, never dead-letter); it now treats `BrokenCircuitException`/`TimeoutRejectedException` the same way, so a Postgres outage during message processing backs off and retries indefinitely instead of eventually dead-lettering a perfectly valid message.

Every pipeline's execution, retry, timeout, and circuit-breaker-state events are emitted automatically as OpenTelemetry metrics by `Microsoft.Extensions.Resilience` (meter `Polly`, added to `AddOrdersObservability`) — no custom instrumentation needed. A new Grafana panel (`Resilience events (retries, timeouts, circuit breaker)`) graphs `resilience_polly_strategy_events_total` by pipeline and event type.

### Toxiproxy

`toxiproxy` runs in Compose (`ghcr.io/shopify/toxiproxy:2.9.0`), pre-configured via a mounted `proxies.json` with two proxies: `postgres` (`:15432` → `postgres:5432`) and `kafka` (`:19092` → `kafka:9092`). Its admin API is published on `127.0.0.1:8474`, matching the loopback-only pattern already used for Prometheus and Grafana.

Toxiproxy is deliberately **not** part of the default traffic path. `kubernetes/base/infrastructure-services.yaml` and the `postgres`/`kafka` EndpointSlices are untouched by default — every other milestone's k6 profile runs against the real backends with zero added latency. `scripts/resilience-chaos.sh <postgres|kafka> <latency|outage>` reversibly patches the target's EndpointSlice to route through Toxiproxy only for the duration of one experiment, using a `trap`-guarded cleanup that always reverts the EndpointSlice and re-enables the proxy, even on failure. Because PostgreSQL/Kafka client connections are pooled, the script also forces a `kubectl rollout restart` of both workloads immediately after re-routing (and again after reverting) so the fault is actually exercised on fresh connections rather than silently bypassed by already-open pooled sockets.

## Results

### PostgreSQL — latency (100 ms ± 20 ms jitter)

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Failed HTTP requests | 0.00% | < 5% |
| Successful checks | 100.00% | > 95% |
| Order flow success | 100.00% | > 95% |
| Create p95 | 470.8 ms | < 3,000 ms |
| Get p95 | 170.4 ms | < 3,000 ms |
| Circuit breaker tripped | No | — |

The system absorbed the added latency transparently — zero failures, no circuit trip, just proportionally higher latency. (An earlier attempt at 300 ms latency, run immediately after a cold connection-pool restart, did trip the breaker — see "What didn't work" below.)

### PostgreSQL — outage (proxy disabled)

10 sequential requests sent while PostgreSQL was fully unreachable:

| Attempt | Status | Elapsed |
| --- | --- | ---: |
| 1 | 500 | 412 ms |
| 2 | 500 | 369 ms |
| 3–10 | 503 | 316–329 ms |

The first two requests fail before the circuit breaker has enough samples to open (raw connection failures surfacing as 500). From attempt 3 onward the breaker is open and every call fails in a **consistent ~320 ms** — no request ever approached Npgsql's default multi-second connection timeout. Zero orders were created during the outage window (`SELECT count(*) FROM orders WHERE customer_id = 'chaos-outage-customer'` → `0`): the whole create-and-commit sequence is wrapped in one pipeline execution, so a failure guarantees no partial write. Full recovery confirmed by the script's post-revert functional smoke check.

### Kafka — latency (100 ms ± 20 ms jitter)

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Failed HTTP requests | 0.00% | < 5% |
| Order flow success | 100.00% | > 95% |
| Pipeline convergence | 196 / 196 orders, 0 pending Outbox | 100% |

Kafka latency has no direct effect on HTTP latency (publishing happens asynchronously via the Outbox), and the background pipeline still drained fully within the runner's timeout.

### Kafka — outage (proxy disabled)

10 sequential order creations sent while Kafka was fully unreachable:

| Attempt | Status | Elapsed |
| --- | --- | ---: |
| 1–10 | 201 | 11–14 ms |

Every single request succeeded, fast, with Kafka completely down — this is the entire point of the transactional Outbox pattern: order creation only depends on PostgreSQL. After Kafka was restored, all 10 Outbox messages published (`pending_outbox` reached `0`) and all 10 orders converged to `Confirmed` (Worker processed them and the cache was invalidated), proving the backlog fully drains once the dependency returns.

## What didn't work (and the fixes)

- An initial 300 ms Postgres latency toxic, injected immediately after a forced pool-cold-start restart, compounded with retries into failures and tripped the circuit breaker — a real but *unintended* outcome for the "latency" story (that's what the outage scenario is for). Lowered to 100 ms ± 20 ms jitter, which stays comfortably under the 2 s timeout even accounting for cold-connection overhead.
- Freshly restarted pods pay a one-time JIT/tiered-compilation warmup cost (observed as ~500–900 ms tail latency on the first dozen requests against otherwise-instant endpoints) that tripped `smoke`'s strict latency thresholds without indicating any real problem. The chaos script's pre/post-fault verification now checks functional correctness (zero failures, full pipeline convergence) via a warm-up-then-measure pattern instead of relying on `smoke`'s exit code.
- The first revert attempt used `kubectl apply` against a point-in-time backup, which conflicted with the object's `last-applied-configuration` annotation after being mutated by `kubectl patch` — silently leaving `kafka-compose` (and, it turned out, `postgres-compose` from an earlier run) pointed at Toxiproxy after the script exited. Both were caught and manually reverted; the script now reverts with the same `kubectl patch --type=json` mechanism used to inject the fault, and explicitly re-verifies the EndpointSlice address after reverting — since a passthrough Toxiproxy with no active toxic is functionally indistinguishable from the real backend, a passing smoke check alone can't prove the revert actually happened.

## Running the experiments

```bash
cd /srv/local-distributed-lab
scripts/resilience-chaos.sh postgres latency
scripts/resilience-chaos.sh postgres outage
scripts/resilience-chaos.sh kafka latency
scripts/resilience-chaos.sh kafka outage
```

Each run is fully self-contained and reverts itself; results land under `artifacts/k6/<timestamp>-chaos-<target>-<scenario>/`.
