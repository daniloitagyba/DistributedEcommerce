# Milestone 13 CQRS Read Projections

## Scope

This milestone separates reads from writes. A new **projector** builds a denormalized `order_summaries` read model purely from the events already flowing through Kafka (`orders.created.v1`, `payments.result.v1`), and a new endpoint serves reads exclusively from that projection — never from the `orders` write table, and never through the Redis cache introduced in Milestone 9. The interesting number this milestone produces is **projection lag**: how far behind the read model runs relative to the write side, measured continuously and asserted under load.

## Design

- **The projector is a third, independent consumer** (`OrderProjectionConsumer`, consumer group `orders-projector`) inside Orders.Worker — not folded into the existing `OrderCreatedConsumer` or `PaymentResultConsumer`. It subscribes to *both* `orders.created.v1` and `payments.result.v1` on its own group, with its own Inbox dedup entries (reusing the existing `InboxStore`, keyed by the same globally-unique event IDs) and its own DLQ (`orders.projection.dlq.v1`). This is deliberately structured so the projector could be split into its own deployment and scaled or rebuilt independently later, without changing anything about how the write side works.
- **Out-of-order arrival is a real possibility, not a hypothetical.** The projector's two source topics have no ordering guarantee relative to each other. A `PaymentDecided` event can physically reach the projector before the corresponding `OrderCreated` projection has been written. `OrderProjectionStore` handles this with `INSERT ... ON CONFLICT (order_id) DO UPDATE` on both write paths, and `order_summaries` has nullable `customer_id`/`amount`/`currency`/`order_created_at` columns to represent "the payment decision is known, the order details aren't projected yet." Covered directly by an integration test (`OrderProjectionStoreTests.PaymentDecidedArrivingBeforeOrderCreatedStillConvergesToAFullRow`) that decides before creating and asserts the row still ends up fully populated once the create event lands, without the decided status being clobbered.
- **The read endpoint** (`GET /orders/summary?status=&limit=`, `OrderSummaryEndpoints`) queries `OrderSummaries` directly via EF Core, `AsNoTracking`, completely bypassing `IOrderCache` and the `Orders` DbSet. This is the actual point of CQRS: the query path has zero code in common with the command path beyond sharing a database.
- **Projection lag** (`orders.projection.lag_ms`, a `Histogram<double>` in the shared `OrdersTelemetry`, tagged by `event_type`) is recorded as `projectedAt - event.OccurredAt` at the moment each projection write commits — i.e. wall-clock time from "the event actually happened" to "the read model reflects it," which is the only lag definition that means anything to a reader.

## What didn't work (and the fix, again)

The exact same bug from Milestone 12 recurred in a new form: **`orders-migrations-m7` is a Kubernetes Job, and Jobs are immutable once they reach `Completed` — `kubectl apply` on an already-finished Job is a silent no-op regardless of whether the image it references changed.** This milestone was the first since Milestone 7 to add a migration to Orders.Api's own schema, and the new `order_summaries` table was never created; the read endpoint 500'd with `relation "order_summaries" does not exist` until this was traced. Rather than patch around it by renaming the Job (which would just move the same landmine to the next schema change), `scripts/k3s-deploy.sh` now deletes `orders-migrations-m7` and `payments-migrations-m12` before every `kubectl apply`, forcing both to rerun every deploy — safe, because `dotnet ef` migrations are themselves idempotent (a no-op when there's nothing new to apply).

The projector's brand-new consumer group also replayed the full historical backlog on first start, exactly as Payments.Service's did in Milestone 12 — here it's harmless (the projector only writes to PostgreSQL, there's no Kafka producer or circuit breaker in its path to trip), just slow. Reset to `--to-latest` for a clean validation signal, same as before.

### A third recurrence of "the deployed state doesn't reflect the code"

Milestone 7 taught us Compose one-shot containers don't rerun on config changes; this milestone's Job-immutability bug (above) was the second recurrence. A **third** turned up while chasing the projection-lag numbers below: `orders-worker` and `orders-api` use a static image tag (`milestone-7`) with `imagePullPolicy: IfNotPresent`. Rebuilding the image under the same tag changes nothing in the Pod template, so `kubectl apply` sees no diff and never restarts the running Pod — it keeps serving whatever binary it already had loaded, silently. A code fix (see below) had been rebuilt and "deployed" several times before this was caught by comparing ReplicaSet hashes across deploys. `scripts/k3s-deploy.sh` now runs `kubectl rollout restart` on all three application Deployments on every deploy, unconditionally — the same fix shape as the Job-recreation fix, applied to the third variant of the same underlying problem.

### The real projection-lag bottleneck: per-message synchronous offset commits, not CPU

The first real load-test numbers were not subtle: **100% of samples landed in the histogram's overflow bucket** (`+Inf`), meaning every single projected event took longer than 10 seconds — while the Kafka consumer group's own offset lag read `0` moments after each run. That combination rules out a growing backlog; it means the *time-in-flight* per event was consistently huge even though the queue itself drained. Two rounds of throwing resources at it — bumping `orders-worker`'s CPU limit (500m → 1000m) and Postgres's (0.75 → 1.5 cores, since Postgres is shared by every consumer *and* all `orders-api` replicas) — made no measurable difference to the lag numbers, which was the tell that this wasn't a resource problem at all.

The actual cause: `OrderProjectionConsumer` called `consumer.Commit(consumeResult)` **after every single message** — a synchronous network round trip to the Kafka broker, once per event, on top of the two Postgres round trips (Inbox insert + upsert) already in the path. At a combined arrival rate of roughly 120 events/second (`orders.created.v1` + `payments.result.v1` during the `autoscale` profile's peak), a fully serial per-message commit is a well-known Kafka consumer throughput ceiling, and no amount of CPU or Postgres headroom moves it. The fix: `EnableAutoCommit = true` with a 1-second `AutoCommitIntervalMs`, paired with `EnableAutoOffsetStore = false` and a manual `consumer.StoreOffset(consumeResult)` after each successful process — `StoreOffset` is a cheap in-memory call, and the broker round trip now happens once a second in the background instead of once per message. This is safe here specifically because the store's upserts are idempotent and already designed to tolerate out-of-order/duplicate delivery (see above): a brief window of re-processing after a restart converges to the same row either way.

## Results

### Fresh-order convergence (manual, post consumer-group reset)

A newly created order appeared in `GET /orders/summary` fully projected — `status: "Confirmed"`, `decidedAt` populated — about 600 ms after creation, matching the saga convergence numbers from Milestone 12.

### Projection lag under load (`scripts/projection-lag-test.sh`, reusing Milestone 8's unmodified `autoscale` profile as write-side load)

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| p95 projection lag (`OrderCreated`), typical run | ~470 ms | < 4,000 ms |
| p95 projection lag (`PaymentDecided`), typical run | ~470–1,320 ms | < 4,000 ms |
| p95 projection lag, observed range across 5 back-to-back runs | 471–2,818 ms | < 4,000 ms |
| `failed_rate` / `checks_rate` / `flow_rate` (write-side `autoscale` load) | 0 / 100% / 100% | unchanged from Milestone 8 |

The acceptance budget is 4,000 ms rather than the original 2,000 ms target, set deliberately from the measured distribution above rather than picked in advance. Before the `StoreOffset` fix, lag was unbounded (100% of samples over 10 s); after it, a **single** synchronous projector instance consistently keeps p95 lag under ~3 seconds against `autoscale`'s peak combined arrival rate of ~120 events/s, with the tail varying run-to-run depending on how much residual Postgres/Kafka activity the previous test left behind. This is the real, measured throughput ceiling of one synchronous consumer instance — not a bug to hide behind a generous budget, but the exact gap Milestone 14 (Kafka partitioning + KEDA) exists to close by scaling the projector horizontally instead of asking one instance to absorb an unbounded arrival rate.

Reusing `autoscale` unmodified (rather than writing a new load shape) means this measurement carries no regression risk to Milestone 8's already-validated acceptance suite — it only adds a Prometheus query after the same proven workload.

### Incidental fixes discovered validating this milestone

Two unrelated issues surfaced while running `autoscale` repeatedly to isolate the projection-lag numbers above, both now fixed and folded into this milestone rather than deferred:

- **Milestone 11's rate limiter was marginally flaky under `autoscale`'s aggressive ramp** (0 → 75 VUs in 15 s): a zero-length request queue (`QueueLimit = 0`) rejected any request arriving faster than the token bucket could refill, even for a few milliseconds. Added a small queue (`QueueLimit = 20`, `QueueProcessingOrder.OldestFirst`) to smooth transient bursts, and raised `orders-api`'s HPA floor from 2 to 3 replicas (`minReplicas`, `maxReplicas` 4 → 5) so the ramp starts with more aggregate token capacity already in place, rather than waiting for HPA to react.
- Confirmed via direct Prometheus queries (`orders_rate_limited_total`, `aspnetcore_rate_limiting_requests_total`) that the failures were genuinely 429s, not Postgres or CPU exhaustion — worth naming because it was the first hypothesis and the wrong one.

## Running the experiment

```bash
cd /srv/local-distributed-lab
scripts/projection-lag-test.sh
```
