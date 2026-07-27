# Milestone 9 Redis Cache

## Scope

This milestone adds a Redis cache-aside layer in front of `GET /orders/{id}` and demonstrates cross-service cache invalidation: the Orders API populates the cache on read, and the Orders Worker invalidates it after processing the corresponding `OrderCreated` event, once it transitions the order from `Created` to `Confirmed`.

PostgreSQL remains the system of record. Redis only ever serves as a read-through cache with a bounded time-to-live; losing it does not lose data, only cached responses.

## Design

- **Cache-aside**: `GET /orders/{id}` first checks Redis (`orders:cache:{id}`). On a miss, it takes a short distributed lock (`orders:cache-lock:{id}`, `LockTakeAsync`/`LockReleaseAsync`) before querying PostgreSQL, to avoid a cache stampede when many concurrent requests miss at once. Callers that lose the lock race poll the cache briefly before falling through to an uncached read, so latency stays bounded even under contention.
- **TTL**: 30 seconds by default (`Cache:TimeToLiveSeconds`).
- **Invalidation**: after the Worker records an event in the Inbox, it updates the order's status to `Confirmed` in PostgreSQL and deletes the Redis key, so the next read reflects the new status instead of a stale cached `Created`.
- **Observability**: `orders.cache.hits` / `orders.cache.misses` counters feed a hit-ratio panel and a hits-vs-misses panel on the `Orders Lab Overview` Grafana dashboard. The API also returns an `X-Cache: HIT|MISS` response header for manual inspection.
- **Topology**: Redis runs in Docker Compose (no host port, no authentication — same trust model as Kafka) and is reachable from K3s through the existing selectorless-Service/EndpointSlice bridge pattern used for PostgreSQL and Kafka.

## Cache experiment

The `cache` k6 profile seeds a pool of 15 orders, waits for the Worker to converge, then runs 10 VUs for 30 seconds performing only `GET /orders/{id}` reads against random IDs from the pool.

Final result:

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Cache hit ratio | 98.46% | > 90% |
| Cached read p95 | 1.77 ms | < 100 ms |
| Cached read p99 | 152.72 ms | < 250 ms |
| Failed HTTP requests | 0.00% | < 1% |
| Successful checks | 100.00% | > 99% |
| Iterations | 979 | Informational |
| Orders seeded / processed | 15 / 15 | 100% |
| Pending Outbox messages | 0 | 0 |

For comparison, the `smoke` profile (unchanged: creates and reads a fresh, never-cached order every iteration) measured a `GET /orders/{id}` p95 of 388 ms and p99 of 594 ms — every read there is a guaranteed cache miss followed by a real PostgreSQL query plus the Redis round trip, versus the 1.77 ms p95 for cached reads above.

## Invalidation verification

The Worker processes events fast enough (well under a second, end to end through Kafka) that a plain manual sequence usually already observes `Confirmed` on the very first read. To deterministically observe the `Created -> Confirmed` transition, the Worker was scaled to zero replicas before creating the order, then scaled back to one:

1. With `orders-worker` scaled to 0, `POST /orders` returns `201` with the new order ID.
2. First `GET /orders/{id}` returns `X-Cache: MISS` and `"status":"Created"`.
3. Second `GET /orders/{id}` returns `X-Cache: HIT` with the same `Created` status — proven cached, since PostgreSQL was never touched again.
4. `orders-worker` is scaled back to 1 and processes the queued event (Inbox insert, status update to `Confirmed`, Redis key delete).
5. The next `GET /orders/{id}` returns `X-Cache: MISS` and `"status":"Confirmed"` — the invalidated key forced a fresh PostgreSQL read.
6. A further `GET` returns `X-Cache: HIT` again with `Confirmed`, proving the cache repopulated with the fresh value.

All six steps were observed exactly as expected against the live K3s deployment.
