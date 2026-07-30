# Milestone 44: Bestsellers Projection via Redis Sorted Sets

## Scope

This is the payoff for a design decision made all the way back in Milestone 40. When the e-commerce expansion was first proposed, the plan called for a home page listing products by "most sold," and the natural first instinct was to build that off MongoDB - it was already the catalog's store. It's the wrong tool for this specific job: "most sold" is an ordered, frequently incremented, top-N read - exactly what a Redis sorted set (`ZINCRBY`/`ZREVRANGE`) is built for, not a `$group`/`$sort` aggregation pipeline re-run against a document store on every read. This milestone builds that instead, closing the loop on that earlier design call.

## Design

- **Write side**: `OrderSagaReplyConsumer` (Orders.Worker), on a genuinely confirmed saga - Milestone 43's `CommitInventory` step succeeding, not just a payment approval - calls `RedisBestsellersStore.RecordSaleAsync`, which does `ZINCRBY bestsellers:global {quantity} {sku}` and `ZINCRBY bestsellers:category:{slug} {quantity} {sku}`. Incrementing rather than setting matters: a SKU sold across many separate orders accumulates its running total in the sorted set's score, which is exactly what `ZINCRBY` gives for free and a naive `ZADD` would silently overwrite.
- **The category lookup is a new synchronous call from a background worker.** Orders.Worker only knows `Sku` and `Quantity` from the saga; it doesn't own product-category data and has no reason to duplicate it. It calls Catalog.Service's `GET /products/by-sku/{sku}` - the same endpoint Milestone 42 added for Cart.Service's price snapshotting - to resolve the category at the moment of confirmation. Same idiom as M42: `AddStandardResilienceHandler()` on a scoped `HttpClient`, not the hand-rolled Postgres/Kafka/Redis pipelines.
- **Best-effort, deliberately.** Ranking a sale is an analytics side-effect of a saga outcome, not part of the outcome itself. `RecordSaleBestEffortAsync` wraps both the Catalog lookup and the Redis write in one try/catch that only logs on failure - a Catalog or Redis outage at exactly the wrong moment means one sale doesn't count toward the rankings yet, never a failed, retried, or reprocessed saga completion. This mirrors the graceful-degrade philosophy already established for `RedisOrderCache` (Milestone 9) and Catalog's own readiness check (Milestone 42): a non-critical dependency failing shouldn't make a critical path fail with it.
- **Read side**: Catalog.Service gains `GET /products/bestsellers?category=&limit=`. `BestsellersReader.GetTopAsync` runs `ZREVRANGE` (via `SortedSetRangeByScoreWithScoresAsync`, descending) against either `bestsellers:global` or the category-scoped set, returning ranked `(Sku, UnitsSold)` pairs; the endpoint then resolves each Sku's full product document from MongoDB (`ProductRepository.FindBySkuAsync`) and reassembles the response in the rank order Redis gave, not whatever order MongoDB would return them in. Two stores, two jobs: Redis says who's winning right now, MongoDB says what that product actually is.
- **Shared key shapes live in `BuildingBlocks/BestsellersKeys.cs`**, not duplicated in each service - the same reasoning `OrderCacheKeys` already established: a writer (Orders.Worker) and a reader (Catalog.Service) that don't share code must still agree byte-for-byte on the key format, so that agreement is centralized rather than trusted to stay in sync by convention.

## Live results

Eight approvable orders (49.90 BRL, under the decline threshold) created through the real, Keycloak-authenticated `POST /orders` path, each deterministically hashed (Milestone 43's `SagaSkuMapper`) to one of the nine seeded SKUs:

```
redis-cli ZREVRANGE bestsellers:global 0 -1 WITHSCORES
SKU-BOOK-001  3
SKU-HOME-002  1
SKU-HOME-001  1
SKU-CLTH-002  1
SKU-CLTH-001  1
SKU-BOOK-002  1
```

Totals to 8, matching the 8 orders - three of them independently hashed to `SKU-BOOK-001`, and its score correctly accumulated to 3 rather than being overwritten by the last write.

`GET /products/bestsellers?limit=5` returns the same ranking with full product data attached (name, price, attributes, images from MongoDB), `SKU-BOOK-001` first with `unitsSold: 3`. `GET /products/bestsellers?category=books&limit=5` correctly scopes to just the two book SKUs, still ranked `SKU-BOOK-001` (3) ahead of `SKU-BOOK-002` (1) - proving the category-scoped sorted set is a genuinely independent structure from the global one, not a filtered view of it.

**Unit/integration tests**: `RedisBestsellersStoreTests` (Orders.IntegrationTests, real Testcontainers Redis) - accumulation across multiple sales of the same SKU, and that a `null` category correctly skips the category-scoped write rather than writing to a malformed key. `BestsellersReaderTests` (Catalog.IntegrationTests, real Testcontainers Redis) - descending rank order, category-set isolation from the global set, and `limit` enforcement.

**Regression check**: `scripts/k6-run.sh smoke` post-deploy - `failed_rate=0`, `checks_rate=1`, `flow_rate=1`. The new Catalog→Redis dependency and Orders.Worker→Catalog synchronous call have no effect on the existing orders pipeline.

## Running it

```bash
curl "http://<catalog-service-clusterip>/products/bestsellers?limit=10"
curl "http://<catalog-service-clusterip>/products/bestsellers?category=electronics&limit=5"
```
