# Milestone 42: Cart Service with Redis as the System of Record

## Scope

`Cart.Service` owns the shopping cart. The interesting part isn't the CRUD - it's the persistence choice: Redis holds the cart data **directly**, not as a cache in front of something durable. Every prior use of Redis in this lab was in service of a source of truth living somewhere else - Milestone 9's cache-aside sits in front of Postgres, Milestone 38's sliding-window counter sits in front of nothing durable but is itself disposable derived state (rate-limit counts, not business data). A cart is the first genuinely durable-*feeling* business entity in this lab with no database behind it at all.

## Why Redis-as-primary is the right call here, and why it wouldn't be for orders

A cart is ephemeral, high-write, and low-consequence-if-lost: if Redis restarts and a shopper's cart disappears, the fix is "the shopper re-adds a few items," not "an order silently vanished." That's a fundamentally different risk profile from `orders`/`payments`/`inventory`, all of which use Postgres specifically because losing that data is a real incident, not an inconvenience. Cart data is also naturally TTL-shaped - abandoned carts should just disappear, which is exactly what a Redis key's `EXPIRE` does for free, versus a Postgres table needing an explicit cleanup job. This is the polyglot-persistence theme from Milestone 40 continuing: pick the store whose native behavior matches the data's actual durability and shape requirements, rather than defaulting to Postgres for everything.

## Design

- **One Redis Hash per cart** (`cart:{cartId}`), field = SKU, value = JSON-encoded `CartLineItem`. This lets the whole cart be read or have its TTL refreshed in a single round trip, and lets a single SKU be added/updated/removed with a single `HSET`/`HDEL` - no read-modify-write race on the *whole* cart is needed for a single-item mutation, since hash fields are independently addressable.
- **Sliding TTL, refreshed on every write** (`CartOptions.TimeToLiveSeconds`, default 1800s / 30 minutes). There is no separate cleanup job for abandoned carts - expiry *is* the deletion mechanism. `GetAsync` on an unknown or expired cart returns an empty list rather than a 404: carts are implicit, created lazily on first add, matching ordinary cart UX (no "create a cart" step a client has to remember to call first).
- **Price snapshot on first add only.** `CartLineItem.UnitPrice`/`ProductName`/`Currency` are fetched from Catalog.Service once, when a SKU is first added to a given cart, and never re-fetched on a bare quantity change. This is a deliberate e-commerce convention (the price you saw when you added the item is the price in your cart, not whatever the catalog says a minute later) - checkout, not the cart, is where prices get revalidated against the live catalog.
- **This lab's first synchronous inter-service HTTP call.** Every inter-service call built so far - Payments reacting to `OrderCreated`, Inventory's `InventoryReservationRequested`/`Replied`, the M22 orchestrated saga - is async over Kafka. Adding a new SKU needs the catalog's current name/price *right now*, synchronously, to snapshot it; there's no sensible way to model that as a fire-and-forget event. `CatalogClient` calls the new `GET /products/by-sku/{sku}` endpoint (added to Catalog.Service this milestone - Catalog previously only supported lookup by Mongo ObjectId or category) via `HttpClient` + `Microsoft.Extensions.Http.Resilience`'s `AddStandardResilienceHandler()`, not the hand-rolled `ResiliencePipelineProvider` this codebase uses for Postgres/Kafka/Redis. That provider predates the dedicated HTTP resilience package; the dedicated package wraps the identical Polly v8 engine specifically for `HttpClient` via `DelegatingHandler`s, so this is the idiomatic choice for HTTP, not an inconsistency.
- **Readiness reflects only Redis, not Catalog.** Cart.Service's core function (read/update-quantity/remove/clear an existing line item) doesn't need Catalog at all - only adding a brand-new SKU does. Making Catalog reachability part of `/health/ready` would make the whole service falsely unready during a transient Catalog blip that only affects one code path. A Catalog failure surfaces as a per-request error on that one path instead.

## Live results

Full lifecycle exercised against the live ClusterIP:

- `PUT /carts/{id}/items/SKU-ELEC-001 {quantity:2}` and `.../SKU-BOOK-001 {quantity:1}` - both resolved real Catalog data (`4299.90 BRL` / `89.90 BRL`), cart total correctly `8689.70`.
- **Redis inspection confirms the actual storage shape**: `redis-cli HGETALL cart:<id>` returns exactly the two JSON-encoded line items as hash fields; `TTL cart:<id>` returns a live countdown (~1788s of the 1800s window).
- **Statelessness proven, not assumed**: killed a `cart-service` pod mid-test, cart was still fully readable through the Service immediately after - the data was never in that pod's memory to lose.
- Quantity update (`2` → `5` on `SKU-ELEC-001`) preserved the original `addedAt` timestamp and did not re-call Catalog, confirming the "snapshot once" design actually behaves as designed rather than just being stated as intent.
- `quantity: 0` correctly rejected with `400` and a clear validation message directing the caller to `DELETE` instead.
- Unknown SKU correctly rejected with `404` and a clear message, without ever touching Redis.
- `DELETE /carts/{id}/items/{sku}` removed one line item and left the rest and the total correctly recalculated; `DELETE /carts/{id}` cleared the whole cart (`items: [], total: 0, expiresInSeconds: null` - the key is gone, not just empty).
- **Unit/integration tests**: 4/4 passing against real Testcontainers Redis (upsert+TTL, same-SKU overwrite not duplication, remove-one-vs-clear-all, unknown cart returns empty rather than throwing). Catalog's new `FindBySkuAsync`/`GET /products/by-sku/{sku}` also covered (found + not-found) against real Testcontainers MongoDB.
- **Regression check**: `scripts/k6-run.sh smoke` post-deploy - `failed_rate=0`, `checks_rate=1`, `flow_rate=1`. Cart.Service's presence (2 pods, a new synchronous dependency on Catalog.Service) has no effect on the existing orders pipeline.

## Running it

```bash
curl -X PUT http://<cart-service-clusterip>/carts/<cart-id>/items/<sku> -d '{"quantity":2}'
curl http://<cart-service-clusterip>/carts/<cart-id>
```
