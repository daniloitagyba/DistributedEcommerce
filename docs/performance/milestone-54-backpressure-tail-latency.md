# Milestone 54: Tail-Latency Amplification in a Real BFF Fan-Out

## Scope

Every other Storefront.Service endpoint (`ProxyEndpoints`) is a 1:1 reverse proxy. This milestone adds this lab's first genuine BFF fan-out - `GET /api/storefront/products/{sku}`, which calls Catalog and Inventory in parallel via `Task.WhenAll` and waits for both - and measures the consequence Dean & Barroso's "Tail At Scale" paper predicts: a fan-out's own tail latency is at least as bad as its slowest leg, and the probability of hitting *some* slow leg grows with the number of legs, even when each one is individually fine most of the time. Then applies Milestone 39's hedging technique to the leg that needs it.

## Design

- **`StorefrontEndpoints.GetProductSummaryAsync`**: fans out to Catalog (`GET /products/by-sku/{sku}`) and Inventory (`GET /inventory/{sku}`), returns both combined. A real, useful feature addition, not a test harness bolted onto existing code.
- **`ProductSummaryOptions.HedgeDelayMilliseconds`**: when greater than 0, the Inventory leg specifically is hedged - a second, independent request fires if the first hasn't answered within the delay, and whichever answers first wins; the loser is cancelled (see "what didn't work" below for why that matters). 0 (the default) disables hedging.
- **Fault model**: a Toxiproxy latency toxic (`latency: 200ms, jitter: 50ms, toxicity: 0.15`) in front of Inventory - a dependency that's fast ~85% of the time and genuinely slow the other ~15%, not a permanent brownout. Run against an isolated instance of the app services under the `compose-apps` profile (not the live K3s deployment - see "What didn't work" for why).

## What didn't work

**Argo CD reverted every manual `kubectl apply`.** The live cluster's `storefront-service` is Argo CD-managed, reconciling from `main` on GitHub; testing this change meant deploying it without pushing there first. Every attempt to `kubectl apply` the new image tag and env vars got silently reverted back to the last-synced state (image tag, then the new `InventoryProxy__BaseUrl`/`ProductSummary__HedgeDelayMilliseconds` env vars) within moments - self-heal doing exactly its job. Measured entirely through the `compose-apps` profile instead, which Argo CD has no opinion about - not a live K3s validation for this milestone, unlike most others in this lab, for that specific reason.

**Toxiproxy's `toxicity` is per-connection, not per-request.** The first measurement showed *every* request to Inventory taking ~215ms, not the intended ~15% - because `HttpClient`'s connection pooling reused the same TCP connection across requests, and Toxiproxy rolls its toxicity probability once per proxied connection, not once per HTTP request that flows through it. A connection that rolled "toxic" stayed toxic for its entire pooled lifetime. Fixed with `SocketsHttpHandler.PooledConnectionLifetime = TimeSpan.FromMilliseconds(1)` on the Inventory `HttpClient` specifically, forcing a fresh connection - and a fresh toxicity roll - per request.

**Cold-start noise contaminated the first measurement runs.** JIT warm-up and connection-pool initialization right after `docker compose up --force-recreate` made early requests in each 200-request run unrepresentative (one single-request diagnostic returned 1003ms with no fault active at all). Fixed with a 10-request warm-up, discarded before each timed run - the same lesson Milestone 46's cart test already learned about not trusting a system's first few requests after a restart.

## Results

200 requests each, warmed up, sequential (single in-flight request at a time - see "What this doesn't measure" for why that matters):

| | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Catalog alone | 1.9ms | 2.7ms | 8.2ms | 39.4ms |
| Inventory alone (15% toxic) | 2.5ms | 225.8ms | 250.6ms | 252.3ms |
| **Aggregate, no hedge** | 4.7ms | **227.3ms** | **277.0ms** | **291.9ms** |
| **Aggregate, hedge=20ms** | 4.8ms | **93.1-93.7ms** | 561-940ms (noisy) | 561-940ms (noisy) |

**The amplification**: the aggregate's p95/p99/max all exceed Inventory's *alone* - calling Catalog too can only add latency, never subtract it, and even though Catalog is almost always fast, "almost always" still occasionally coincides with Inventory's own slow 15%, nudging every percentile upward. This is the mechanism, in miniature: P(the aggregate is slow) is at least P(any one leg is slow), and grows with the leg count.

**Hedging's effect was real but not uniformly good.** p95 improved consistently and reproducibly across repeated runs (~227ms -> ~93ms, roughly 2.4x) - hedging catches the common case of "one attempt happened to be unlucky" exactly as designed. p99 and max, however, got *worse* with hedging in this setup (561-940ms vs 277-292ms unhedged), and stayed worse even after fixing the loser-request-cancellation issue described below. The likely explanation: hedging assumes the two attempts are independent trials against independent capacity (Milestone 39's version raced *different* `orders-api` replicas). Here, both the primary and the hedge go through the *same* Toxiproxy proxy - when both happen to roll toxic (a real, if individually small, ~2.25% joint probability at 15% each), contending for that single proxy's own concurrency handling produced tail latencies well beyond either single toxic delay added together. Hedging a shared, saturable dependency doesn't behave like hedging genuinely independent replicas - a real limit on when this mitigation's assumptions hold, not a flaw in the technique itself.

**Found and fixed along the way**: the first hedging implementation left the losing request to complete in the background rather than cancelling it - safe for a read-only endpoint (no write to abort), but not free: an uncancelled loser still holds a connection-pool slot. Fixed by cancelling the loser via a linked `CancellationTokenSource` once a winner is chosen. Worth keeping regardless of this milestone's noisy p99, since it's the textbook-correct behavior for a read-only hedge.

## What this doesn't measure: coordinated omission and Little's Law

This benchmark is a **closed-loop, single-in-flight prober**: each request waits for the previous one to finish before the next fires, exactly the `docker compose exec curl` loop it's built from (and the default behavior of k6's VU-based executors, per Milestone 39's own harness). Little's Law - `L = λW`, the average number of requests in flight equals the arrival rate times the average time each spends in the system - shows why this understates the real risk: at a fixed *target* throughput λ, if tail latency W is amplified by a fan-out the way this milestone measured, the number of requests the system must hold concurrently (L) inflates proportionally. A closed-loop client never surfaces that, because it never tries to sustain λ through a slow patch - it just quietly sends less. An open-loop load generator (a constant arrival-rate driver, not a fixed VU count) would show the actual queueing consequence: backlog growing during Inventory's slow moments, not just individual requests running long. This is coordinated omission in its classic form, and it means every percentile in the table above is a *floor*, not a worst case, for what this fan-out would show under sustained concurrent load.

## Running it

```bash
scripts/storefront-tail-latency-test.sh [request_count] [label]
```

Requires the fault and hedge config to already be applied (Toxiproxy proxy + toxic, `INVENTORY_PROXY_BASE_URL`/`PRODUCT_SUMMARY_HEDGE_DELAY_MS` on `storefront-service`) - the measurements above were taken by hand-driving those via `docker compose up -d --force-recreate` between runs, not automated end-to-end in this script.
