# Milestone 39: Hedged Requests for GET /orders/{id}

## Scope

The last Tier A item, and the "Tail at Scale" pattern (Dean & Barroso): rather than waiting indefinitely for one backend to respond, fire a request to a replica, and if it hasn't answered within a short delay, *also* fire the same request to a different replica - take whichever answers first. A tail-latency mitigation, not a throughput or availability one; this milestone measures its real effect, not just implements it.

## Where hedging belongs

There's no internal service in this lab that calls `GET /orders/{id}` today - `Orders.Worker` and `Payments.Service` both go straight to Postgres, never back through `orders-api`'s HTTP surface. Hedging is fundamentally a *client-side* decision (this is also how it's used in the real world: gRPC's own client-side `hedgingPolicy`, Google's internal RPC hedging, etc. - a caller races replicas, not a server racing itself). With no natural internal caller to add it to, this milestone builds and measures it at the client that actually exists: the k6 load-testing harness (`load-tests/k6/orders.js`), extended with a `hedged` profile and a real racing implementation, not a synthetic double-request.

## Design

- **Direct pod IPs, not the Service ClusterIP.** Hedging needs to race two *different* backend replicas. A single client connection through a K8s Service VIP is load-balanced by kube-proxy at the TCP-connection level, not per-request - it can't be steered to hit two specific, different pods on purpose. `scripts/hedged-read-benchmark.sh` discovers the live `orders-api` pod IPs via `kubectl get pods -o jsonpath` and passes them to k6.
- **Real hedging, not "always send two".** `k6/timers`' `setTimeout` races against the primary request's own `Promise` (via `k6/http`'s `asyncRequest`, k6's native async HTTP API): if the primary answers before the hedge delay elapses, the hedge is never sent at all. Only when the timer wins does a second request go out - to a *different* replica than the primary, since hedging the same already-slow backend wouldn't help.
- **Freshly-created orders for both strategies.** A brand-new order's first read is a guaranteed cache miss (Milestone 9's cache-aside is only populated by a read, never by the create path itself) - giving both the hedged and unhedged strategy a real Postgres-backed read with genuine latency variance, instead of a sub-millisecond Redis hit with no tail to hedge against.

## What didn't work: the benchmark tripped Milestone 38's own rate limiter

The first run of the `hedged` profile (10 VUs, 30s, ~950 total HTTP calls) failed with a 52.5% request failure rate - not a hedging bug, but Milestone 38's cluster-wide distributed rate limiter (150 requests/10s on `/orders`, deployed two milestones before this one) correctly doing its job against a benchmark that hadn't accounted for it. Isolating the cause took three targeted debug scripts (plain concurrent `http.asyncRequest`, `Promise.race` against `k6/timers`, and finally the exact hedging pattern) that each ran clean with zero failures - proving the mechanism itself was sound and the fault was in the *volume* of traffic the profile generated, not the racing logic. The fix: reduce the profile from 10 VUs to 2 and lengthen the per-iteration sleep, keeping total throughput comfortably under the 150/10s cap the lab already committed to elsewhere. A real cross-milestone interaction, left in the profile's own comment as a reminder for the next person who wants to crank the VU count back up.

## Results

Two independent runs against the live K3s cluster, hedge delay 20ms:

**Run 1:**
```
Unhedged GET /orders/:id  - p50=3ms  p95=79.5ms   p99=811.7ms  max=877ms
Hedged GET /orders/:id    - p50=3ms  p95=18.5ms   p99=23.9ms   max=179ms
Hedge fired rate: 5.4%   (6 of 111 reads had a primary slower than 20ms)
Hedge won rate (of fired hedges): 83.3%
```

**Run 2:**
```
Unhedged GET /orders/:id  - p50=3ms  p95=5ms   p99=816.7ms  max=1937ms
Hedged GET /orders/:id    - p50=3ms  p95=5ms   p99=5ms      max=23ms
Hedge fired rate: 0.9%   (1 of 113 reads had a primary slower than 20ms)
Hedge won rate (of fired hedges): 100%
```

p50 is identical in both runs, exactly as expected - hedging only ever affects the tail, since the hedge simply never fires when the primary is already fast. The tail is where it matters: p99 improved 34x and 163x across the two runs, and worst-case max latency improved 5-84x. The hedge itself fires rarely (well under 10% of reads) - most reads are fast enough that the 20ms timer never wins the race - but on the rare occasions a replica is having a slow moment, hedging almost always (83-100%) successfully routes around it rather than making the client wait it out.

### Regression check

`scripts/k6-run.sh smoke` post-change: `failed_rate=0`, `checks_rate=1`, `flow_rate=1` - the new `hedged` profile, `k6/timers` import, and async default-function change didn't affect any of the existing profiles.

## Running it

```bash
scripts/hedged-read-benchmark.sh [hedge_delay_ms]   # default 20ms
```
