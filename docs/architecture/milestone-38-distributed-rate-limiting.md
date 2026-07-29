# Milestone 38: Cluster-Wide Distributed Rate Limiting

## Scope

Milestone 11 added per-pod token-bucket rate limiting to `/orders` - real, working load shedding, but scoped to a single process's memory. With `orders-api` running 3 replicas (and autoscaling 3-5 via HPA), the *actual* cluster-wide ceiling is `replica count × per-pod limit`, not the number in `RateLimit__TokenLimit` - and it silently shifts every time the Rollout scales. This milestone adds the missing distributed dimension: a limit that means the same thing regardless of how many pods are running.

## Design

- **`RedisSlidingWindowRateLimiter`** (`Orders.Infrastructure/RateLimiting`): a single Redis sorted set per rate-limit key, `member = <request GUID>`, `score = <request timestamp in ms>`. One atomic Lua `EVAL` per request does `ZREMRANGEBYSCORE` (drop entries older than the window), `ZCARD` (count what's left), and conditionally `ZADD` + `PEXPIRE` (admit the request) - all as a single round-trip, avoiding any check-then-act race between concurrent replicas.
- **Sliding-window-log, not a fixed-window counter, deliberately.** A naive fixed window (e.g. "reset the counter every 10s on the clock") allows up to 2x the configured limit right at a window boundary (a burst just before the reset, another just after). Tracking exact request timestamps in a sorted set avoids that at the cost of one small sorted-set entry per admitted request instead of a single integer - a real, worthwhile tradeoff for correctness.
- **A second, independent layer, not a replacement.** `DistributedRateLimitingMiddleware` runs immediately after `app.UseRateLimiter()` (Milestone 11's per-pod limiter) in the pipeline - a request has to clear the cheap local check first, and only then costs a Redis round-trip. Scoped to `/orders` only, matching `RateLimitingExtensions.OrdersPolicy`'s existing endpoint group.
- **Fails open on Redis unavailability**, matching `RedisOrderCache`/`RedisIdempotencyStore`'s established philosophy in this codebase: the distributed limiter is defense-in-depth on top of the always-available local limiter, not the sole protection - a Redis outage degrades to "only the per-pod limit applies" rather than failing every request.

## Results

Fired 220 concurrent `POST /orders` requests directly at the orders-api `Service` ClusterIP (bypassing per-pod load balancing quirks) against the configured cluster-wide limit of 150 requests / 10s:

```
=== status code distribution ===
    150 201
     70 429
```

Exactly 150 succeeded - the configured limit, not `3 × 80` (240, what Milestone 11's per-pod limiter alone would have allowed if perfectly load-balanced) or any other replica-count-dependent number. All 70 rejections carried the distinguishing detail message, confirming the *distributed* limiter (not the per-pod one) made the call:

```json
{"title":"Too Many Requests","status":429,"detail":"The orders API's cluster-wide rate limit is shedding load; retry after the indicated delay."}
```

**Proof the counter is genuinely shared, not per-pod**: correlating each response's `X-Instance-ID` header with its `X-RateLimit-Distributed-Count` header shows one monotonic sequence interleaved across all three replicas - not three independent counters each reaching their own local limit:

```
orders-api-...-ck6b2,1
orders-api-...-ck6b2,2
orders-api-...-db4jf,3
orders-api-...-db4jf,4
...
orders-api-...-ck6b2,11
orders-api-...-ck6b2,12
orders-api-...-db4jf,13
...
orders-api-...-ck6b2,148
orders-api-...-ck6b2,149
orders-api-...-49nxh,150
```

Requests landing on `ck6b2`, `db4jf`, and `49nxh` all draw from the same shared count, and it stops admitting at exactly 150 regardless of which pod happens to receive the 151st request.

### Regression check

`dotnet test`: 28 unit tests unchanged, 3 new integration tests (`RedisSlidingWindowRateLimiterTests`, against a real Testcontainers Redis) covering admit-up-to-the-limit-then-reject, independent keys, and real window-sliding-past recovery (using an actual `Task.Delay`, not a mocked clock) - all passing. `scripts/k6-run.sh smoke` post-deploy: `failed_rate=0`, `checks_rate=1`, `flow_rate=1` - the configured limit (150/10s) sits well above smoke's traffic volume, so it doesn't interfere with routine validation.

## Running it

```bash
# Check current config
kubectl get rollout orders-api -n orders-lab -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' | grep DistributedRateLimit

# Response headers on every /orders request show the live shared count
curl -sD - -X POST http://<orders-api>/orders ... | grep -i x-ratelimit-distributed
```
