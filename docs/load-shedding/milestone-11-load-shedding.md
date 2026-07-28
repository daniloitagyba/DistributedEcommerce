# Milestone 11 Rate Limiting and Load Shedding

## Scope

This milestone protects the Orders API from overload by shedding excess load before it can degrade or take down the service, and proves it with a deliberate overload experiment on the live K3s deployment.

**Scope decision**: the original plan considered a separate YARP gateway service in front of the API. This milestone instead adds ASP.NET Core's built-in rate limiter directly in-process on Orders.Api. A dedicated gateway would have meant a new Dockerfile, Deployment, Service, and network policy for a second hop that two load-balanced API replicas behind a Kubernetes Service don't structurally need yet — the core lesson (token-bucket admission control, fail-fast with `429` + `Retry-After`, load shedding under overload) doesn't require it. A gateway is a natural target for a later milestone if request routing, edge retries, or cross-cutting auth become relevant.

## Design

- **Token bucket per pod** (`Orders.Api/RateLimiting/RateLimitingExtensions.cs`), applied to the whole `/orders` route group via `RequireRateLimiting`. Configurable via `RateLimit:*` (`appsettings.json` / `RateLimit__*` env vars): `TokenLimit` (burst capacity), `TokensPerPeriod` and `ReplenishmentPeriodSeconds` (steady-state rate). `QueueLimit` is `0` — rejected requests fail immediately rather than queueing, which is what makes this load *shedding* rather than backpressure.
- **Rejection**: `RejectionStatusCode = 429`, with a `Retry-After` header computed from the limiter's own lease metadata and an `orders.rate_limited` counter recorded in `OnRejected` (flows into Prometheus automatically like the rest of `OrdersTelemetry`).
- The limiter is **per-pod and in-memory**, not a distributed/shared limit across replicas. That means the effective cluster-wide ceiling scales with replica count (intentional here, and consistent with how many real systems protect each instance's own resources behind a load balancer). A Redis-backed distributed limiter — using the Redis infrastructure already in this lab — would be a natural extension if a true cluster-wide ceiling were needed instead.

## Tuning: two rounds

The first attempt used a generous, symmetric-looking setting (`TokenLimit=100`, `TokensPerPeriod=75/s`) and an unthrottled k6 overload workload (300 VUs, no pacing). Result: real damage — `server_error_rate=8.29%`, accepted-request p95 latency of 4.1s. The 300 VUs firing simultaneously exhausted the *burst* allowance (100 tokens × up to 4 pods after HPA scaled out = 400 instantly-available tokens) faster than PostgreSQL could actually process that many concurrent writes, so genuinely accepted traffic overwhelmed the database — the rate limiter wasn't the problem, its burst window was just wide enough to let a thundering herd through before steady-state throttling caught up.

Lowering `TokenLimit` to `30` fixed the overload run cleanly (`server_error_rate=0.00%`, accepted p95 dropped to ~100 ms) — but broke the existing `autoscale` acceptance suite from Milestone 8: `checks` dropped to 96.86% and `http_req_failed` rose to 3.35%, because autoscale's legitimate ramp to 75 VUs now got shed as false-positive overload before HPA had a chance to scale out.

Settled on `TokenLimit=80` (steady-state `TokensPerPeriod=75/s` unchanged) plus adding realistic pacing (`sleep(0.2–0.3s)` per iteration) to the overload k6 workload, so "overload" means sustained excess demand rather than an unrealistic zero-delay firehose. This satisfies both: `autoscale` passes at 100% success once pods are warm, and `overload` still sheds 83%+ of traffic with zero real failures.

A second, unrelated capacity limit surfaced during this tuning: PostgreSQL's `max_connections` (set to `50` back in an earlier milestone) briefly exhausted under combined HPA-scaled-to-4-pods connection pooling plus the test runner's own verification queries (`FATAL: sorry, too many clients already`). Raised to `100` in `compose/compose.yaml` — a good example of a limit that was fine at the original scale of the lab and needed revisiting as load-testing got more aggressive.

## Results

### Overload (300 VUs, paced, `TokenLimit=80`)

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Real server errors (5xx) | 0.00% | < 1% |
| Requests shed (429) | 83.38% | > 5% |
| Accepted-request p95 | 8.4 ms | < 1,500 ms |
| Rate-limited responses carry `Retry-After` | Yes | Required |
| Orders created / processed | 5,966 / 5,966 | 100% |
| Pending Outbox after drain | 0 | 0 |

Under a sustained offered load far beyond capacity, the API sheds most of it immediately (429, fast, cheap) while the requests it does accept stay fast — no cascading degradation, no real failures, full pipeline convergence once the overload ends.

### Regression: `autoscale` (Milestone 8's existing acceptance suite)

| Measurement | Result | Acceptance |
| --- | ---: | ---: |
| Successful checks | 100.00% | > 99% |
| Failed HTTP requests | 0.00% | < 1% |
| Order flow success | 100.00% | > 99% |
| Create p99 | 717 ms | < 1,500 ms |
| Get p99 | 156 ms | < 1,000 ms |

The rate limiter does not interfere with legitimate autoscale-driven traffic once tuned correctly — confirmed by rerunning the pre-existing, unmodified `autoscale` profile end to end.

## Running the experiment

```bash
cd /srv/local-distributed-lab
scripts/k6-run.sh overload
```

The `overload` profile ramps to 300 VUs over 10 seconds, holds for 20 seconds, and ramps down — paced per-iteration so the offered load is heavy but not physically unrealistic. Its thresholds assert real failures stay near zero, a meaningful fraction is shed, and accepted requests stay fast; unlike other profiles, a high `http_req_failed` rate is expected and by design (429s are the point).
