# Local Distributed Systems Lab

This repository is a practical distributed systems laboratory running on an Ubuntu server. Milestone 11 adds token-bucket rate limiting to the Orders API and proves it sheds overload (429s, fast) without real failures or accepted-request degradation.

## Architecture

```text
Mac client
    |
    +-> SSH tunnel -> kubectl port-forward -> K3s Service -> Orders.Api pod 1/2
                                                        |
                                                        +-> PostgreSQL (order + outbox)
                                                        |
                                                        +-> Redis (order read cache)
                                                        |
                                                        +-> Kafka orders.created.v1
                                                                  |
                                                                  +-> Orders.Worker pod
                                                                        |
                                                                        +-> PostgreSQL Inbox + order status
                                                                        +-> Redis (cache invalidation)
                                                                        +-> Kafka orders.created.dlq.v1

K3s applications -> OTLP -> OpenTelemetry Collector -> Prometheus (metrics)
                                                    |-> Tempo (traces)
                                                    +-> Loki (logs)

Prometheus + Tempo + Loki -> Grafana
```

K3s reaches the Compose infrastructure through a dedicated internal Docker bridge. Selectorless Kubernetes Services and EndpointSlices provide stable in-cluster names for PostgreSQL, Kafka, Redis, and the OpenTelemetry Collector. The bridge is not published on the host.

PostgreSQL, Kafka, Redis, Tempo, Loki, and the Collector have no host ports. Kafka UI, Prometheus, and Grafana bind only to server loopback. The Orders API is exposed temporarily through `kubectl port-forward`, also bound to loopback, and all Mac access uses SSH port forwarding.

## Repository layout

- `apps/src/BuildingBlocks`: event contracts, shared OpenTelemetry instrumentation, shared Redis wiring, and the Polly resilience pipelines.
- `apps/src/Orders.Api`: order API, PostgreSQL persistence, transactional Outbox, migrations, Kafka producer, the Redis cache-aside layer, and token-bucket rate limiting, all wrapped with resilience pipelines.
- `apps/src/Orders.Worker`: idempotent Kafka consumer, PostgreSQL Inbox, order status transitions, cache invalidation, bounded retries, and DLQ publisher, all wrapped with resilience pipelines.
- `apps/tests`: unit tests and PostgreSQL/Redis Testcontainers integration tests.
- `compose`: external infrastructure, Toxiproxy for fault injection, and the optional legacy Compose application profile.
- `docs/caching`: reviewed Redis cache-aside and invalidation reports.
- `docs/load-shedding`: reviewed rate-limiting and overload reports.
- `docs/performance`: versioned baseline reports and interpretation notes.
- `docs/resilience`: reviewed autoscaling and controlled-failure reports.
- `kubernetes/base`: reusable application, migration, health, security, resource, and network-policy manifests.
- `kubernetes/overlays/local`: the K3s-to-Compose endpoints and local image policy.
- `load-tests/k6`: versioned workload behavior, profiles, and thresholds.
- `observability`: Collector, Prometheus, Tempo, Loki, and provisioned Grafana configuration.
- `scripts`: repeatable image, deployment, access, and verification workflows.

## Configure local secrets

The real `compose/.env` file is local-only and ignored by Git:

```bash
cd /srv/local-distributed-lab/compose
cp .env.example .env
# Replace POSTGRES_PASSWORD with a random local password and restrict the file to mode 600.
```

`scripts/k3s-deploy.sh` derives the database connection string from the resolved Compose configuration and streams it directly into the `orders-runtime` Kubernetes Secret. Its value is not printed, stored in a manifest, or committed.

Grafana uses anonymous Viewer access because it is read-only, bound to server loopback, and reached through SSH. Basic authentication, login forms, user signup, and initial admin creation are disabled. Dashboards and data sources are managed as code.

## Build and test

All commands run on the Ubuntu server:

```bash
cd /srv/local-distributed-lab/apps
dotnet restore LocalDistributedLab.slnx
dotnet build LocalDistributedLab.slnx --no-restore
dotnet test LocalDistributedLab.slnx --no-build
```

The integration test starts a disposable PostgreSQL 17 container, applies the real EF Core migrations, and verifies Inbox deduplication by stable event identity even when a recreated Kafka topic reuses an earlier source position.

## Start the external infrastructure

The default Compose profile now starts only PostgreSQL, Kafka, Kafka UI, and observability:

```bash
cd /srv/local-distributed-lab/compose
docker compose config --quiet
docker compose up --detach --wait
```

The previous Compose application runtime remains available as an explicit fallback:

```bash
docker compose --profile compose-apps up --detach --wait
```

Do not run the Compose application profile concurrently with K3s during normal operation because both Workers use the same Kafka consumer group.

## Deploy the applications to K3s

Build the application images and import them into the K3s containerd image store:

```bash
cd /srv/local-distributed-lab
scripts/k3s-build-images.sh
```

The import uses the official K3s image as a short-lived, network-isolated Docker client with access only to the K3s containerd socket. It does not require `sudo`, change the host configuration, or publish an image registry.

Apply the namespace, runtime Secret, bridge endpoints, migration Job, two minimum API replicas, CPU-based HPA, Worker, Service, PodDisruptionBudget, and NetworkPolicies:

```bash
scripts/k3s-deploy.sh
```

The deployment waits for infrastructure connectivity, database migration, and both application rollouts before stopping the legacy Compose API, Worker, and Nginx containers. Persistent infrastructure and volumes remain untouched.

Inspect the runtime:

```bash
kubectl get pods,services,endpointslices -n orders-lab -o wide
kubectl logs -n orders-lab deployment/orders-worker --follow
kubectl describe deployment/orders-api -n orders-lab
```

Run the end-to-end verification:

```bash
scripts/k3s-smoke-test.sh
```

The test verifies two Ready API replicas, order creation and retrieval, Worker consumption, correlation propagation, and Loki ingestion. A service port-forward selects one backend pod, so replica readiness is validated independently through the Deployment status and Kubernetes health probes.

## Performance testing

The k6 workload runs on the Ubuntu server directly against the Orders API ClusterIP. This exercises the real Kubernetes Service and both API replicas without publishing a port or measuring SSH tunnel overhead.

Available profiles:

- `smoke`: one VU for 10 seconds.
- `baseline`: ramps to 5 VUs, holds, ramps to 10 VUs, holds, and ramps down over 70 seconds.
- `autoscale`: ramps to 75 VUs, holds for 60 seconds, and validates HPA scale-up and scale-down.
- `resilience`: holds 5 VUs for 75 seconds while API and Worker rolling restarts are exercised.
- `stress`: optional ramp to 30 VUs over 90 seconds.
- `soak`: optional 5 VUs for 5 minutes.
- `cache`: seeds a pool of orders, then holds 10 VUs for 30 seconds reading only, to measure cache hit ratio and cached-read latency.
- `chaos`: holds 5 VUs for 40 seconds with relaxed latency thresholds, used by `scripts/resilience-chaos.sh` while a fault is injected through Toxiproxy.
- `overload`: paced ramp to 300 VUs for 30 seconds, deliberately exceeding capacity to exercise rate limiting and load shedding.

Run the conservative profiles:

```bash
cd /srv/local-distributed-lab
scripts/k6-run.sh smoke
scripts/k6-run.sh baseline
scripts/hpa-test.sh
scripts/resilience-test.sh
```

The runner verifies Kubernetes readiness and zero pre-existing Kafka lag, captures PostgreSQL and Prometheus counters, samples pod CPU and memory every two seconds, waits for Inbox/Outbox convergence, and reports per-pod API distribution. The HPA test requires scale-up above two replicas and a return to the minimum. The resilience test performs only rolling restarts, requires zero HTTP failures, and verifies the Worker's graceful-shutdown log in Loki.

The API HPA targets 60% of its CPU request, keeps 2 to 4 replicas, scales up promptly, and uses a conservative 60-second scale-down window. API pods wait five seconds in a `preStop` hook before receiving SIGTERM so terminating endpoints can drain during a rollout.

Raw output is written under ignored `artifacts/k6/`; reviewed results are documented in `docs/performance/milestone-7-baseline.md` and `docs/resilience/milestone-8-autoscaling-resilience.md`.

Baseline and autoscale thresholds require fewer than 1% failed HTTP requests and more than 99% successful checks and order flows. The resilience profile requires exactly zero failed HTTP requests and 100% successful checks and flows. Stress and soak profiles are intentionally manual because they consume more resources and create persistent laboratory data.

## Access from the Mac

In VS Code, connect with Remote SSH and open `/srv/local-distributed-lab`. This keeps source edits, terminals, builds, containers, logs, and tests on the Ubuntu server while the Mac remains the management workstation.

Open one terminal on the Mac for the Orders API. This command starts the server-side Kubernetes port-forward and carries it through SSH:

```bash
ssh -L 8088:127.0.0.1:8088 \
  itagyba@192.168.15.10 \
  /srv/local-distributed-lab/scripts/k3s-port-forward.sh 8088
```

Open another terminal for the loopback-only administrative interfaces:

```bash
ssh \
  -L 8080:127.0.0.1:8080 \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  itagyba@192.168.15.10
```

- Orders API: `http://127.0.0.1:8088`
- Kafka UI: `http://127.0.0.1:8080`
- Grafana: `http://127.0.0.1:3000`
- Prometheus: `http://127.0.0.1:9090`

Keep these SSH sessions open while using the interfaces. No service is exposed to the LAN or Internet.

## API example

```bash
curl --request POST http://127.0.0.1:8088/orders \
  --header 'Content-Type: application/json' \
  --header 'X-Correlation-ID: example-001' \
  --data '{"customerId":"customer-42","amount":49.90,"currency":"BRL"}'
```

The response contains `X-Correlation-ID` and `X-Instance-ID`. The instance ID is the K3s pod name. Correlation and W3C trace context continue through the Outbox, Kafka, and Worker.

## Observability

Applications export traces, metrics, and structured `ILogger` records over OTLP/gRPC through the selectorless `otel-collector` Service. The Collector exposes application metrics to Prometheus, sends traces to Tempo, and sends logs to Loki through OTLP/HTTP. Backends retain local data for 24 hours; Prometheus storage is additionally capped at 512 MB.

K3s telemetry includes `k8s.namespace.name=orders-lab` and `service.instance.id` set to the pod name. Correlation IDs, event IDs, order IDs, trace IDs, and span IDs remain structured metadata so logs and traces can be joined without high-cardinality index labels.

Example LogQL queries:

```logql
{service_name="orders-worker", k8s_namespace_name="orders-lab"}
{service_name=~"orders-api|orders-worker"} | CorrelationId = "example-001"
{service_name="orders-worker"} | trace_id = "<trace-id>"
```

The provisioned Grafana dashboard is in the `Distributed Systems Lab` folder. Trace details link to matching logs, and correlated Loki logs link back to Tempo traces.

## Delivery guarantees

The API writes the order and its `OrderCreated` Outbox message in the same PostgreSQL transaction. Two API replicas safely poll pending messages with `FOR UPDATE SKIP LOCKED`. Failed Kafka publishes remain durable and use capped exponential backoff.

Kafka delivery remains at least once. Before committing an offset, the Worker deduplicates by `(consumer_name, event_id)` in the PostgreSQL Inbox. Topic, partition, and offset are retained in a non-unique diagnostic index, so a local Kafka topic can be recreated without causing a new event at a reused offset to be discarded. Processing failures are retried three times and then published to `orders.created.dlq.v1`; PostgreSQL or DLQ publication failures leave the source offset uncommitted.

Inspect Outbox and Inbox state without publishing PostgreSQL:

```bash
cd /srv/local-distributed-lab/compose
docker compose exec -T postgres psql --username orders --dbname orders --command "SELECT id, event_type, attempt_count, processed_at FROM outbox_messages ORDER BY occurred_at DESC LIMIT 20;"
docker compose exec -T postgres psql --username orders --dbname orders --command 'SELECT consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at FROM inbox_messages ORDER BY processed_at DESC LIMIT 20;'
```

Dead letters are retained for 24 hours in the internal Kafka topic `orders.created.dlq.v1` and can be inspected through Kafka UI.

## Caching

`GET /orders/{id}` uses cache-aside against Redis: a hit is served directly from `orders:cache:{id}`; a miss takes a short distributed lock (`orders:cache-lock:{id}`) to avoid a stampede, reads PostgreSQL, and repopulates the cache with a 30-second TTL. Responses include an `X-Cache: HIT|MISS|BYPASS` header. After the Worker processes an order's event, it transitions the order to `Confirmed` and deletes the cache entry, so the next read reflects the new status instead of a stale one. Redis has no host port and no authentication, matching the existing Kafka trust model. See `docs/caching/milestone-9-cache.md` for the measured hit ratio and cached-read latency.

## Resilience and chaos engineering

Every call to PostgreSQL, Kafka, and Redis goes through a named Polly pipeline (timeout, retry, circuit breaker) registered in `BuildingBlocks/ResilienceExtensions.cs`. PostgreSQL and Kafka failures fail fast — a `503 Service Unavailable` with `Retry-After` instead of a multi-second hang — while a Redis outage degrades gracefully: the cache is bypassed (`X-Cache: BYPASS`) and reads fall straight through to PostgreSQL rather than failing the request. Every pipeline execution, retry, timeout, and circuit-breaker transition is emitted automatically as an OpenTelemetry metric (`resilience_polly_*`) and graphed on the `Orders Lab Overview` dashboard.

`scripts/resilience-chaos.sh <postgres|kafka> <latency|outage>` proves these policies against real faults using [Toxiproxy](https://github.com/Shopify/toxiproxy), which runs in Compose but is **not** in the default traffic path. The script reversibly reroutes the target's EndpointSlice through Toxiproxy for the duration of one experiment — restarting the workloads so pooled connections actually traverse the fault, injecting a latency or outage toxic, running a workload, then always reverting (via a `trap`-guarded cleanup) and re-verifying the EndpointSlice is back on the real backend before declaring success. See `docs/resilience/milestone-10-chaos-resilience.md` for the measured results, including a PostgreSQL outage that fails every request in a consistent ~320 ms with zero partial writes, and a Kafka outage that has no effect at all on order creation (201s in ~12 ms) because the transactional Outbox decouples the two.

## Load shedding

`GET`/`POST /orders` share a per-pod token-bucket rate limiter (`RateLimit:*` config, `Orders.Api/RateLimiting`): burst capacity 80, refilling 75 tokens/second. Requests beyond the bucket get a `429 Too Many Requests` with a `Retry-After` header instead of queueing or being accepted into an overloaded backend — an `orders.rate_limited` metric tracks how often this triggers. This is deliberately a per-instance, in-memory limiter rather than a distributed one: the effective ceiling scales with replica count, protecting each pod's own resources behind the existing Kubernetes Service load balancing.

Tuning this required two real fixes, documented in `docs/load-shedding/milestone-11-load-shedding.md`: an initial generous burst setting let a simulated thundering herd overwhelm PostgreSQL before steady-state throttling caught up (real 5xx errors, not just 429s), and PostgreSQL's `max_connections` (set to 50 in an earlier milestone) briefly exhausted under HPA-scaled connection pooling — raised to 100. The final settings shed 83%+ of a deliberate 300-VU overload with zero real failures and accepted-request p95 under 10 ms, while leaving Milestone 8's `autoscale` acceptance suite passing at 100%.

The next milestone can add a second service (Payments) consuming order events with a choreographed saga, building on the resilience and load-shedding primitives introduced so far.
