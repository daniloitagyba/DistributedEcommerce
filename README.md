# Local Distributed Systems Lab

This repository is a practical distributed systems laboratory running on an Ubuntu server. Milestone 4 adds OpenTelemetry traces and metrics, an OpenTelemetry Collector, Prometheus, Tempo, and provisioned Grafana dashboards to the reliable order flow.

## Architecture

```text
Client -> Nginx -> Orders.Api replica 1/2 -> PostgreSQL (order + outbox event)
                                                    |
                                                    +-> outbox publisher
                                                          |
                                                          +-> Kafka orders.created.v1 -> Orders.Worker -> PostgreSQL Inbox
                                                                                         |
                                                                                         +-> Kafka orders.created.dlq.v1

Orders.Api + Orders.Worker -> OTLP -> OpenTelemetry Collector -> Prometheus (metrics)
                                                           |
                                                           +-> Tempo (traces)

Prometheus + Tempo -> Grafana
```

PostgreSQL, Kafka, Tempo, and the Collector are available only on the internal Compose network. Nginx, Kafka UI, Prometheus, and Grafana bind to server loopback and are accessed through SSH port forwarding.

## Repository layout

- `apps/src/BuildingBlocks`: event contracts and shared OpenTelemetry instrumentation.
- `apps/src/Orders.Api`: order API, PostgreSQL persistence, transactional Outbox, migrations, and Kafka producer.
- `apps/src/Orders.Worker`: idempotent Kafka consumer, PostgreSQL Inbox, bounded retries, and DLQ publisher.
- `apps/tests`: unit and PostgreSQL Testcontainers integration tests.
- `compose`: Compose, Nginx, and environment examples.
- `observability`: Collector, Prometheus, Tempo, and provisioned Grafana configuration.
- `scripts`: repeatable verification scripts.
- `kubernetes`: reserved for the K3s migration milestone.

## Configure local secrets

The real `compose/.env` file is local-only and ignored by Git. To create it manually:

```bash
cd /srv/local-distributed-lab/compose
cp .env.example .env
# Replace POSTGRES_PASSWORD with a random local password and restrict the file to mode 600.
```

Grafana uses anonymous Viewer access because it is read-only, bound to server loopback, and reached through SSH. Basic authentication, login forms, user signup, and initial admin creation are disabled. Dashboards and data sources are managed as code.

## Build and test

```bash
cd /srv/local-distributed-lab/apps
dotnet restore LocalDistributedLab.slnx
dotnet build LocalDistributedLab.slnx --no-restore
dotnet test LocalDistributedLab.slnx --no-build
```

The integration test starts a disposable PostgreSQL 17 container, applies the real EF Core migrations, and verifies Inbox deduplication.

## Run the Compose stack

```bash
cd /srv/local-distributed-lab/compose
docker compose config --quiet
docker compose build
docker compose up --detach --wait
```

Run the end-to-end verification:

```bash
/srv/local-distributed-lab/scripts/smoke-test.sh
```

Inspect the stack:

```bash
cd /srv/local-distributed-lab/compose
docker compose ps
docker compose logs --follow orders-api-1 orders-api-2 orders-worker otel-collector
```

Stop containers without deleting persistent volumes:

```bash
cd /srv/local-distributed-lab/compose
docker compose down
```

Do not add `--volumes` unless volume deletion has been explicitly approved.

## Access from the Mac

Use one SSH session for all loopback-only services:

```bash
ssh \
  -L 8088:127.0.0.1:8088 \
  -L 8080:127.0.0.1:8080 \
  -L 3000:127.0.0.1:3000 \
  -L 9090:127.0.0.1:9090 \
  itagyba@192.168.15.10
```

- Orders API: `http://127.0.0.1:8088`
- Kafka UI: `http://127.0.0.1:8080`
- Grafana: `http://127.0.0.1:3000`
- Prometheus: `http://127.0.0.1:9090`

The provisioned Grafana dashboard is in the `Distributed Systems Lab` folder. Use Explore with the Tempo data source to search traces by `service.name`, span name, duration, or trace ID.

## API example

```bash
curl --request POST http://127.0.0.1:8088/orders \
  --header 'Content-Type: application/json' \
  --header 'X-Correlation-ID: example-001' \
  --data '{"customerId":"customer-42","amount":49.90,"currency":"BRL"}'
```

The response contains `X-Correlation-ID` and `X-Instance-ID`. Correlation and W3C trace context continue through the Outbox, Kafka, and Worker. Structured logs include correlation, event, order, and trace identifiers.

## Observability

Applications export OTLP over gRPC to the Collector. The Collector exposes application metrics to Prometheus and sends traces to Tempo. Prometheus and Tempo retain local data for 24 hours. Prometheus storage is additionally capped at 512 MB.

The default dashboard includes order, processing, Outbox, duplicate, HTTP request-rate, and HTTP latency panels. Metrics preserve `service_name` and `service_instance_id`, allowing both API replicas to be compared.

Inspect Prometheus targets and a business metric from the server:

```bash
curl --silent http://127.0.0.1:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health}'
curl --silent 'http://127.0.0.1:9090/api/v1/query?query=orders_created_total' | jq '.data.result'
```

## Delivery guarantees

The API writes the order and its `OrderCreated` Outbox message in the same PostgreSQL transaction. Two API replicas safely poll pending messages with `FOR UPDATE SKIP LOCKED`. Failed Kafka publishes remain durable and use capped exponential backoff.

Kafka delivery remains at least once, but the Worker records `(consumer_name, event_id)` in the PostgreSQL Inbox before committing the Kafka offset. Redelivered events are skipped safely. Processing failures are retried three times and then published to `orders.created.dlq.v1`. PostgreSQL or DLQ publication failures leave the source offset uncommitted for recovery.

Inspect Outbox and Inbox state without exposing PostgreSQL outside the Compose network:

```bash
cd /srv/local-distributed-lab/compose
docker compose exec -T postgres psql --username orders --dbname orders --command "SELECT id, event_type, attempt_count, processed_at FROM outbox_messages ORDER BY occurred_at DESC LIMIT 20;"
docker compose exec -T postgres psql --username orders --dbname orders --command 'SELECT consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at FROM inbox_messages ORDER BY processed_at DESC LIMIT 20;'
```

Dead letters are retained for 24 hours in the internal Kafka topic `orders.created.dlq.v1` and can be inspected through Kafka UI.

The next observability milestone can add Loki and structured-log correlation after measuring the current stack's steady-state memory use.
