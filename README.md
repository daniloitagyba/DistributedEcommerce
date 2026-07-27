# Local Distributed Systems Lab

This repository is a practical distributed systems laboratory running on an Ubuntu server. Milestone 6 migrates the stateless Orders API and Worker workloads to K3s while PostgreSQL, Kafka, Kafka UI, and the observability stack remain in Docker Compose.

## Architecture

```text
Mac client
    |
    +-> SSH tunnel -> kubectl port-forward -> K3s Service -> Orders.Api pod 1/2
                                                        |
                                                        +-> PostgreSQL (order + outbox)
                                                        |
                                                        +-> Kafka orders.created.v1
                                                                  |
                                                                  +-> Orders.Worker pod
                                                                        |
                                                                        +-> PostgreSQL Inbox
                                                                        +-> Kafka orders.created.dlq.v1

K3s applications -> OTLP -> OpenTelemetry Collector -> Prometheus (metrics)
                                                    |-> Tempo (traces)
                                                    +-> Loki (logs)

Prometheus + Tempo + Loki -> Grafana
```

K3s reaches the Compose infrastructure through a dedicated internal Docker bridge. Selectorless Kubernetes Services and EndpointSlices provide stable in-cluster names for PostgreSQL, Kafka, and the OpenTelemetry Collector. The bridge is not published on the host.

PostgreSQL, Kafka, Tempo, Loki, and the Collector have no host ports. Kafka UI, Prometheus, and Grafana bind only to server loopback. The Orders API is exposed temporarily through `kubectl port-forward`, also bound to loopback, and all Mac access uses SSH port forwarding.

## Repository layout

- `apps/src/BuildingBlocks`: event contracts and shared OpenTelemetry instrumentation.
- `apps/src/Orders.Api`: order API, PostgreSQL persistence, transactional Outbox, migrations, and Kafka producer.
- `apps/src/Orders.Worker`: idempotent Kafka consumer, PostgreSQL Inbox, bounded retries, and DLQ publisher.
- `apps/tests`: unit and PostgreSQL Testcontainers integration tests.
- `compose`: external infrastructure and the optional legacy Compose application profile.
- `kubernetes/base`: reusable application, migration, health, security, resource, and network-policy manifests.
- `kubernetes/overlays/local`: the K3s-to-Compose endpoints and local image policy.
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

The integration test starts a disposable PostgreSQL 17 container, applies the real EF Core migrations, and verifies Inbox deduplication by both event identity and Kafka source position.

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

Apply the namespace, runtime Secret, bridge endpoints, migration Job, two API replicas, Worker, Service, PodDisruptionBudget, and NetworkPolicies:

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

Kafka delivery remains at least once. Before committing an offset, the Worker records both event identity and source position in the PostgreSQL Inbox. Replayed events and reused source positions are skipped safely. Processing failures are retried three times and then published to `orders.created.dlq.v1`; PostgreSQL or DLQ publication failures leave the source offset uncommitted.

Inspect Outbox and Inbox state without publishing PostgreSQL:

```bash
cd /srv/local-distributed-lab/compose
docker compose exec -T postgres psql --username orders --dbname orders --command "SELECT id, event_type, attempt_count, processed_at FROM outbox_messages ORDER BY occurred_at DESC LIMIT 20;"
docker compose exec -T postgres psql --username orders --dbname orders --command 'SELECT consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at FROM inbox_messages ORDER BY processed_at DESC LIMIT 20;'
```

Dead letters are retained for 24 hours in the internal Kafka topic `orders.created.dlq.v1` and can be inspected through Kafka UI.

The next milestone adds reproducible k6 workload profiles, performance thresholds, capacity baselines, and resource tuning for the K3s application runtime.
