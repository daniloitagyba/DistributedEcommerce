# Milestone 19 Schema Registry + Contract Evolution

## Scope

Every event on this lab's Kafka topics has been raw JSON, deserialized with nothing checking that a consumer's expectations match what a producer actually sent - a field rename or type change ships as a silent runtime failure, discovered only when a consumer throws on real traffic. This milestone adds a schema registry in front of `orders.created.v1` (the topic every one of the three services - Orders.Api, Orders.Worker, Payments.Service - either produces or consumes), migrates that one contract to Avro, and proves the registry actually rejects an incompatible schema change rather than just documenting one.

## Design

- **Karapace, not Confluent Schema Registry.** This lab already runs vanilla `apache/kafka` rather than Confluent Platform specifically to avoid Confluent's more restrictive licensing on server components; Karapace ([Aiven-Open/karapace](https://github.com/Aiven-Open/karapace), Apache 2.0) is wire-compatible with the same REST API and Avro serialization format, so the same open-source posture extends to the registry. Deployed via Compose on the existing `k3s-bridge` static-IP pattern (Milestone 14/15's approach for Kafka/Prometheus), so it's reachable from both Compose and K3s.
- **`OrderCreated` moved to Avro; every other topic stays JSON.** `orders.created.v1` is the one topic three separate services touch, making it the highest-value (and highest-risk) place to prove the concept - `payments.result.v1` and the various dead-letter topics are each single-producer/single-consumer pairs already living in the same codebase, where a schema registry adds less protection for meaningfully more surface area. `BuildingBlocks/OrderCreatedAvroSchema.cs` is now the single place that owns the .avsc definition and the conversion to/from `OrderCreated`, so the producer (`KafkaOrderEventPublisher` in `Orders.Infrastructure`) and all three consumers (`Orders.Worker`'s `OrderCreatedConsumer` and `OrderProjectionConsumer`, `Payments.Service`'s `OrderCreatedConsumer`) can't independently drift on the mapping.
- **Guid, DateTimeOffset, and decimal fields are Avro strings, not logical types.** Avro has native `uuid`, `timestamp-micros`, and `decimal` logical types, but implementing the exact byte-level encoding for `decimal` by hand (scale/precision-aware two's-complement bytes) for a real financial `Amount` field is real risk for no benefit this milestone actually needs - the point is demonstrating schema registry mechanics, not decimal wire-encoding. String-encoding avoids floating-point precision loss entirely (unlike Avro `double`) at the cost of slightly larger messages.
- **Kafka transport moved from `string` to `byte[]` value type everywhere `orders.created.v1` is touched**, with Avro encode/decode as an explicit step at the edges (`AvroSerializer<GenericRecord>.SerializeAsync`/`AvroDeserializer<GenericRecord>.DeserializeAsync`) rather than baking the serializer into the `ProducerBuilder`/`ConsumerBuilder` generic type. `OrderProjectionConsumer` reads both `orders.created.v1` (now Avro) and `payments.result.v1` (still JSON) on the same subscription - `byte[]` is the one transport type that works for both without forcing a topic-wide format decision, decoding differently per topic exactly where the processor already branches on `consumeResult.Topic`.
- **The outbox's Postgres storage format is unchanged.** `CreateOrderHandler` still serializes `OrderCreated` to JSON before writing the `outbox_messages` row; `OutboxPublisher` still deserializes that JSON back into an `OrderCreated` object. Only `KafkaOrderEventPublisher.PublishAsync` - the actual Kafka wire boundary - was touched, converting that in-memory object to Avro immediately before producing. Postgres storage format and Kafka wire format are two different concerns that happened to share an accidental JSON dependency before this milestone; they no longer do.
- **Dead-letter envelopes now base64-encode the original payload** (`DeadLetterEnvelope.OriginalPayload`) instead of assuming it's printable text - a dead-lettered Avro-encoded message isn't valid UTF-8, and the envelope itself is still a JSON document produced by a separate, unchanged `IProducer<string, string>`.
- **Compatibility mode is `BACKWARD`** (Karapace's config default, set explicitly): a new schema version must be readable using the previous version's reader schema - the standard mode for "producers can deploy ahead of consumers."

## What didn't work

**Argo CD's `selfHeal` reverted the Kubernetes manifest changes before they were committed - the exact Milestone 15 lesson, recurring.** After building the new images and running `k3s-deploy.sh` (which `kubectl apply`s the kustomize output directly, same as every prior milestone), the smoke test failed: the worker never processed the final order. The actual cause took real investigation - not a code bug at all. `outbox_messages.last_error` showed `HttpRequestException: Connection refused (localhost:8081)`: the *live* Rollout's env vars were missing `SchemaRegistry__Url` entirely, despite the manifest on disk (and the version just `kubectl apply`-ed) having it. Argo CD's `selfHeal: true` reconciles the cluster toward whatever `main` currently says, on its own poll cycle, independent of any `kubectl apply` run in between - since the schema registry env var wasn't committed yet, Argo CD quietly reverted it back out from under the manual apply moments after it landed. The fix is identical to Milestone 15's: commit and push first, then deploy, never the other way around for anything Argo CD owns.

**`Confluent.SchemaRegistry` 2.15.0 ships no mock/in-memory client**, unlike `Testcontainers.PostgreSql`/`Testcontainers.Redis` already used elsewhere in the integration test suite. `ISchemaRegistryClient` has 24+ members; hand-rolling a fake risked subtly wrong behavior around schema-ID assignment specifically in the one place (`PaymentMessageProcessorTests`) that needed to exercise a full Avro round-trip. Since this lab runs on a single host where the real Karapace instance is always up on the Compose network, the test points `CachedSchemaRegistryClient` directly at it (`172.30.0.16:8081`) rather than faking it - a deliberate departure from the hermetic-Testcontainers pattern used for Postgres/Redis, traded for correctness confidence over isolation.

**Dropping required fields is BACKWARD-compatible in Avro, which is not the intuitive answer.** The first attempt at proving the registry rejects a breaking change removed every field except `eventId` from a v2 candidate schema, expecting `is_compatible: false`. The registry said `true`. Avro's schema resolution rule for `BACKWARD` compatibility is "the new reader schema must be able to read data written with the old schema" - a reader with fewer fields just ignores whatever extra data is in the old records, which is genuinely safe at the *wire* level even though deleting `customerId`/`amount`/`currency` would obviously break every consumer's actual business logic. The registry only enforces structural readability, not semantic correctness. The real incompatible case - and the one that actually demonstrates the registry catching something - is adding a *new required field with no default*: old data has nothing to populate it with, and that genuinely fails compatibility (`is_compatible: false`).

**A third consumer of `orders.created.v1` almost got missed.** The task was framed around Orders.Api (producer) and Orders.Worker (consumer), but `Payments.Service` has its own independent `OrderCreatedConsumer` reading the same topic to drive the saga's payment decision - found only by grepping for `OrderCreatedTopic` usage across every service before writing any code, not from the topic's name alone suggesting only two services cared about it.

## Results

### Registry round-trip

| Check | Result |
| --- | --- |
| `dotnet test` (all 3 places `OrderCreated` is produced/consumed) | 24 + 7 passing - `PaymentMessageProcessorTests` genuinely round-trips through the live registry, registering subject `orders.created.v1-value` version 1 |
| `curl http://schema-registry:8081/subjects` | `["orders.created.v1-value"]` |

### Live validation (after the Argo CD fix, full redeploy)

| Check | Result |
| --- | --- |
| `k3s-smoke-test.sh` | Passed - orders created via Avro-producing `orders-api`, consumed and logged by Avro-decoding `orders-worker` |
| `k6-run.sh saga` | `failed_rate=0`; `saga_correct_outcome_rate=99.48%` (390/392) - Payments.Service correctly Avro-decodes every message and decides approve/decline; the sub-100% figure matches this lab's already-documented pre-existing tail-latency flakiness in the saga poll window, not a regression from this milestone |
| `k6-run.sh baseline` | `failed_rate=0`, `checks_rate=1`, `flow_rate=1`; `create_p95_ms=3.9` - no regression from the JSON→Avro transport change |

### Compatibility enforcement

```bash
# The registered v1 schema (BACKWARD compatibility, Karapace's default):
curl http://schema-registry:8081/subjects/orders.created.v1-value/versions/latest

# Dropping fields is actually BACKWARD-compatible in Avro's resolution rules
# (a reader with fewer fields just ignores the extra data in old records) -
# tried first, expecting a rejection, and got a real lesson instead:
# {"is_compatible": true}. The genuinely incompatible change is adding a new
# required field with no default - old data has nothing to satisfy it with:
curl -X POST http://schema-registry:8081/compatibility/subjects/orders.created.v1-value/versions/latest \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"OrderCreated\",\"namespace\":\"local_distributed_lab.orders\",\"fields\":[{\"name\":\"eventId\",\"type\":\"string\"},{\"name\":\"orderId\",\"type\":\"string\"},{\"name\":\"customerId\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"string\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"occurredAt\",\"type\":\"string\"},{\"name\":\"correlationId\",\"type\":\"string\"},{\"name\":\"schemaVersion\",\"type\":\"int\",\"default\":1},{\"name\":\"shippingAddress\",\"type\":\"string\"}]}"}'
# -> {"is_compatible": false, "messages": ["shippingAddress"]}
```

## Running the experiment

```bash
scripts/k3s-build-images.sh
scripts/k3s-deploy.sh   # only after committing - see "what didn't work" above
scripts/k3s-smoke-test.sh
curl http://<schema-registry>:8081/subjects
```
