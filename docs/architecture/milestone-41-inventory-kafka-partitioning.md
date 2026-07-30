# Milestone 41: Inventory Service with Kafka-Partitioned Stock Reservation

## Scope

The last Tier A gap-analysis item: serializing concurrent writes to the same piece of shared state (a SKU's stock count) without reaching for a database lock. `Inventory.Service` owns per-SKU stock and answers reservation requests - the interesting part isn't the CRUD, it's how it stays race-free under concurrency.

## The problem this is solving

Two orders for the last unit of the same SKU, arriving at roughly the same time, must not both succeed. The conventional fix is a database-level lock: `SELECT ... FOR UPDATE` (used elsewhere in this lab - see the outbox pollers) or an atomic `UPDATE ... WHERE available_quantity >= @qty`. Both work, but both serialize through the database itself, and the second gives you *no visibility* into how many callers had to wait or retry.

This milestone uses a different mechanism: Kafka's own consumer-group partition assignment. A topic partition is owned by exactly one consumer instance in a group at any given time, and the same message key always maps to the same partition. So if `InventoryReservationRequested` messages are **produced keyed by `Sku`** (not by `OrderId`, which is what every other request/reply pair in this lab is keyed by - see `PaymentDecisionReplied`), then every reservation request for a given SKU is guaranteed to land on the same partition and be processed strictly one-at-a-time by exactly one `Inventory.Service` replica - never two replicas touching the same SKU concurrently.

## Design

- `InventoryItem.TryReserve` (`Domain/InventoryItem.cs`) is **deliberately a plain read-then-write**, no optimistic concurrency token, no `SELECT ... FOR UPDATE`, no `WHERE available_quantity >= @qty` guard on the eventual `UPDATE`. This is the whole point: if this code were ever run concurrently against the same row, it would oversell. Safety comes entirely from the fact that it never *is* run concurrently for the same SKU, because of the partitioning above. Using a WHERE-guarded atomic update instead would have made the correctness test meaningless - it would pass even if the partitioning claim were false (e.g. if the topic were accidentally keyed by `OrderId`), because Postgres's own row lock would have quietly saved it. The naive implementation makes the live test in this milestone an actual test of the architectural claim, not of an unrelated safety net.
- Structured like `Payments.Service`: transactional outbox (`OutboxMessage` + `OutboxPublisher`) so the stock decrement and the reply-is-queued decision commit atomically, and an `inbox_messages` table for `ReservationId` dedup (exactly the same shape as Payments' `EventId` dedup) so a redelivered Kafka message can't double-decrement.
- Messages are plain JSON, not Avro/schema-registered - same rationale as Milestone 22's `PaymentDecisionRequested`/`Replied`: internal, transient request/reply traffic between one producer and one responder, not a domain event other services evolve against independently.
- No caller exists yet - the same situation Milestone 39's hedging was in. Cart/Checkout (M42/M43) will be the real producer of `InventoryReservationRequested`; this milestone builds and proves the mechanism directly against Kafka, the same way M39 proved hedging directly through k6 before any internal caller existed.
- **2 replicas is load-bearing, not just an availability default.** With `replicas: 1` there would be nothing else that *could* race for a partition, and the "Kafka partition ownership prevents the race" claim would be untested by construction.

## Live results

**Same-SKU oversell prevention** (`scripts/inventory-reservation-concurrency-test.sh`): `SKU-ELEC-002` seeded with 8 units. 60 reservation requests fired at it in total across two bursts (both keyed by `SKU-ELEC-002`, so both routed to the same partition regardless of which pod produced or received them):

```
SKU-ELEC-002 reserved=true:  8
SKU-ELEC-002 reserved=false: 52   (reason: "insufficient stock")
```

Final state: `availableQuantity=0, reservedQuantity=8` - exactly the seeded stock, no more, no less. `kubectl logs` on each pod individually confirmed all 60 of that SKU's "Decided reservation" log lines came from a single pod for the duration of a given consumer-group generation; the other pod logged zero.

**Partition ownership, shown authoritatively rather than inferred from log timing** - `kafka-consumer-groups.sh --describe --group inventory-service`:

```
PARTITION  CONSUMER-ID
1          inventory-service-k3s-1228c99b-...   <- SKU-ELEC-002's partition
0          inventory-service-k3s-1228c99b-...   <- SKU-BOOK-001's partition
2          inventory-service-k3s-7e77cb02-...
```

Every partition has exactly one owning consumer instance - never split. This is the actual mechanism the correctness result above depends on, shown directly rather than inferred.

**Honest finding: 3 partitions and 2 replicas don't guarantee every SKU spreads across both pods.** In this run, both test SKUs (`SKU-ELEC-002` and `SKU-BOOK-001`) happened to hash to partitions 1 and 0, both owned by the *same* consumer instance - so this particular pair of SKUs was processed serially by one pod rather than in parallel across two. By the pigeonhole principle, with 3 partitions and 2 consumers, at least one consumer always owns 2 of the 3 partitions; which SKUs land together depends on hash distribution, not on anything this milestone controls. The correctness guarantee (never two consumers on the same partition) held regardless - the parallelism *upside* is probabilistic, the safety property is not. A production system wanting more guaranteed spread would need more partitions than replicas by a wider margin than this lab's 3:2 ratio.

**Full round trip confirmed**: `inventory.reservation-replied.v1` carries real `InventoryReservationReplied` messages produced by the outbox publisher, with the correct `reserved`/`reason` shape - the transactional outbox path works end to end, not just the in-memory decision.

**Regression check**: `scripts/k6-run.sh smoke` post-deploy - `failed_rate=0`, `checks_rate=1`, `flow_rate=1`. Inventory.Service's presence (2 pods, new Postgres database, 3 new Kafka topics) has no effect on the existing orders pipeline.

## Deployment notes

- New `inventory` Postgres database on the shared compose instance (`scripts/init-inventory-db.sh`, same pattern as Milestone 17's `init-payments-db.sh`).
- `orders-runtime` SealedSecret extended with an `inventory-connection-string` key. The re-seal was done entirely server-side (extract existing values into shell variables never echoed back, `kubectl create secret --dry-run | kubeseal`, then `shred` the temp files) - the plaintext Postgres password was never exposed to the assistant at any point, and a first attempt at a shell-side `cat`/decode was correctly blocked by the permission system before that could happen.
- **Same PreSync-hook-ordering hazard as Milestone 40, new flavor**: `inventory-migrations-m41` (`PreSync`) initially failed with `couldn't find key inventory-connection-string in Secret orders-lab/orders-runtime`, even though the updated `SealedSecret` manifest was already committed and part of the same sync. Cause: the `SealedSecret` resource is an ordinary (non-hook) resource, applied during the *main* Sync phase - which runs *after* all `PreSync` hooks. A `PreSync` job can never be the first consumer of a secret key that's being added in the same sync operation, for exactly the same structural reason a `PreSync` job can't be the first consumer of a Service created in the same sync (Milestone 40's finding). Existing `PreSync` jobs never hit this because their secret keys had already existed since a prior sync. Fixed by applying the updated `SealedSecret` directly via `kubectl apply` ahead of the stuck hook's retry - the sealed-secrets controller decrypted it into the live `Secret` immediately, and the Job's next automatic retry (within its existing `backoffLimit: 3`) succeeded.

## Running it

```bash
scripts/inventory-reservation-concurrency-test.sh
```
