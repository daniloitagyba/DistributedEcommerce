# Milestone 51: Rebalance vs the Per-SKU Serialization Guarantee

## Scope

Milestone 41's whole argument for oversell-safe stock reservation is that Kafka partition ownership serializes same-SKU requests without a database lock: the default partitioner routes every message for a given key to the same partition, and only one consumer instance in the group ever owns a partition at a time. That guarantee has two documented-but-never-tested ways to break: (a) a consumer-group rebalance briefly overlapping old and new partition owners, and (b) changing the topic's partition count, which changes the key-to-partition mapping for every key, retroactively.

## Result 1: partition resize breaks the mapping - proven

Kafka's default partitioner is `murmur2(key) % num_partitions`. Change `num_partitions` and the formula's output changes for most keys - there is no rebalancing of *existing* key assignments, only a new modulus applied going forward. Proven against an isolated demo topic (`chaos-demo.sku-partitioning.v1`, created fresh, deleted after - never `inventory.reservation-requested.v1` itself, so the live consumer group's real traffic was never touched): 10 SKUs produced under 3 partitions (mirroring the live topic's partition count), the topic resized to 6 partitions, the same 10 SKUs produced again.

| SKU | partition @ 3 | partition @ 6 | changed? |
|---|---|---|---|
| SKU-A | 1 | 4 | **YES** |
| SKU-B | 2 | 2 | no |
| SKU-C | 2 | 2 | no |
| SKU-D | 1 | 1 | no |
| SKU-E | 2 | 5 | **YES** |
| SKU-F | 0 | 0 | no |
| SKU-G | 0 | 0 | no |
| SKU-H | 0 | 3 | **YES** |
| SKU-I | 2 | 2 | no |
| SKU-J | 2 | 2 | no |

**3 of 10 SKUs landed on a different partition after the resize.** Had this been the live `inventory.reservation-requested.v1` topic, a reservation request for `SKU-A` produced before the resize (owned by whichever `inventory-service` pod held partition 1) and another for `SKU-A` produced after (now owned by whichever pod holds partition 4) could be picked up and processed by two *different* pods with no coordination between them - the exact same-SKU mutual exclusion Milestone 41 measured and relied on, silently gone for that SKU from the moment the topic was resized, with no error, no log line, nothing to notice by unless someone was specifically looking for a stock discrepancy afterward.

The practical implication: **never increase partitions on a topic whose consumers depend on key-based serialization**, not even to scale up processing capacity - the standard, supported way to do that safely is to create a new topic with the target partition count and cut over, not `--alter --partitions`.

## Result 2: rebalance protocol - reasoned, not independently reproduced

Neither `ReservationRequestedConsumer` nor its siblings set `partition.assignment.strategy` explicitly, so Inventory.Service's consumer group uses librdkafka's default: `range,roundrobin` - both **eager** protocols. Eager rebalancing revokes *every* partition from *every* consumer in the group before reassigning any of them - a real, measurable pause (no SKU on any partition is processed until reassignment completes), but one that structurally prevents the specific hazard being asked about here: there is no window where two consumers simultaneously believe they own the same partition, because ownership is fully torn down before it's rebuilt.

`cooperative-sticky` would shrink that pause to only the partitions actually changing hands, leaving untouched partitions (and the SKUs on them) processing continuously through the rebalance - better throughput, but it depends on both parties correctly completing a two-phase handoff (the losing consumer must stop processing a partition before the gaining consumer starts), which is a protocol-level guarantee, not something this milestone independently instrumented and measured. Left as a reasoned discussion rather than a claimed proof, the same way Milestones 22 and 23 were explicit about scope boundaries they didn't re-derive.

## Running it

```bash
scripts/kafka-partition-resize-sku-test.sh
```

Creates and deletes its own isolated topic; never touches `inventory.reservation-requested.v1`.
