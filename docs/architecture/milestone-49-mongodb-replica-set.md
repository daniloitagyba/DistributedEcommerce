# Milestone 49: MongoDB Replica Set - Stale Reads and the w:majority Cost, Measured

## Scope

Catalog.Service's MongoDB (`compose/compose.yaml`'s `mongodb` service, since Milestone 40) runs standalone: `image: mongo:8.0`, no `--replSet`. No replication means no write concern choice, no read preference, no failover - `w` and `readPreference` are configurable on every MongoDB driver call in this codebase, but with one node they're not really *choices*, since there's nothing else to read from or wait on. This milestone proves what replication actually buys and costs, against an isolated 3-node replica set, with real numbers.

## Design: isolated, not the live store

Same judgment as Milestone 27's `postgres-ha` and Milestone 47's `kafka-quorum-demo`: `compose/compose.yaml` gets `mongo-rs1/2/3` (a 3-node `rs-demo` replica set) gated behind the `mongo-replicaset-demo` profile, entirely separate from the standalone `mongodb` service Catalog.Service actually depends on.

**One member, `mongo-rs3`, is configured with `secondaryDelaySecs: 5` and `priority: 0`** (a replication delay MongoDB requires pairing with zero election priority - a lagging node can't be allowed to become primary) - a deliberate, fixed replication lag, not a race against real timing. This is what makes the stale-read proof deterministic instead of "usually reproducible": the demo doesn't need to get lucky beating replication to a read, it forces the read to land inside a guaranteed 5-second window where the secondary is known to not yet have the write.

## Results

**Write latency, `w:1` vs `w:majority`** (200 inserts each, same 3-node cluster, same network):

| Write concern | Total (200 docs) | Avg/doc |
|---|---|---|
| `w:1` (primary ack only) | 1304ms | **6.52ms** |
| `w:majority` (2 of 3 nodes) | 4102ms | **20.51ms** |

`w:majority` cost **~3.1x** the latency of `w:1` here - on a single Docker host, same network, no simulated WAN latency between members. This is the real, unavoidable cost of "don't tell the caller it's durable until a majority actually has it": every write blocks on a round trip to at least one more node, not just an fsync on the node you're already talking to. Cross-AZ or cross-region replica members would widen this gap further, not narrow it.

**Stale read against the deliberately-lagging secondary**:

```
marker written to primary (w:1)
Immediate read from mongo-rs3 (readPreference=secondary): NOT_FOUND
[wait 6s, past the 5s delay]
Same read, same secondary: FOUND
```

A write the primary already acknowledged was invisible to a `secondaryPreferred`/`secondary` read against a lagging member moments later, and then became visible once replication caught up - the textbook "read-your-writes" hazard, produced on demand rather than argued about. Any code path in this codebase that started routing reads to secondaries for scale (the way Milestone 8's read replicas exist for exactly this reason on the Postgres side) would need to either accept this staleness window, pin session reads to the primary after a write, or use a causally-consistent session - not just flip a `readPreference` setting and assume it's free.

## Fix recommended for the live store

Not applied to the live standalone `mongodb` service in this milestone - see "Design" above. If Catalog.Service's read-heavy, infrequently-written product catalog ever needs to scale reads or survive a node loss, the demonstrated pattern is: a real replica set, `w:majority` for the (rare) catalog writes given its measured ~3x-but-still-single-digit-to-tens-of-milliseconds cost, and secondary reads only where staleness is acceptable (a product listing page) rather than unconditionally.

## Running it

```bash
scripts/mongo-replica-set-test.sh 200
```

Tears down the demo cluster's containers afterward with explicit service names - `docker compose stop mongo-rs1 mongo-rs2 mongo-rs3 mongo-rs-init` then `rm -f`, never a bare `docker compose down` scoped by `--profile` (see Milestone 47 for why that command is unsafe on this shared host).
