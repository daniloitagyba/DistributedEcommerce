# Milestone 47: Acks.All Is a Lie Without Quorum - Proven Against a Real 3-Broker Cluster

## Scope

Every producer in this codebase sets `Acks.All`: `Orders.Worker/Program.cs:110`, `Payments.Service/Program.cs:69`, `Inventory.Service/Program.cs:64`, `Orders.Infrastructure/InfrastructureServiceCollectionExtensions.cs:60`. But every topic the live `kafka` service creates (`compose/compose.yaml`'s `kafka-init`) is `--replication-factor 1` on a single KRaft broker (`KAFKA_NODE_ID: 1`). `acks=all` means "wait for every in-sync replica to acknowledge" - with exactly one replica, that's the same as `acks=1`. The guarantee is configured; it doesn't exist.

This milestone proves the actual quadrant - `min.insync.replicas` × `unclean.leader.election.enable` - against a real, isolated 3-broker cluster, not by reasoning about it.

## Design: an isolated cluster, not the live one

Same judgment call as Milestone 27's `postgres-ha`: this experiment kills brokers and forces unclean leader elections on purpose, against traffic that matters. `compose/compose.yaml` gets a second, separate KRaft cluster - `kafka-ctrl1/2/3` (controller-only) and `kafka-q1/q2/q3` (broker-only), gated behind the `kafka-quorum-demo` Compose profile so it costs nothing on the shared host except while the experiment runs - entirely separate from the single-broker `kafka` service the live `orders-lab` saga pipeline depends on.

**Controller quorum is deliberately split from the broker nodes**, which is not how the live single-node `kafka` service (or a naive "just add 2 more brokers" version of this cluster) is built. First attempt combined `broker,controller` on the same 3 nodes, mirroring the production service's `KAFKA_PROCESS_ROLES: broker,controller`. Pausing 2 of those 3 nodes to shrink the *data*-plane ISR for the replica experiment also cut the KRaft *controller* quorum's own voters from 3 to 1 - below the majority the metadata log itself needs to make progress. Every `kafka-topics.sh --describe` call then hung until timing out with `Timed out waiting to send the call: listTopics` - not a symptom of the replica experiment at all, but of the control plane losing its own quorum. Decoupling the two (3 controller-only nodes, always healthy; 3 broker-only nodes, the ones actually paused/killed) is what makes it possible to degrade the thing being tested without also taking down the metadata layer needed to observe it.

## What didn't work

**Combined broker+controller roles conflated two different quorums** (above) - the fix was architectural, not a workaround: dedicated controller nodes.

**An unguarded pipe killed the test script silently.** `isr=$(echo "$isr_line" | grep -oE 'Isr: [0-9,]+' | cut -d' ' -f2)` has no `|| true`. The very first poll of a freshly-paused cluster has no match yet (ISR hasn't shrunk), so `grep` exits 1; under `pipefail`, that failure propagates through `cut` regardless of `cut`'s own exit code, and under `set -e` the whole script died right there - before printing a single loop iteration, silently jumping straight to the `EXIT` trap's cleanup message. Every earlier "no output, straight to cleanup" run was this, not a hang. Fixed by guarding every such substitution with `|| true`.

**`kafka-console-producer.sh`'s process exit code doesn't reflect per-record failures.** In `safe` mode, every one of 100 messages was correctly rejected with `NotEnoughReplicasException` (visible in the logged callback), but the producer process itself still exited `0`. The real signal had to be the `NotEnoughReplicasException` count in its output, not its exit code - a good reminder that "the client didn't error" and "the write succeeded" are different claims, in either direction.

**`delivery.timeout.ms` has a hard constraint** (`>= linger.ms + request.timeout.ms`) that a naive `--producer-property` combination violated, failing the producer's own startup (`KafkaException: Failed to construct kafka producer`) before it could even try to send.

## Method

Same physical fault, two configs:

1. Pause both followers (`docker pause` - frozen, not killed, so they fall out of the ISR without triggering an election yet).
2. Wait for the ISR to shrink to the leader alone (`replica.lag.time.max.ms`, default 30s).
3. Produce 100 `acks=all` messages with only the leader reachable.
4. `unsafe` config only: kill the leader (`SIGKILL`), unpause the followers, let one of the now-out-of-sync followers win a forced election, then consume from the beginning and count what survived.
5. `safe` config only: unpause the followers, confirm the ISR heals to all 3, and retry the same batch.

## Results

**`min.insync.replicas=1`, `unclean.leader.election.enable=true`** (the dangerous, but common-to-default-to, combination):

```
Producer exit code: 0   (100/100 messages accepted, acks=all satisfied by the leader alone)
[leader killed]
New leader: broker 1    (an out-of-sync follower, elected only because unclean election allows it)
Consumed from earliest: 0/100
==> DATA LOSS CONFIRMED: 100 acked message(s) never made it to the elected leader.
```

Every message the producer believed was durably written - clean exit code, no errors - is gone. `acks=all` was honored exactly as configured; the configuration itself just didn't provide durability, because `min.insync.replicas=1` means "all" is satisfied by one replica, and `unclean.leader.election.enable=true` allowed a replica that never saw those writes to become the leader of record.

**`min.insync.replicas=2`, `unclean.leader.election.enable=false`** (same fault, safe config):

```
NotEnoughReplicasException count: 100/100
==> SAFE UNAVAILABILITY CONFIRMED: every write was correctly refused - no data was silently lost
    because the write was never falsely acknowledged.

[followers unpaused, ISR healed: 1,2,3]
Retry after ISR healed - exit code: 0
```

Identical fault, opposite outcome, purely from two config values: instead of silently losing 100 acknowledged messages, the producer was correctly told, 100 times, that durability couldn't be guaranteed right now - and the moment the ISR recovered, the same batch went through cleanly. This is the entire point of `min.insync.replicas`: it turns silent data loss into visible, recoverable unavailability.

## Fix recommended for the live cluster

Not applied to the live single-broker `kafka` service in this milestone - see "Design" above for why. The demonstrated fix for any topic that actually needs the `Acks.All` this codebase already requests: run at least 3 brokers, `--replication-factor 3`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`. On a single broker, `Acks.All` should be treated as documentation of intent, not a guarantee currently in force.

## Running it

```bash
scripts/kafka-quorum-durability-test.sh unsafe 100   # min.insync.replicas=1, unclean election - loses acked data
scripts/kafka-quorum-durability-test.sh safe 100      # min.insync.replicas=2, no unclean election - safe unavailability instead
```

Tears the demo cluster's containers down afterward with explicit service names - never `docker compose down` scoped by `--profile`. That command doesn't actually scope teardown to the named profile; it stops and removes every container in the project, profiled or not, including the ones the live saga pipeline depends on. Found out by doing it once against this same host, recovered with `docker compose up -d --wait` (named volumes meant no data was actually lost, only a brief connection outage - one `orders-worker` pod restart, self-healed).
