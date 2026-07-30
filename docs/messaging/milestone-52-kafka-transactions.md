# Milestone 52: Kafka Transactions (EOS) Alongside Outbox/Inbox

## Scope

Two prior milestones flagged the same gap and deliberately didn't close it: `docs/saga/milestone-22-orchestration-vs-choreography.md` notes the orchestrated saga "has no inbox-based deduplication, unlike its choreographed counterpart," and `docs/data/milestone-23-event-sourcing.md` says the event-sourced read side has "no inbox-based deduplication" either, both calling out that "a real production orchestrator would need the same `InboxStore`-style deduplication the choreographed consumers already have." Kafka transactions are the other documented way to close that gap - not by deduplicating on read, but by making the consume-transform-produce cycle itself atomic. This proves the mechanism actually works, measures what it costs, and shows precisely where its guarantee stops.

## Design

`KafkaTransactionsTests` (real Redpanda via Testcontainers, not the live cluster - this is a client-library/protocol feature, better proven in a hermetic, repeatable test than raced against real timing on a shared host) runs the same fault twice:

1. **Non-transactional**: consume a message, produce a derived output message, "crash" before committing the input offset (modeled by simply not calling `Commit`). A new consumer in the same group - simulating a restart - re-fetches the same message from the last committed offset (unchanged) and reprocesses it, producing a second output message and committing this time.
2. **Transactional**: same fault, but the produce and the offset registration (`SendOffsetsToTransaction`) happen inside one Kafka transaction, and the simulated crash is modeled by `AbortTransaction()` instead of `CommitTransaction()` - the actual effect on what a `read_committed` consumer sees downstream is identical to a real crash before commit, without needing to actually kill a process mid-flight to prove it.

## Results

**Non-transactional**: 2 output messages for 1 logical input - the crash-orphaned write from the first attempt and the real one from the retry both landed, because nothing tied the produce to the offset commit atomically.

**Transactional**: exactly 1 output message. The aborted transaction's produce was never visible to the `read_committed` consumer that counted them - Kafka discards everything written inside an aborted transaction, and the paired `SendOffsetsToTransaction` call meant the consumer offset was never really advanced either, so the retry correctly re-read the same input message and produced the single output that actually counts.

**Latency cost** (100 messages, Redpanda, one transaction per message - the worst case for this saga's shape, where each step is a single message, not a batch that would amortize the transaction overhead across many records):

| Mode | Total | Avg/msg |
|---|---|---|
| Non-transactional | 752ms | 7.52ms |
| Transactional (1 txn/msg) | 1287ms | 12.87ms |

**~1.7x** the latency per message, for a guarantee that Outbox+Inbox already provides this codebase a different way - and Outbox+Inbox costs its own overhead too (an extra Postgres table, a poller, an inbox-dedup check on every consume). Neither is free; the choice is about which system should own the guarantee.

## Why this still doesn't cross the Postgres boundary

Kafka transactions make the consume-transform-**produce** cycle atomic - entirely within Kafka. `OrderSagaReplyConsumer`'s real job on every reply isn't just "produce the next step's request message," it's also "advance `saga_orchestration_states` in Postgres" (`SagaOrchestrationStore.TryAdvanceAsync`). A Kafka transaction has no way to also enlist that Postgres write - it's a different system, with its own commit protocol. Wrapping the Kafka half in a transaction and leaving the Postgres write outside it just moves the duplicate-write risk to a different boundary: a crash between `CommitTransaction()` and the Postgres update would leave Kafka correctly advanced (the request published exactly once) while Postgres still shows the *old* step, and a naive retry would produce the request a second time after all - the inbox check this codebase already has (`InboxStore`, used by the choreographed consumers) is what actually closes *that* gap, by making the write to Postgres itself idempotent against redelivery, regardless of what Kafka guarantees on its own side.

This is the actual, general shape of "EOS doesn't cross a database boundary": Kafka transactions guarantee exactly-once *within Kafka*; the moment a step also writes to an external store, that store needs its own idempotency mechanism - Outbox+Inbox, a unique constraint, a fencing token - because no distributed transaction spans both systems here. The two patterns aren't competing solutions to the same problem; Kafka transactions solve the Kafka-to-Kafka hop, Outbox+Inbox solves the Kafka-to-Postgres hop, and a fully rigorous saga step needs both if it does both kinds of write.

## Running it

```bash
cd apps
dotnet test tests/Orders.IntegrationTests/Orders.IntegrationTests.csproj --filter 'FullyQualifiedName~KafkaTransactionsTests'
```
