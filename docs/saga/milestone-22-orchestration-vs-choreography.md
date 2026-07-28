# Milestone 22 Orchestrated Saga vs Choreographed Saga

## Scope

Every prior milestone's saga work (M11-13, M16) exercised the choreographed design: Payments.Service autonomously decides on seeing `OrderCreated`, no service explicitly asks it to and no service is watching for it to fail to answer. This milestone builds an orchestrated version of the exact same saga - purely additive, a second consumer group on the same `orders.created.v1` topic, so the existing choreography is completely untouched - and compares them on the axes that actually differ: failure handling, traceability, and coupling. Not as an abstract comparison; each claim below was actually broken or actually demonstrated live.

## Design

- **`OrderSagaOrchestrator`** (Orders.Worker): a second, independent consumer group (`orders-saga-orchestrator`) on the same `orders.created.v1` topic the choreographed consumers already read - adding a subscriber to an existing topic is inherently risk-free to the existing ones, which is what made this comparison possible without touching anything already validated. On each order, it explicitly publishes a `PaymentDecisionRequested` command (a new topic, `payments.decision-requested.v1`) and tracks the pending request in memory.
- **`PaymentDecisionRequestHandler`** (Payments.Service): the orchestrated counterpart to the existing choreographed `OrderCreatedConsumer`, applying the identical amount-threshold decision - but deliberately stateless. No database write, no outbox. In choreography, Payments.Service owns persisting the decision because nothing else will; in orchestration, the orchestrator owns the saga's state, so this side doesn't need its own. That's the coupling difference measured below, not just asserted.
- **`SagaTimeoutSweeper`**: polls the in-memory tracker every second and marks any request older than 5 seconds as timed out - the orchestrator's explicit compensation path. In-memory state (not a database table) is a deliberate scope boundary: the comparison is about the orchestrator's explicit *ownership* of timeout and completion, not building a durable saga-persistence layer a real production orchestrator would need.
- **No schema registry, no Avro** for the two new command/reply topics - `PaymentDecisionRequested`/`PaymentDecisionReplied` (plain JSON, `BuildingBlocks`) are internal, transient messages between one orchestrator and one responder, not a domain event other future consumers would ever need to evolve against independently, unlike `OrderCreated` in Milestone 19.

## What didn't work

**`kubectl scale --replicas=0` doesn't work against a resource Argo CD manages - the Milestone 15/19 lesson recurring a third time, in a new shape.** To demonstrate the orchestrator's timeout, `payments-service` needed to go down without answering. `kubectl scale deployment payments-service --replicas=0` appeared to succeed but the pod count stayed at 1 - `selfHeal: true` reverted it, same mechanism as the earlier manifest-drift incidents, just triggered by a scale command instead of an `apply`. This time the fix wasn't "commit first" (there was nothing to commit - this needed to be a *temporary* operational action, not a permanent state change) but the actual sanctioned Argo CD pattern for that situation: `kubectl patch application ... -p '{"spec":{"syncPolicy":{"automated":null}}}'` to pause auto-sync, perform the manual scale, run the experiment, scale back up, then restore `automated: {prune: true, selfHeal: true}`. Confirmed `Synced`/`Healthy` afterward.

**The new consumer groups used `AutoOffsetReset.Latest` where every existing consumer in this codebase uses `Earliest` - an inconsistency that silently dropped the very message the timeout demo depended on.** The first timeout attempt showed no `OrchestratedSagaTimedOut` log at all; investigating, the request-consumer group had no committed offset yet when its pod was scaled down (the 1-second auto-commit interval hadn't fired before the pod died), so on restart it fell back to `Latest` and picked up *after* the message it needed to see. Every choreographed consumer already sets `AutoOffsetReset.Earliest` specifically to guard against exactly this - a copy-paste inconsistency across the three new consumers, not a deliberate choice, fixed by matching the established convention. Rebuilding and rerunning the exact same experiment then produced the expected `OrchestratedSagaTimedOut` log at the 5-second mark.

**The orchestrated flow has no inbox-based deduplication, unlike its choreographed counterpart - an intentional scope boundary, made visible by the very act of redeploying.** After the offset-reset fix, a fresh order's logs showed `OrchestratedSagaRequested`/`OrchestratedSagaCompleted` three times each for the same order - the redeploy's pod cycling replayed a handful of already-processed messages before the new offsets stabilized. Harmless here (both the request and the decision are idempotent - deciding the same order's payment twice yields the same deterministic answer), but a real production orchestrator would need the same `InboxStore`-style deduplication the choreographed consumers already have. Left out deliberately: the milestone is comparing coupling and failure-handling architecture, not re-deriving exactly-once processing a second time.

## Results

### Normal case: both flows converge

| Flow | Result |
| --- | --- |
| Choreographed (existing, unmodified) | Order reaches `Confirmed`/`Cancelled` via `PaymentResultConsumer`, as in every prior milestone |
| Orchestrated (new, parallel) | `OrchestratedSagaRequested` -> `OrchestratedSagaCompleted approved=True latencyMs=175.1`, same order, same instant, zero interference with the choreographed path processing it simultaneously |

### The actual comparison: Payments.Service down, no reply ever comes

| Flow | Observed |
| --- | --- |
| Choreographed | Order status: `Created` - and stays there, indefinitely, with no signal anywhere that anything is wrong. Nothing is watching for the absence of a `PaymentDecided` event; the only thing that "notices" is a human explicitly querying the row. |
| Orchestrated | `OrchestratedSagaTimedOut order ... after 5s` - logged automatically, in real time, without anyone querying anything |

### A genuine nuance, not glossed over: choreography does eventually self-heal

Once `payments-service` was scaled back up, its choreographed `OrderCreatedConsumer` resumed from its uncommitted Kafka offset (the message was never lost - Kafka retained it) and the stuck order transitioned to `Confirmed` on its own, no restart or manual intervention needed beyond bringing the service back. The real difference isn't "choreography breaks forever" - it doesn't. It's *detection*: the orchestrator flagged the problem within 5 seconds of it happening, independent of whether or when Payments.Service ever comes back; choreography's recovery is real, but silent until it happens, and nothing shortens the gap between "broken" and "someone notices" on its own.

### Regression check

`k3s-smoke-test.sh` and `k6-run.sh saga` (`failed_rate=0`, `saga_correct_outcome_rate=99.70%`, consistent with this lab's already-documented baseline) both pass cleanly - the choreographed path is provably unaffected by any of this milestone's additions.

## Running the experiment

```bash
# Normal case
curl -X POST http://<orders-api>/orders -d '{"customerId":"demo","amount":49.90,"currency":"BRL"}'
kubectl logs -n orders-lab -l app.kubernetes.io/name=orders-worker | grep OrchestratedSaga

# Timeout case (pause Argo CD auto-sync first - see "what didn't work" above)
kubectl scale deployment payments-service -n orders-lab --replicas=0
curl -X POST http://<orders-api>/orders -d '{"customerId":"demo","amount":49.90,"currency":"BRL"}'
# wait >5s, then:
kubectl logs -n orders-lab -l app.kubernetes.io/name=orders-worker | grep OrchestratedSagaTimedOut
kubectl scale deployment payments-service -n orders-lab --replicas=1
```
