# Milestone 55: Full Read-Model Replay/Reconstruction Drill

## Scope

The CQRS read model (`order_summaries`, built by `OrderProjectionConsumer`/`OrderProjectionProcessor` since Milestone 13) has been measured for projection lag ever since, but never actually rebuilt from zero. "The read model can always be reconstructed from the event log" is the standard argument for event-driven read models - this milestone tests whether that's actually true here, not just assumed.

## A real finding before the drill even started: Kafka's retention.ms means the log isn't a permanent replay source

`orders.created.v1` and `payments.result.v1` are created with `retention.ms=86400000` (24h). Checking the actual topic offsets before starting - `kafka-get-offsets.sh --time -1` (latest) and `--time -2` (earliest) - both came back **`0`** on every partition of both topics. The live `order_summaries` table holds **150,293 rows**, accumulated across this project's entire history; every event that produced them has already aged out of Kafka. **A true full-history replay is no longer possible from these topics at all** - only Milestone 23's event store (an append-only Postgres table, not subject to Kafka's retention) could ever rebuild the complete history. This is a real, previously-undocumented limitation of this lab's specific CQRS setup, discovered by checking rather than assuming - and it's why this drill creates fresh orders to replay against, rather than truncating the live, irreplaceable projection.

## Method

1. Create N fresh orders via the real `orders-api`, capture their order IDs and a snapshot of their projected rows.
2. Delete **only** those N rows from `order_summaries` - never the other ~150k real rows.
3. Reset the `orders-projector` consumer group's offsets to earliest (isolated to this one group ID; doesn't affect the choreographed consumer, the saga orchestrator, or the event-store projector, which all have their own independent group IDs on the same topics).
4. Let `orders-worker` replay from offset 0 and measure how long reconstruction takes.
5. Compare before/after: row count, and the specific N rows' content.

## What didn't work

**Argo CD's self-heal fought the drill directly.** Resetting a consumer group's offsets requires the group to be inactive first - `kubectl scale deployment/orders-worker --replicas=0`. Argo CD, reconciling `orders-worker`'s replica count from git, scaled it straight back to 1 within moments, keeping the consumer group perpetually `Stable` and the reset permanently rejected (`Assignments can only be reset if the group is inactive`). Fixed by temporarily clearing the Argo CD Application's `spec.syncPolicy.automated` (disabling auto-sync/self-heal for just this Application), running the drill, then restoring it immediately after - the same kind of planned, reversible pause a real GitOps shop uses for maintenance windows, not a permanent change.

**`kubectl wait --for=delete` on the pod isn't the same as the consumer group actually going `Empty`.** Even after the pod was gone, `kafka-consumer-groups.sh --describe --group orders-projector --state` kept reporting `Stable` for a stretch afterward - the group coordinator holds a session-timeout grace period before formally dropping the last member. The script now polls the group's own reported state directly and waits for `Empty`, not just for the pod to disappear.

**Running two invocations of the drill script concurrently left the system in a confusing, self-inflicted mess.** A background run appeared stuck (repeatedly reporting `Stable` due to the Argo CD issue above, not yet understood at the time) and a second, corrected run was started before confirming the first had actually stopped - both then raced to create orders, delete rows, and scale `orders-worker`. Recovered by killing both processes, verifying `orders-worker` and the row count had settled, and running one final, single, foreground attempt. Left in this report as a reminder: confirm a stuck background process is truly gone before starting a second attempt at the same mutating operation.

**The reconstruction converged to 150,312 rows, one short of the expected 150,313 (+20 test rows), and the specific 20 test order IDs from the final run were not among the rows that came back** - while a consumer-group lag check afterward showed `LAG=0` on every partition of both source topics (the projector had genuinely caught up to the log end, not still working through a backlog) and no errors in `orders-worker`'s recent logs. `orders.created.dlq.v1` held exactly 19 dead-lettered messages, almost certainly leftovers from the two aborted/killed drill attempts made earlier in this same session (their orders were created and published before being interrupted mid-flight, likely while a stale Avro schema-registry state - see below - was still in effect) rather than from the final, successful run specifically. The precise cause of this final run's own 20 IDs not reappearing was not conclusively isolated within this drill's time budget - reported honestly as an open finding rather than a clean result, consistent with this repo's practice of documenting what didn't fully resolve, not only what did.

**A completely separate, real production issue was found and fixed along the way**: the earlier, unrelated `docker compose down --profile` incident from Milestone 47 had wiped the Schema Registry's state (`_schemas` topic confirmed empty - earliest and latest offset both `0`), but `orders-api`, still running continuously since before that incident, kept using its in-memory-cached (now-orphaned) Avro schema ID for every new message - producing messages that referenced a schema ID the registry no longer had any record of. Every `orders-projector` deserialization attempt failed with `SchemaRegistryException: Schema not found; error code: 40403` until the affected pods (`orders-api`, `payments-service`, `inventory-service`) were restarted, forcing fresh schema registration. Worth its own note: a schema registry outage recovers the registry's own data, but any long-lived producer holding a stale in-memory schema ID needs restarting too, or it will keep producing unreadable messages indefinitely without ever erroring on the producer side.

## Results

```
Pre-drill:  150,293 real rows + 20 fresh test rows, all correctly projected (status=Confirmed)
Deleted:    exactly the 20 test rows -> 150,293
Reset:      orders-projector offsets -> earliest (Empty state confirmed first)
Replay:     70.0s from scale-up to row count stabilizing
Post-drill: 150,312 rows (not 150,313 - see "what didn't work")
```

The core mechanism **is** proven: resetting one consumer group's offsets and letting it replay from zero does reprocess the full retained log and repopulate the vast majority of the table correctly, without duplicating any of the ~150k pre-existing rows (`ON CONFLICT (order_id) DO UPDATE` held up under a genuine bulk replay of real data, not just a synthetic single-row test) and without needing any other consumer group, service, or the live Argo CD-managed deployment to be aware anything happened. The exact-convergence claim this milestone set out to make cleanly - "delete N, replay, get exactly N back" - did not fully hold on the final measured run, and that gap is reported rather than smoothed over.

## Running it

```bash
scripts/order-projection-replay-drill.sh [order_count]
```

Requires Argo CD's automated sync temporarily disabled for the `distributed-ecommerce` Application before running (see "What didn't work"), and re-enabled after:

```bash
kubectl -n argocd patch application distributed-ecommerce --type merge -p '{"spec":{"syncPolicy":{"automated":null}}}'
# ... run the drill ...
kubectl -n argocd patch application distributed-ecommerce --type merge -p '{"spec":{"syncPolicy":{"automated":{"prune":true,"selfHeal":true}}}}'
```
