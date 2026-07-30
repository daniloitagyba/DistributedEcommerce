# Milestone 50: Clock Skew via Chaos Mesh TimeChaos

## Scope

Chaos Mesh has been installed since Milestone 31 and has run pod-kill (31) and network-partition (37) game days since - `TimeChaos` had never been used. Every timeout, TTL, and lease comparison in this codebase assumes the local wall clock is trustworthy. This milestone skews a real pod's clock and proves one concrete place that assumption breaks: `SagaTimeoutSweeper`.

## The mechanism under test

`SagaOrchestrationStore.ClaimTimedOutAsync` computes `cutoff = now - timeout` where `now` is `DateTimeOffset.UtcNow` - `orders-worker`'s own local clock - and `timeout` is `SagaOrchestrationOptions.TimeoutSeconds` (default 5s). A saga's `requested_at` is written once, in real time, when it starts. If `orders-worker`'s clock runs ahead of real time, `now` (and therefore `cutoff`) is inflated, and a saga that's only been open for a couple of *real* seconds can already be `<= cutoff` - swept as timed out, purely because the sweeper's own clock lied to it, not because anything actually took too long.

## Method

A real saga's natural lifecycle (reserve -> decide payment -> commit) usually completes in well under a second - not enough of a window to reliably inject a fault mid-flight before the row completes and deletes itself. Instead: `TimeChaos` is applied to `orders-worker` first (`+180s`, `CLOCK_REALTIME`, 60s duration - `kubernetes/chaos-experiments/timechaos-orders-worker-clock-skew.yaml`), then one synthetic `saga_orchestration_states` row is inserted directly via `psql`, with Postgres's own `NOW()` (a different container, never skewed - a genuinely real timestamp) and `step = 'DecidePayment'`. This exercises the exact same `SagaTimeoutSweeper`/`ClaimTimedOutAsync` code path a real saga would hit; only the row's origin is synthetic, not the mechanism being proven.

## Results

```
TimeChaos applied and confirmed injected (AllInjected)
Row inserted, requested_at = real NOW()
Row count immediately after insert: 1
[wait 2 real seconds]
Row count after 2.46s real time: 0

orders-worker log:
  "OrchestratedSagaTimedOut order 704b505e-... at step DecidePayment after 5s"
```

**2.46 real seconds elapsed - well under the configured 5-second timeout - and the sweeper logged and deleted the row as timed out "after 5s".** The 5 seconds it believed had passed never happened; only its own skewed clock did. A control run without any chaos, checked at the same ~2-second mark, left an identical row untouched (confirmed separately: still present at 2s, only genuinely swept once real elapsed time itself crossed 5s) - isolating the effect to the clock skew specifically, not to the timeout margin being tight.

Chaos reverted cleanly after the experiment: `orders-worker`'s pod stayed `2/2 Running` throughout (no crash, no additional restarts beyond an unrelated one from earlier in the session), and its clock read normal real time again immediately after `TimeChaos` was deleted.

## What this means beyond the sweeper

The sweeper was the concrete, provable case, but the same hazard applies anywhere this codebase compares a locally-read `DateTimeOffset.UtcNow`/`DateTime.UtcNow` against a timestamp that originated elsewhere or earlier: JWT `exp` validation (a clock running behind could accept an already-expired token; running ahead could reject a valid one early), the outbox poller's retry backoff windows, and Kubernetes `Lease` renewal timing for the leader-election sweeper gate itself (Milestone 36) - `client-go`'s leader-election library compares local `time.Now()` against the `Lease`'s recorded renewal time using the same kind of local-clock trust this milestone just showed can't be assumed. None of those were independently reproduced here; the sweeper case is the one concretely measured, and the mechanism generalizes directly.

Physical clocks are not a reliable source of ordering across processes - the standard argument for logical clocks (Lamport clocks, vector clocks, HLC) applies for exactly this reason. This codebase has none; every "how long has this been running" check is done by trusting local wall time.

## Running it

```bash
kubectl apply -f kubernetes/chaos-experiments/timechaos-orders-worker-clock-skew.yaml
scripts/clock-skew-saga-timeout-test.sh
```

The script applies the chaos itself (idempotent if already applied) and deletes it afterward - not left running, same convention as the Milestone 31/37 game days.
