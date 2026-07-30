# Milestone 56: TLA+ Formal Verification of the 4-Step Saga

## Scope

The orchestrated saga (Milestone 22, extended to 4 steps in Milestone 43) has real, documented failure interleavings that integration tests only sample: a reservation reply racing a timeout, a compensation that itself could fail, a duplicate reply arriving after the saga has already moved on. Model checking exhaustively explores every interleaving of a finite state machine, rather than the handful a game day happens to hit. This milestone models the saga's actual guard mechanism in TLA+ and checks it two ways: as built, and as a counterfactual without the guard - to show precisely what that guard is protecting against.

## What's actually being modeled

`SagaOrchestrationStore`'s `TryAdvanceAsync`/`TryCompleteAsync` only mutate the saga's row if its **current** `step` column still equals the step the incoming reply is for (`UPDATE ... WHERE step = @expected_step`, `DELETE ... WHERE step = @expected_step`). A reply for a step the saga has already moved past - because a fresher reply already arrived, or because `SagaTimeoutSweeper` already terminated it (Milestone 43's documented scope note: a timed-out saga is just terminated, never compensated) - finds no matching row state and is silently a no-op. This is the real mechanism that makes "a reply racing a timeout" safe; this milestone checks whether that claim actually holds, and what breaks without it.

Two small TLA+ modules, same state machine (`ReserveRequested -> DecidePayment -> {CommitInventory, ReleaseInventory} -> Done`, plus `TimeoutSweep` reachable from any non-terminal step):

- **`OrderSagaGuarded.tla`**: every action requires `step` to still equal its precondition - the real design.
- **`OrderSagaUnguarded.tla`**: identical, plus one extra action, `BuggyLateReserveReply`, that applies a `ReserveInventory` reply's effect **unconditionally** - modeling what a handler without the SQL `WHERE`-clause guard would do.

Both check the same invariant: **`NoResurrection`** - once the saga reaches `Done`, it must never leave `Done` again.

## Results

**Guarded (the real design)**: `Model checking completed. No error has been found.` Full state space (5 distinct states, exhaustively explored) - `NoResurrection` holds in every reachable state. A late or duplicate reply for a step the saga has already left has no enabled action to apply, exactly like the real `UPDATE`/`DELETE` affecting zero rows.

**Unguarded (the counterfactual)**: `Error: Invariant NoResurrection is violated`, with the exact counterexample TLC found:

```
State 1: step = "ReserveRequested", wasDone = FALSE
State 2: step = "Done" (TimeoutSweep fires - a real, valid transition)
State 3: step = "DecidePayment" (BuggyLateReserveReply fires - no guard, so it's
         enabled even here - a completed saga was just resurrected into an active step)
```

Three steps: start, time out, get resurrected by a stale reply the real code would have silently rejected. This is precisely the hazard the `WHERE step = @expected_step` guard exists for, produced by exhaustive search rather than argued about - and it's a 3-state counterexample that would be easy to miss in an integration test unless someone specifically engineered a timeout to race a delayed reply at the right instant.

## Running it

```bash
cd apps/formal-methods
docker run --rm -v "$(pwd)":/work -w /work eclipse-temurin:21-jre \
  sh -c 'curl -sL -o tla2tools.jar https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar && \
         java -jar tla2tools.jar -workers 2 OrderSagaGuarded.tla && \
         java -jar tla2tools.jar -workers 2 OrderSagaUnguarded.tla'
```
