# Milestone 58: Deterministic Simulation Testing of the Saga

## Scope

Complements Milestone 56's TLA+ exhaustive proof with the FoundationDB/TigerBeetle-style alternative: instead of symbolically exploring every reachable state, run actual code under a seeded pseudo-random schedule, and get the defining promise of deterministic simulation testing for free - a failing run is reproduced **exactly** by rerunning its seed, not chased down as a one-off flaky race.

Scoped down to the saga's core state machine, as the milestone's own notes anticipated: `SagaOrchestrationStore` and `OrderSagaReplyConsumer` are tightly coupled to `NpgsqlDataSource`/`Confluent.Kafka` directly, not structured for full network/clock virtualization today. Re-deriving the same state machine Milestone 56 modeled in TLA+ - this time as running C#, not a symbolic spec - is the explicitly-allowed scoped-down version, not a compromise made silently.

## Design

`SagaDeterministicSimulationTests` models the same guard Milestone 56 checked: `Guarded` only transitions if the current step matches the event's precondition (mirroring `WHERE step = @expected_step`); `Unguarded` adds one event, `LateReserveReply`, that applies unconditionally regardless of current step - modeling a handler without that guard.

Each simulated run takes a fixed pool of events (a normal reservation-approved reply, a payment decline, the resulting release reply, a timeout sweep, and a late/duplicate reserve reply) and shuffles their delivery order with a **seeded** Fisher-Yates shuffle - this is the simulated network reordering. Same seed, same shuffle, same outcome, every time. The invariant checked after every event is the same `NoResurrection` from Milestone 56: once the modeled saga reaches `Done`, it must never leave `Done` again.

## Results

**`GuardedTransitionNeverResurrectsAcrossManySeeds`** - 5,000 seeds, zero violations. Every one of the 120 possible orderings of the 5-event pool (and the seeded shuffle covers all of them many times over across 5,000 runs) leaves the guarded model's `NoResurrection` invariant intact, matching Milestone 56's exhaustive TLA+ result from an entirely different verification technique.

**`UnguardedTransitionResurrectsAndTheFailingSeedReproducesExactly`** - found and reproduced:

```
Failing seed: 0
Trace: ReserveReplyOk=>DecidePayment -> PaymentDeclined=>ReleaseInventory ->
       ReleaseReply=>Done -> LateReserveReply=>DecidePayment
```

Seed `0` was already enough: the saga runs its normal path to completion (reserved, payment declined, inventory released, `Done`), and then the unconditional `LateReserveReply` - modeling a redelivered or simply-delayed `ReserveInventory` reply arriving after everything already finished - resurrects it straight back into `DecidePayment`. Rerunning seed `0` twice in the same test produces the identical trace both times, character for character - the core deterministic-simulation guarantee, verified rather than assumed.

## Why both this and Milestone 56

TLA+ proves the property holds (or doesn't) across the **entire** reachable state space of the model - a stronger guarantee than any finite number of simulated runs can give. Deterministic simulation proves the property against **actual executable code**, catching the class of bug a hand-translated formal model could itself get wrong (a mismatch between what the spec says and what the code does) - and gives a bug report that includes a seed anyone can rerun to see the exact same failure, which a symbolic TLC counterexample trace doesn't directly hand you in a form you can execute. Neither replaces the other; together they check the same claim from two independent directions.

## Running it

```bash
cd apps
dotnet test tests/Orders.UnitTests/Orders.UnitTests.csproj --filter 'FullyQualifiedName~SagaDeterministicSimulationTests' --logger 'console;verbosity=detailed'
```
