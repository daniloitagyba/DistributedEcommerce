namespace Orders.UnitTests;

/// <summary>
/// Milestone 58: complements Milestone 56's TLA+ exhaustive proof with a
/// FoundationDB/TigerBeetle-style deterministic simulation - the same
/// saga state machine and the same NoResurrection property, but exercised
/// by running actual code under a seeded pseudo-random schedule instead of
/// symbolic exploration. The point of "deterministic" here: every event's
/// delivery order and delay is derived purely from a seeded Random, so a
/// failing run is reproduced exactly by rerunning the same seed - no
/// flaky, one-off race to chase down, unlike a real timing-dependent bug.
///
/// Scoped down to the saga's core state machine, not the full
/// network/clock-virtualized I/O stack a production deterministic
/// simulation framework would use - SagaOrchestrationStore and
/// OrderSagaReplyConsumer are tightly coupled to Npgsql/Confluent.Kafka
/// directly and aren't structured for that level of virtualization today;
/// re-deriving the same state machine Milestone 56 already modeled in
/// TLA+, this time as running code, is the scoped-down version explicitly
/// allowed for this milestone.
/// </summary>
public sealed class SagaDeterministicSimulationTests
{
    private enum Step
    {
        ReserveRequested,
        DecidePayment,
        CommitInventory,
        ReleaseInventory,
        Done
    }

    private enum EventKind
    {
        ReserveReplyOk,
        ReserveReplyInsufficient,
        PaymentApproved,
        PaymentDeclined,
        CommitReply,
        ReleaseReply,
        TimeoutSweep,
        LateReserveReply
    }

    /// <summary>
    /// The saga's real transition rule: SagaOrchestrationStore.TryAdvanceAsync/
    /// TryCompleteAsync only mutate the row if its CURRENT step still equals
    /// the step the event is for - a stale event finds nothing to do.
    /// </summary>
    private static Step? Guarded(Step current, EventKind evt) => (current, evt) switch
    {
        (Step.ReserveRequested, EventKind.ReserveReplyOk) => Step.DecidePayment,
        (Step.ReserveRequested, EventKind.ReserveReplyInsufficient) => Step.Done,
        (Step.DecidePayment, EventKind.PaymentApproved) => Step.CommitInventory,
        (Step.DecidePayment, EventKind.PaymentDeclined) => Step.ReleaseInventory,
        (Step.CommitInventory, EventKind.CommitReply) => Step.Done,
        (Step.ReleaseInventory, EventKind.ReleaseReply) => Step.Done,
        (not Step.Done, EventKind.TimeoutSweep) => Step.Done,
        (Step.ReserveRequested, EventKind.LateReserveReply) => Step.DecidePayment,
        _ => null // no matching transition - the event is a no-op, exactly like a 0-row UPDATE
    };

    /// <summary>
    /// The counterfactual: LateReserveReply applies unconditionally,
    /// regardless of current step - what a handler without the SQL
    /// WHERE-clause guard would do.
    /// </summary>
    private static Step? Unguarded(Step current, EventKind evt) =>
        evt == EventKind.LateReserveReply ? Step.DecidePayment : Guarded(current, evt);

    private sealed record SimulationResult(bool NoResurrectionHeld, IReadOnlyList<(EventKind Event, Step StepAfter)> Trace);

    /// <summary>
    /// Drives one simulated saga run: a fixed pool of events that can
    /// occur (the normal happy-path replies, a timeout, and a late/stale
    /// reserve reply modeling redelivery or a race with the timeout
    /// sweeper), delivered in an order and at delays entirely determined
    /// by <paramref name="seed"/> - the same seed always produces the
    /// same schedule and therefore the same outcome.
    /// </summary>
    private static SimulationResult Simulate(int seed, Func<Step, EventKind, Step?> transition)
    {
        var random = new Random(seed);
        var pending = new List<EventKind>
        {
            EventKind.ReserveReplyOk,
            EventKind.PaymentDeclined,
            EventKind.ReleaseReply,
            EventKind.TimeoutSweep,
            EventKind.LateReserveReply
        };

        // Shuffle deterministically per-seed (Fisher-Yates) - this is the
        // "network reordering" being simulated: the late reply, the
        // timeout, and the real replies can arrive in any order.
        for (var i = pending.Count - 1; i > 0; i--)
        {
            var j = random.Next(i + 1);
            (pending[i], pending[j]) = (pending[j], pending[i]);
        }

        var step = Step.ReserveRequested;
        var wasDone = false;
        var trace = new List<(EventKind, Step)>();

        foreach (var evt in pending)
        {
            var next = transition(step, evt);
            if (next.HasValue)
            {
                step = next.Value;
            }

            trace.Add((evt, step));

            if (step == Step.Done)
            {
                wasDone = true;
            }
            else if (wasDone)
            {
                // wasDone was set on an earlier event and step is no
                // longer Done - the saga was resurrected.
                return new SimulationResult(NoResurrectionHeld: false, trace);
            }
        }

        return new SimulationResult(NoResurrectionHeld: true, trace);
    }

    [Fact]
    public void GuardedTransitionNeverResurrectsAcrossManySeeds()
    {
        for (var seed = 0; seed < 5_000; seed++)
        {
            var result = Simulate(seed, Guarded);
            Assert.True(result.NoResurrectionHeld, $"Guarded model resurrected at seed {seed}: {string.Join(" -> ", result.Trace)}");
        }
    }

    [Fact]
    public void UnguardedTransitionResurrectsAndTheFailingSeedReproducesExactly()
    {
        int? failingSeed = null;
        for (var seed = 0; seed < 5_000; seed++)
        {
            var result = Simulate(seed, Unguarded);
            if (!result.NoResurrectionHeld)
            {
                failingSeed = seed;
                break;
            }
        }

        Assert.True(failingSeed.HasValue, "Expected the unguarded model to resurrect a completed saga for at least one seed in range.");

        // The deterministic-simulation promise: rerunning the exact same
        // seed reproduces the exact same failing trace, not just "a"
        // failure - no flakiness to chase, unlike a real timing race.
        var first = Simulate(failingSeed!.Value, Unguarded);
        var second = Simulate(failingSeed.Value, Unguarded);

        Console.WriteLine($"Failing seed: {failingSeed.Value}");
        Console.WriteLine($"Trace: {string.Join(" -> ", first.Trace.Select(e => $"{e.Event}=>{e.StepAfter}"))}");

        Assert.False(first.NoResurrectionHeld);
        Assert.False(second.NoResurrectionHeld);
        Assert.Equal(first.Trace, second.Trace);
    }
}
