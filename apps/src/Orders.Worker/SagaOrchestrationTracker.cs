using System.Collections.Concurrent;

namespace Orders.Worker;

public sealed record PendingSaga(string CorrelationId, DateTimeOffset RequestedAt);

/// <summary>
/// In-memory saga state for Milestone 22's orchestrated comparison - a
/// deliberate scope choice over a dedicated database table, since the point
/// being demonstrated is the orchestrator's explicit ownership of timeout
/// and completion, not durable saga persistence (a real production
/// orchestrator would need that; this comparison doesn't).
/// </summary>
public sealed class SagaOrchestrationTracker
{
    private readonly ConcurrentDictionary<Guid, PendingSaga> _pending = new();

    public void TrackRequested(Guid orderId, string correlationId, DateTimeOffset requestedAt)
    {
        _pending[orderId] = new PendingSaga(correlationId, requestedAt);
    }

    public bool TryCompleteReplied(Guid orderId, out PendingSaga saga)
    {
        return _pending.TryRemove(orderId, out saga!);
    }

    public IReadOnlyList<(Guid OrderId, PendingSaga Saga)> SweepTimedOut(TimeSpan timeout, DateTimeOffset now)
    {
        List<(Guid, PendingSaga)>? timedOut = null;

        foreach (var entry in _pending)
        {
            if (now - entry.Value.RequestedAt <= timeout)
            {
                continue;
            }

            if (_pending.TryRemove(entry.Key, out var saga))
            {
                (timedOut ??= []).Add((entry.Key, saga));
            }
        }

        return timedOut ?? [];
    }
}
