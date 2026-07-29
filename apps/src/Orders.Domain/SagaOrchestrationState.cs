namespace Orders.Domain;

/// <summary>
/// Milestone 36: durable state for the Milestone 22 orchestrated saga's
/// in-flight requests, replacing an in-memory ConcurrentDictionary that
/// didn't survive a pod restart or a KEDA scale-in - the exact kind of
/// event this lab's chaos testing induces on purpose. EF Core owns this
/// table's schema (migrations) only; runtime reads/writes go through raw
/// Npgsql in Orders.Worker's SagaOrchestrationStore, matching the existing
/// OrderEvent/order_events pattern.
/// </summary>
public sealed class SagaOrchestrationState
{
    private SagaOrchestrationState()
    {
    }

    public Guid OrderId { get; private set; }

    public string CorrelationId { get; private set; } = string.Empty;

    public DateTimeOffset RequestedAt { get; private set; }
}
