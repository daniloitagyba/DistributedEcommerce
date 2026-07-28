namespace Orders.Worker;

public sealed class SagaOrchestrationOptions
{
    public const string SectionName = "SagaOrchestration";

    public string BootstrapServers { get; init; } = "localhost:9092";

    public string OrderCreatedTopic { get; init; } = "orders.created.v1";

    public string DecisionRequestedTopic { get; init; } = "payments.decision-requested.v1";

    public string DecisionRepliedTopic { get; init; } = "payments.decision-replied.v1";

    public string RequestConsumerGroup { get; init; } = "orders-saga-orchestrator";

    public string ReplyConsumerGroup { get; init; } = "orders-saga-orchestrator-reply";

    public string ClientId { get; init; } = "orders-saga-orchestrator";

    public int TimeoutSeconds { get; init; } = 5;

    public int SweepIntervalMilliseconds { get; init; } = 1_000;
}
