namespace Payments.Service;

public sealed class PaymentsKafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = "localhost:9092";

    public string OrderCreatedTopic { get; init; } = "orders.created.v1";

    public string PaymentResultTopic { get; init; } = "payments.result.v1";

    public string DeadLetterTopic { get; init; } = "payments.decisions.dlq.v1";

    public string ConsumerGroup { get; init; } = "payments-service";

    public string ClientId { get; init; } = "payments-service";
}
