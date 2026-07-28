namespace Orders.Worker;

public sealed class PaymentResultKafkaOptions
{
    public const string SectionName = "PaymentResultKafka";

    public string BootstrapServers { get; init; } = "localhost:9092";

    public string PaymentResultTopic { get; init; } = "payments.result.v1";

    public string DeadLetterTopic { get; init; } = "payments.result.dlq.v1";

    public string ConsumerGroup { get; init; } = "orders-worker-payments-result";

    public string ClientId { get; init; } = "orders-worker-payments-result";
}
