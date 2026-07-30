namespace Inventory.Service;

public sealed class InventoryKafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; init; } = "localhost:9092";

    public string ReservationRequestedTopic { get; init; } = "inventory.reservation-requested.v1";

    public string ReservationRepliedTopic { get; init; } = "inventory.reservation-replied.v1";

    public string DeadLetterTopic { get; init; } = "inventory.reservation.dlq.v1";

    public string ConsumerGroup { get; init; } = "inventory-service";

    public string ClientId { get; init; } = "inventory-service";
}
