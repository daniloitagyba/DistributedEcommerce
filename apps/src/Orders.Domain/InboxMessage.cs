namespace Orders.Domain;

public sealed class InboxMessage
{
    private InboxMessage()
    {
    }

    public string ConsumerName { get; private set; } = string.Empty;

    public Guid EventId { get; private set; }

    public string Topic { get; private set; } = string.Empty;

    public int Partition { get; private set; }

    public long Offset { get; private set; }

    public string CorrelationId { get; private set; } = string.Empty;

    public DateTimeOffset ProcessedAt { get; private set; }

    public static InboxMessage Create(
        string consumerName,
        Guid eventId,
        string topic,
        int partition,
        long offset,
        string correlationId,
        DateTimeOffset processedAt)
    {
        return new InboxMessage
        {
            ConsumerName = consumerName,
            EventId = eventId,
            Topic = topic,
            Partition = partition,
            Offset = offset,
            CorrelationId = correlationId,
            ProcessedAt = processedAt
        };
    }
}
