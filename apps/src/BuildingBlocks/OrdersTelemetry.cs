using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace BuildingBlocks;

public static class OrdersTelemetry
{
    public const string SourceName = "LocalDistributedLab.Orders";

    public static readonly ActivitySource ActivitySource = new(SourceName);

    private static readonly Meter Meter = new(SourceName);
    private static readonly Counter<long> CreatedCounter = Meter.CreateCounter<long>("orders.created");
    private static readonly Counter<long> ProcessedCounter = Meter.CreateCounter<long>("orders.processed");
    private static readonly Counter<long> OutboxPublishedCounter = Meter.CreateCounter<long>("outbox.messages.published");
    private static readonly Counter<long> OutboxRetryCounter = Meter.CreateCounter<long>("outbox.publish.retries");
    private static readonly Counter<long> InboxDuplicateCounter = Meter.CreateCounter<long>("inbox.messages.duplicates");
    private static readonly Counter<long> ProcessingRetryCounter = Meter.CreateCounter<long>("messaging.processing.retries");
    private static readonly Counter<long> DeadLetterCounter = Meter.CreateCounter<long>("messaging.dead_letters");

    public static Activity? StartActivity(
        string name,
        ActivityKind kind,
        string? traceParent = null,
        string? traceState = null)
    {
        if (!string.IsNullOrWhiteSpace(traceParent)
            && ActivityContext.TryParse(traceParent, traceState, true, out var parentContext))
        {
            return ActivitySource.StartActivity(name, kind, parentContext);
        }

        return ActivitySource.StartActivity(name, kind);
    }

    public static void RecordCreated(string currency)
    {
        CreatedCounter.Add(1, new KeyValuePair<string, object?>("currency", currency));
    }

    public static void RecordProcessed(string result)
    {
        ProcessedCounter.Add(1, new KeyValuePair<string, object?>("result", result));
    }

    public static void RecordOutboxPublished(string eventType)
    {
        OutboxPublishedCounter.Add(1, new KeyValuePair<string, object?>("event.type", eventType));
    }

    public static void RecordOutboxRetry(string eventType)
    {
        OutboxRetryCounter.Add(1, new KeyValuePair<string, object?>("event.type", eventType));
    }

    public static void RecordInboxDuplicate(string consumerName)
    {
        InboxDuplicateCounter.Add(1, new KeyValuePair<string, object?>("consumer.name", consumerName));
    }

    public static void RecordProcessingRetry(string topic)
    {
        ProcessingRetryCounter.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", topic));
    }

    public static void RecordDeadLetter(string topic)
    {
        DeadLetterCounter.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", topic));
    }
}
