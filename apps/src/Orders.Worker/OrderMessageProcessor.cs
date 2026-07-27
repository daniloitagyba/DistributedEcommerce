using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

public enum MessageProcessingResult
{
    Processed,
    Duplicate
}

public sealed class InvalidOrderMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

public sealed class OrderMessageProcessor(
    InboxStore inboxStore,
    OrderStatusStore orderStatusStore,
    IOrderCacheInvalidator cacheInvalidator,
    IOptions<KafkaOptions> options,
    ILogger<OrderMessageProcessor> logger)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly KafkaOptions _options = options.Value;

    public async Task<MessageProcessingResult> ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        var orderCreated = DeserializeAndValidate(consumeResult.Message.Value);
        var correlationId = GetHeader(consumeResult.Message.Headers, MessagingHeaders.CorrelationId)
            ?? orderCreated.CorrelationId;
        var traceParent = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceParent);
        var traceState = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceState);

        using var activity = OrdersTelemetry.StartActivity(
            "orders.process",
            ActivityKind.Consumer,
            traceParent,
            traceState);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", consumeResult.Topic);
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.message.id", orderCreated.EventId);
        activity?.SetTag("order.id", orderCreated.OrderId);
        activity?.SetTag("correlation.id", correlationId);

        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["EventId"] = orderCreated.EventId,
            ["OrderId"] = orderCreated.OrderId,
            ["TraceId"] = activity?.TraceId.ToString() ?? string.Empty
        });

        await Task.Delay(TimeSpan.FromMilliseconds(25), cancellationToken);
        var inserted = await inboxStore.TryRecordAsync(
            _options.ConsumerGroup,
            orderCreated.EventId,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            correlationId,
            DateTimeOffset.UtcNow,
            cancellationToken);

        if (!inserted)
        {
            OrdersTelemetry.RecordProcessed("duplicate");
            OrdersTelemetry.RecordInboxDuplicate(_options.ConsumerGroup);
            WorkerLog.Duplicate(logger, orderCreated.EventId, _options.ConsumerGroup);
            return MessageProcessingResult.Duplicate;
        }

        await orderStatusStore.TryConfirmAsync(orderCreated.OrderId, cancellationToken);
        await cacheInvalidator.InvalidateAsync(orderCreated.OrderId, cancellationToken);

        OrdersTelemetry.RecordProcessed("success");
        WorkerLog.Processed(logger, orderCreated.OrderId, orderCreated.EventId, correlationId);
        return MessageProcessingResult.Processed;
    }

    private static OrderCreated DeserializeAndValidate(string payload)
    {
        OrderCreated orderCreated;
        try
        {
            orderCreated = JsonSerializer.Deserialize<OrderCreated>(payload, SerializerOptions)
                ?? throw new JsonException("The Kafka message did not contain an OrderCreated event.");
        }
        catch (JsonException exception)
        {
            throw new InvalidOrderMessageException("The Kafka message is not a valid OrderCreated event.", exception);
        }

        if (orderCreated.EventId == Guid.Empty || orderCreated.OrderId == Guid.Empty)
        {
            throw new InvalidOrderMessageException("The OrderCreated event and order identifiers are required.");
        }

        if (orderCreated.SchemaVersion != 1)
        {
            throw new InvalidOrderMessageException($"Unsupported OrderCreated schema version {orderCreated.SchemaVersion}.");
        }

        return orderCreated;
    }

    private static string? GetHeader(Headers headers, string key)
    {
        var header = headers.LastOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        return header is null ? null : Encoding.UTF8.GetString(header.GetValueBytes());
    }
}
