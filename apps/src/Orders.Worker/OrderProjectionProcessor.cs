using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

public sealed class InvalidProjectionMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

public sealed class OrderProjectionProcessor(
    InboxStore inboxStore,
    OrderProjectionStore projectionStore,
    IOptions<OrderProjectionOptions> options,
    ILogger<OrderProjectionProcessor> logger)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly OrderProjectionOptions _options = options.Value;

    public async Task<MessageProcessingResult> ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        if (consumeResult.Topic == _options.OrderCreatedTopic)
        {
            return await ProcessOrderCreatedAsync(consumeResult, cancellationToken);
        }

        if (consumeResult.Topic == _options.PaymentResultTopic)
        {
            return await ProcessPaymentDecidedAsync(consumeResult, cancellationToken);
        }

        throw new InvalidProjectionMessageException($"Unexpected topic '{consumeResult.Topic}'.");
    }

    private async Task<MessageProcessingResult> ProcessOrderCreatedAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        var orderCreated = Deserialize<OrderCreated>(consumeResult.Message.Value, "OrderCreated event");
        if (orderCreated.EventId == Guid.Empty || orderCreated.OrderId == Guid.Empty)
        {
            throw new InvalidProjectionMessageException("The OrderCreated event and order identifiers are required.");
        }

        var correlationId = GetHeader(consumeResult.Message.Headers, MessagingHeaders.CorrelationId) ?? orderCreated.CorrelationId;
        using var activity = StartActivity(consumeResult, correlationId, orderCreated.EventId, orderCreated.OrderId);

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
            WorkerLog.Duplicate(logger, orderCreated.EventId, _options.ConsumerGroup);
            return MessageProcessingResult.Duplicate;
        }

        var projectedAt = DateTimeOffset.UtcNow;
        await projectionStore.ProjectOrderCreatedAsync(
            orderCreated.OrderId,
            orderCreated.CustomerId,
            orderCreated.Amount,
            orderCreated.Currency,
            orderCreated.OccurredAt,
            projectedAt,
            cancellationToken);

        OrdersTelemetry.RecordProcessed("success");
        OrdersTelemetry.RecordProjectionLag(nameof(OrderCreated), projectedAt - orderCreated.OccurredAt);
        return MessageProcessingResult.Processed;
    }

    private async Task<MessageProcessingResult> ProcessPaymentDecidedAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        var paymentDecided = Deserialize<PaymentDecided>(consumeResult.Message.Value, "PaymentDecided event");
        if (paymentDecided.EventId == Guid.Empty || paymentDecided.OrderId == Guid.Empty)
        {
            throw new InvalidProjectionMessageException("The PaymentDecided event and order identifiers are required.");
        }

        var correlationId = GetHeader(consumeResult.Message.Headers, MessagingHeaders.CorrelationId) ?? paymentDecided.CorrelationId;
        using var activity = StartActivity(consumeResult, correlationId, paymentDecided.EventId, paymentDecided.OrderId);

        var inserted = await inboxStore.TryRecordAsync(
            _options.ConsumerGroup,
            paymentDecided.EventId,
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            correlationId,
            DateTimeOffset.UtcNow,
            cancellationToken);

        if (!inserted)
        {
            OrdersTelemetry.RecordProcessed("duplicate");
            WorkerLog.Duplicate(logger, paymentDecided.EventId, _options.ConsumerGroup);
            return MessageProcessingResult.Duplicate;
        }

        var projectedAt = DateTimeOffset.UtcNow;
        var status = paymentDecided.Approved ? "Confirmed" : "Cancelled";
        await projectionStore.ProjectPaymentDecidedAsync(
            paymentDecided.OrderId,
            status,
            paymentDecided.OccurredAt,
            projectedAt,
            cancellationToken);

        OrdersTelemetry.RecordProcessed("success");
        OrdersTelemetry.RecordProjectionLag(nameof(PaymentDecided), projectedAt - paymentDecided.OccurredAt);
        return MessageProcessingResult.Processed;
    }

    private static Activity? StartActivity(
        ConsumeResult<string, string> consumeResult,
        string correlationId,
        Guid eventId,
        Guid orderId)
    {
        var traceParent = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceParent);
        var traceState = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceState);
        var activity = OrdersTelemetry.StartActivity("orders.project", ActivityKind.Consumer, traceParent, traceState);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", consumeResult.Topic);
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.message.id", eventId);
        activity?.SetTag("order.id", orderId);
        activity?.SetTag("correlation.id", correlationId);
        return activity;
    }

    private static T Deserialize<T>(string payload, string description)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(payload, SerializerOptions)
                ?? throw new JsonException($"The Kafka message did not contain a valid {description}.");
        }
        catch (JsonException exception)
        {
            throw new InvalidProjectionMessageException($"The Kafka message is not a valid {description}.", exception);
        }
    }

    private static string? GetHeader(Headers headers, string key)
    {
        var header = headers.LastOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        return header is null ? null : Encoding.UTF8.GetString(header.GetValueBytes());
    }
}
