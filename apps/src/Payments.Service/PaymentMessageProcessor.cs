using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Payments.Service.Data;
using Payments.Service.Domain;
using Payments.Service.Messaging;

namespace Payments.Service;

public enum MessageProcessingResult
{
    Processed,
    Duplicate
}

public sealed class InvalidOrderMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

public sealed class PaymentMessageProcessor(
    IServiceScopeFactory scopeFactory,
    IOptions<PaymentsKafkaOptions> kafkaOptions,
    IOptions<PaymentDecisionOptions> decisionOptions,
    ILogger<PaymentMessageProcessor> logger)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly PaymentsKafkaOptions _kafkaOptions = kafkaOptions.Value;
    private readonly PaymentDecisionOptions _decisionOptions = decisionOptions.Value;

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
            "payments.decide",
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

        await using var serviceScope = scopeFactory.CreateAsyncScope();
        var dbContext = serviceScope.ServiceProvider.GetRequiredService<PaymentsDbContext>();

        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        var processedAt = DateTimeOffset.UtcNow;
        var insertedRows = await dbContext.Database.ExecuteSqlInterpolatedAsync(
            $"""
            INSERT INTO inbox_messages (consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at)
            VALUES ({_kafkaOptions.ConsumerGroup}, {orderCreated.EventId}, {consumeResult.Topic}, {consumeResult.Partition.Value}, {consumeResult.Offset.Value}, {correlationId}, {processedAt})
            ON CONFLICT (consumer_name, event_id) DO NOTHING
            """,
            cancellationToken);

        if (insertedRows == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            OrdersTelemetry.RecordProcessed("duplicate");
            OrdersTelemetry.RecordInboxDuplicate(_kafkaOptions.ConsumerGroup);
            PaymentsLog.Duplicate(logger, orderCreated.EventId, _kafkaOptions.ConsumerGroup);
            return MessageProcessingResult.Duplicate;
        }

        var approved = orderCreated.Amount <= _decisionOptions.DeclineAmountThreshold;
        var payment = Payment.Decide(
            orderCreated.OrderId,
            orderCreated.Amount,
            orderCreated.Currency,
            approved,
            processedAt,
            correlationId);
        var paymentDecided = new PaymentDecided(
            Guid.NewGuid(),
            orderCreated.OrderId,
            payment.Id,
            approved,
            orderCreated.Amount,
            orderCreated.Currency,
            processedAt,
            correlationId);
        var outboxMessage = OutboxMessage.Create(
            paymentDecided.EventId,
            nameof(PaymentDecided),
            JsonSerializer.Serialize(paymentDecided, SerializerOptions),
            processedAt,
            correlationId,
            Activity.Current?.Id,
            Activity.Current?.TraceStateString);

        dbContext.Payments.Add(payment);
        dbContext.OutboxMessages.Add(outboxMessage);
        await dbContext.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        activity?.SetTag("payment.approved", approved);
        OrdersTelemetry.RecordProcessed("success");
        OrdersTelemetry.RecordPaymentDecided(approved);
        PaymentsLog.Decided(logger, orderCreated.OrderId, payment.Id, approved, correlationId);
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

public sealed partial class PaymentsLog
{
    [LoggerMessage(EventId = 5000, Level = LogLevel.Information, Message = "Payments service subscribed to topic {Topic} with consumer group {GroupId}")]
    public static partial void Started(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 5001, Level = LogLevel.Information, Message = "Payments service is stopping gracefully")]
    public static partial void Stopping(ILogger logger);

    [LoggerMessage(EventId = 5002, Level = LogLevel.Information, Message = "Decided payment {PaymentId} for order {OrderId}: approved={Approved} with correlation {CorrelationId}")]
    public static partial void Decided(ILogger logger, Guid orderId, Guid paymentId, bool approved, string correlationId);

    [LoggerMessage(EventId = 5003, Level = LogLevel.Error, Message = "Kafka consume failed: {Reason}")]
    public static partial void ConsumeFailed(ILogger logger, string reason, Exception exception);

    [LoggerMessage(EventId = 5005, Level = LogLevel.Information, Message = "Skipped duplicate event {EventId} for consumer {ConsumerName}")]
    public static partial void Duplicate(ILogger logger, Guid eventId, string consumerName);

    [LoggerMessage(EventId = 5006, Level = LogLevel.Warning, Message = "Processing at {Offset} failed on attempt {Attempt}; retrying in {DelayMilliseconds} ms")]
    public static partial void RetryScheduled(ILogger logger, string offset, int attempt, double delayMilliseconds, Exception exception);

    [LoggerMessage(EventId = 5007, Level = LogLevel.Error, Message = "Moved message at {Offset} to {DeadLetterTopic} after {AttemptCount} attempts")]
    public static partial void DeadLettered(ILogger logger, string offset, string deadLetterTopic, int attemptCount);

    [LoggerMessage(EventId = 5008, Level = LogLevel.Error, Message = "Infrastructure failure while processing message at {Offset}; offset remains uncommitted")]
    public static partial void InfrastructureFailure(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 5009, Level = LogLevel.Error, Message = "Failed to publish message at {Offset} to the dead-letter topic; offset remains uncommitted")]
    public static partial void DeadLetterFailed(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 5010, Level = LogLevel.Warning, Message = "Failed to commit Kafka offset {Offset}; Inbox will prevent duplicate processing")]
    public static partial void CommitFailed(ILogger logger, string offset, Exception exception);
}
