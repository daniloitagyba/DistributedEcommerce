using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Inventory.Service.Data;
using Inventory.Service.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace Inventory.Service;

public enum MessageProcessingResult
{
    Processed,
    Duplicate
}

public sealed class InvalidReservationMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

public sealed class InventoryReservationMessageProcessor(
    IServiceScopeFactory scopeFactory,
    IOptions<InventoryKafkaOptions> kafkaOptions,
    ILogger<InventoryReservationMessageProcessor> logger)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly InventoryKafkaOptions _kafkaOptions = kafkaOptions.Value;

    public async Task<MessageProcessingResult> ProcessAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        var request = DeserializeAndValidate(consumeResult);
        var correlationId = GetHeader(consumeResult.Message.Headers, MessagingHeaders.CorrelationId)
            ?? request.CorrelationId;
        var traceParent = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceParent);
        var traceState = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceState);

        using var activity = OrdersTelemetry.StartActivity(
            "inventory.reserve",
            ActivityKind.Consumer,
            traceParent,
            traceState);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", consumeResult.Topic);
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("inventory.sku", request.Sku);
        activity?.SetTag("inventory.reservation_id", request.ReservationId);
        activity?.SetTag("order.id", request.OrderId);
        activity?.SetTag("correlation.id", correlationId);

        using var logScope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId,
            ["ReservationId"] = request.ReservationId,
            ["Sku"] = request.Sku,
            ["OrderId"] = request.OrderId,
            ["TraceId"] = activity?.TraceId.ToString() ?? string.Empty
        });

        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<InventoryDbContext>();

        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);

        var processedAt = DateTimeOffset.UtcNow;
        var insertedRows = await dbContext.Database.ExecuteSqlInterpolatedAsync(
            $"""
            INSERT INTO inbox_messages (consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at)
            VALUES ({_kafkaOptions.ConsumerGroup}, {request.ReservationId}, {consumeResult.Topic}, {consumeResult.Partition.Value}, {consumeResult.Offset.Value}, {correlationId}, {processedAt})
            ON CONFLICT (consumer_name, event_id) DO NOTHING
            """,
            cancellationToken);

        if (insertedRows == 0)
        {
            await transaction.RollbackAsync(cancellationToken);
            OrdersTelemetry.RecordProcessed("duplicate");
            InventoryLog.Duplicate(logger, request.ReservationId, _kafkaOptions.ConsumerGroup);
            return MessageProcessingResult.Duplicate;
        }

        // Deliberately a plain read-then-write, no SELECT ... FOR UPDATE.
        // Correctness for concurrent requests against the SAME Sku relies
        // entirely on Kafka never handing this Sku's partition to more than
        // one consumer instance at a time - see InventoryContracts.cs.
        var item = await dbContext.InventoryItems.FirstOrDefaultAsync(i => i.Sku == request.Sku, cancellationToken);
        var reserved = item is not null && item.TryReserve(request.Quantity, processedAt);
        var reason = item is null
            ? "unknown sku"
            : reserved ? null : "insufficient stock";

        var reply = new InventoryReservationReplied(
            request.ReservationId,
            request.OrderId,
            request.Sku,
            request.Quantity,
            reserved,
            reason,
            correlationId,
            processedAt);
        var outboxMessage = OutboxMessage.Create(
            Guid.NewGuid(),
            nameof(InventoryReservationReplied),
            JsonSerializer.Serialize(reply, SerializerOptions),
            processedAt,
            correlationId,
            Activity.Current?.Id,
            Activity.Current?.TraceStateString);

        dbContext.OutboxMessages.Add(outboxMessage);
        await dbContext.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        activity?.SetTag("inventory.reserved", reserved);
        OrdersTelemetry.RecordProcessed("success");
        InventoryLog.Decided(logger, request.ReservationId, request.Sku, reserved, correlationId);
        return MessageProcessingResult.Processed;
    }

    private static InventoryReservationRequested DeserializeAndValidate(ConsumeResult<string, string> consumeResult)
    {
        InventoryReservationRequested request;
        try
        {
            request = JsonSerializer.Deserialize<InventoryReservationRequested>(consumeResult.Message.Value, SerializerOptions)
                ?? throw new JsonException("The request payload deserialized to null.");
        }
        catch (JsonException exception)
        {
            throw new InvalidReservationMessageException("The Kafka message is not a valid InventoryReservationRequested event.", exception);
        }

        if (request.ReservationId == Guid.Empty || request.OrderId == Guid.Empty)
        {
            throw new InvalidReservationMessageException("The reservation and order identifiers are required.");
        }

        if (string.IsNullOrWhiteSpace(request.Sku) || request.Quantity <= 0)
        {
            throw new InvalidReservationMessageException("A non-empty Sku and a positive Quantity are required.");
        }

        return request;
    }

    private static string? GetHeader(Headers headers, string key)
    {
        var header = headers.LastOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        return header is null ? null : Encoding.UTF8.GetString(header.GetValueBytes());
    }
}

public sealed partial class InventoryLog
{
    [LoggerMessage(EventId = 9000, Level = LogLevel.Information, Message = "Inventory service subscribed to topic {Topic} with consumer group {GroupId}")]
    public static partial void Started(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 9001, Level = LogLevel.Information, Message = "Inventory service is stopping gracefully")]
    public static partial void Stopping(ILogger logger);

    [LoggerMessage(EventId = 9002, Level = LogLevel.Information, Message = "Decided reservation {ReservationId} for sku {Sku}: reserved={Reserved} with correlation {CorrelationId}")]
    public static partial void Decided(ILogger logger, Guid reservationId, string sku, bool reserved, string correlationId);

    [LoggerMessage(EventId = 9003, Level = LogLevel.Error, Message = "Kafka consume failed: {Reason}")]
    public static partial void ConsumeFailed(ILogger logger, string reason, Exception exception);

    [LoggerMessage(EventId = 9005, Level = LogLevel.Information, Message = "Skipped duplicate reservation {ReservationId} for consumer {ConsumerName}")]
    public static partial void Duplicate(ILogger logger, Guid reservationId, string consumerName);

    [LoggerMessage(EventId = 9006, Level = LogLevel.Warning, Message = "Processing at {Offset} failed on attempt {Attempt}; retrying in {DelayMilliseconds} ms")]
    public static partial void RetryScheduled(ILogger logger, string offset, int attempt, double delayMilliseconds, Exception exception);

    [LoggerMessage(EventId = 9007, Level = LogLevel.Error, Message = "Moved message at {Offset} to {DeadLetterTopic} after {AttemptCount} attempts")]
    public static partial void DeadLettered(ILogger logger, string offset, string deadLetterTopic, int attemptCount);

    [LoggerMessage(EventId = 9008, Level = LogLevel.Error, Message = "Infrastructure failure while processing message at {Offset}; offset remains uncommitted")]
    public static partial void InfrastructureFailure(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 9009, Level = LogLevel.Error, Message = "Failed to publish message at {Offset} to the dead-letter topic; offset remains uncommitted")]
    public static partial void DeadLetterFailed(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 9010, Level = LogLevel.Warning, Message = "Failed to commit Kafka offset {Offset}; Inbox will prevent duplicate processing")]
    public static partial void CommitFailed(ILogger logger, string offset, Exception exception);
}
