using System.Diagnostics;
using System.Text.Json;
using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Orders.Api.Data;
using Orders.Api.Domain;

namespace Orders.Api.Messaging;

public sealed class OutboxPublisher(
    IServiceScopeFactory scopeFactory,
    IOptions<OutboxOptions> options,
    IConfiguration configuration,
    ILogger<OutboxPublisher> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly OutboxOptions _options = options.Value;
    private readonly string _instanceId = configuration["InstanceId"] ?? Environment.MachineName;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        OutboxPublisherLog.Started(logger, _instanceId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var shouldDelay = false;

                try
                {
                    shouldDelay = await ProcessBatchAsync(stoppingToken) == 0;
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    OutboxPublisherLog.LoopFailed(logger, _instanceId, exception);
                    shouldDelay = true;
                }

                if (shouldDelay)
                {
                    await Task.Delay(_options.PollIntervalMilliseconds, stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal cooperative shutdown.
        }
        finally
        {
            OutboxPublisherLog.Stopping(logger, _instanceId);
        }
    }

    private async Task<int> ProcessBatchAsync(CancellationToken cancellationToken)
    {
        await using var scope = scopeFactory.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
        var publisher = scope.ServiceProvider.GetRequiredService<IOrderEventPublisher>();
        var now = DateTimeOffset.UtcNow;

        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
        var messages = await dbContext.OutboxMessages
            .FromSqlInterpolated($"""
                SELECT *
                FROM outbox_messages
                WHERE processed_at IS NULL
                  AND next_attempt_at <= {now}
                ORDER BY occurred_at
                LIMIT {_options.BatchSize}
                FOR UPDATE SKIP LOCKED
                """)
            .ToListAsync(cancellationToken);

        foreach (var message in messages)
        {
            await PublishAsync(message, publisher, cancellationToken);
        }

        await dbContext.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return messages.Count;
    }

    private async Task PublishAsync(
        OutboxMessage message,
        IOrderEventPublisher publisher,
        CancellationToken cancellationToken)
    {
        using var activity = CreateActivity(message);
        OrderCreated orderCreated;

        try
        {
            if (!string.Equals(message.EventType, nameof(OrderCreated), StringComparison.Ordinal))
            {
                throw new JsonException($"Unsupported outbox event type '{message.EventType}'.");
            }

            orderCreated = JsonSerializer.Deserialize<OrderCreated>(message.Payload, SerializerOptions)
                ?? throw new JsonException("The outbox payload did not contain an OrderCreated event.");

            using var logScope = logger.BeginScope(new Dictionary<string, object?>
            {
                ["CorrelationId"] = message.CorrelationId,
                ["EventId"] = message.Id,
                ["OrderId"] = orderCreated.OrderId,
                ["TraceId"] = activity?.TraceId.ToString() ?? string.Empty
            });

            await publisher.PublishAsync(orderCreated, cancellationToken);
            message.MarkPublished(DateTimeOffset.UtcNow);
            OrdersTelemetry.RecordOutboxPublished(message.EventType);
            OutboxPublisherLog.Published(logger, message.Id, orderCreated.OrderId, _instanceId);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            message.MarkFailed(DateTimeOffset.UtcNow, exception.Message, _options.MaximumRetryDelaySeconds);
            OrdersTelemetry.RecordOutboxRetry(message.EventType);
            OutboxPublisherLog.RetryScheduled(
                logger,
                message.Id,
                message.AttemptCount,
                message.NextAttemptAt,
                _instanceId,
                exception);
        }
    }

    private static Activity? CreateActivity(OutboxMessage message)
    {
        var activity = OrdersTelemetry.StartActivity(
            "outbox.publish",
            ActivityKind.Producer,
            message.TraceParent,
            message.TraceState);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.operation.type", "publish");
        activity?.SetTag("messaging.message.id", message.Id);
        activity?.SetTag("correlation.id", message.CorrelationId);
        return activity;
    }
}

public sealed partial class OutboxPublisherLog
{
    [LoggerMessage(EventId = 3000, Level = LogLevel.Information, Message = "Outbox publisher started on instance {InstanceId}")]
    public static partial void Started(ILogger logger, string instanceId);

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "Published outbox event {EventId} for order {OrderId} on instance {InstanceId}")]
    public static partial void Published(ILogger logger, Guid eventId, Guid orderId, string instanceId);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Warning, Message = "Outbox event {EventId} failed on attempt {AttemptCount}; retry at {NextAttemptAt} on instance {InstanceId}")]
    public static partial void RetryScheduled(ILogger logger, Guid eventId, int attemptCount, DateTimeOffset nextAttemptAt, string instanceId, Exception exception);

    [LoggerMessage(EventId = 3003, Level = LogLevel.Error, Message = "Outbox polling failed on instance {InstanceId}")]
    public static partial void LoopFailed(ILogger logger, string instanceId, Exception exception);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Information, Message = "Outbox publisher is stopping gracefully on instance {InstanceId}")]
    public static partial void Stopping(ILogger logger, string instanceId);
}
