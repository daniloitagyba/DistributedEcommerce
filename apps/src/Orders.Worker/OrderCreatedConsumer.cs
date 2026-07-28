using System.Diagnostics;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Orders.Worker;

public sealed class OrderCreatedConsumer(
    IOptions<KafkaOptions> options,
    IOptions<MessageProcessingOptions> processingOptions,
    OrderMessageProcessor processor,
    IDeadLetterPublisher deadLetterPublisher,
    ILogger<OrderCreatedConsumer> logger) : BackgroundService
{
    private readonly KafkaOptions _options = options.Value;
    private readonly MessageProcessingOptions _processingOptions = processingOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroup,
            ClientId = _options.ClientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = 10_000
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_options.OrderCreatedTopic);
        WorkerLog.Started(logger, _options.OrderCreatedTopic, _options.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException exception)
                {
                    WorkerLog.ConsumeFailed(logger, exception.Error.Reason, exception);
                    await DelayForInfrastructureAsync(stoppingToken);
                    continue;
                }

                var shouldCommit = await ProcessWithRetriesAsync(consumeResult, stoppingToken);
                if (!shouldCommit)
                {
                    consumer.Seek(consumeResult.TopicPartitionOffset);
                    continue;
                }

                try
                {
                    consumer.Commit(consumeResult);
                }
                catch (KafkaException exception)
                {
                    WorkerLog.CommitFailed(logger, consumeResult.TopicPartitionOffset.ToString(), exception);
                    await DelayForInfrastructureAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            WorkerLog.Stopping(logger);
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task<bool> ProcessWithRetriesAsync(
        ConsumeResult<string, string> consumeResult,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= _processingOptions.MaximumAttempts; attempt++)
        {
            try
            {
                await processor.ProcessAsync(consumeResult, cancellationToken);
                return true;
            }
            catch (Exception exception) when (exception is NpgsqlException || ResilienceExtensions.IsInfrastructureFault(exception))
            {
                WorkerLog.InfrastructureFailure(logger, consumeResult.TopicPartitionOffset.ToString(), exception);
                await DelayForInfrastructureAsync(cancellationToken);
                return false;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                if (attempt < _processingOptions.MaximumAttempts)
                {
                    var delay = RetryDelayCalculator.Calculate(
                        attempt,
                        _processingOptions.InitialRetryDelayMilliseconds,
                        _processingOptions.MaximumRetryDelayMilliseconds);
                    OrdersTelemetry.RecordProcessingRetry(consumeResult.Topic);
                    WorkerLog.RetryScheduled(
                        logger,
                        consumeResult.TopicPartitionOffset.ToString(),
                        attempt,
                        delay.TotalMilliseconds,
                        exception);
                    await Task.Delay(delay, cancellationToken);
                    continue;
                }

                return await TryDeadLetterAsync(consumeResult, exception, attempt, cancellationToken);
            }
        }

        throw new UnreachableException();
    }

    private async Task<bool> TryDeadLetterAsync(
        ConsumeResult<string, string> consumeResult,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        try
        {
            await deadLetterPublisher.PublishAsync(consumeResult, exception, attemptCount, cancellationToken);
            OrdersTelemetry.RecordProcessed("dead-letter");
            OrdersTelemetry.RecordDeadLetter(_options.DeadLetterTopic);
            WorkerLog.DeadLettered(
                logger,
                consumeResult.TopicPartitionOffset.ToString(),
                _options.DeadLetterTopic,
                attemptCount);
            return true;
        }
        catch (Exception deadLetterException) when (deadLetterException is not OperationCanceledException)
        {
            WorkerLog.DeadLetterFailed(logger, consumeResult.TopicPartitionOffset.ToString(), deadLetterException);
            await DelayForInfrastructureAsync(cancellationToken);
            return false;
        }
    }

    private Task DelayForInfrastructureAsync(CancellationToken cancellationToken)
    {
        return Task.Delay(_processingOptions.InfrastructureRetryDelayMilliseconds, cancellationToken);
    }
}

public sealed partial class WorkerLog
{
    [LoggerMessage(EventId = 2000, Level = LogLevel.Information, Message = "Orders worker subscribed to topic {Topic} with consumer group {GroupId}")]
    public static partial void Started(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 2001, Level = LogLevel.Information, Message = "Orders worker is stopping gracefully")]
    public static partial void Stopping(ILogger logger);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Information, Message = "Processed order {OrderId} from event {EventId} with correlation {CorrelationId}")]
    public static partial void Processed(ILogger logger, Guid orderId, Guid eventId, string correlationId);

    [LoggerMessage(EventId = 2003, Level = LogLevel.Error, Message = "Kafka consume failed: {Reason}")]
    public static partial void ConsumeFailed(ILogger logger, string reason, Exception exception);

    [LoggerMessage(EventId = 2005, Level = LogLevel.Information, Message = "Skipped duplicate event {EventId} for consumer {ConsumerName}")]
    public static partial void Duplicate(ILogger logger, Guid eventId, string consumerName);

    [LoggerMessage(EventId = 2006, Level = LogLevel.Warning, Message = "Processing at {Offset} failed on attempt {Attempt}; retrying in {DelayMilliseconds} ms")]
    public static partial void RetryScheduled(ILogger logger, string offset, int attempt, double delayMilliseconds, Exception exception);

    [LoggerMessage(EventId = 2007, Level = LogLevel.Error, Message = "Moved message at {Offset} to {DeadLetterTopic} after {AttemptCount} attempts")]
    public static partial void DeadLettered(ILogger logger, string offset, string deadLetterTopic, int attemptCount);

    [LoggerMessage(EventId = 2008, Level = LogLevel.Error, Message = "Infrastructure failure while processing message at {Offset}; offset remains uncommitted")]
    public static partial void InfrastructureFailure(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 2009, Level = LogLevel.Error, Message = "Failed to publish message at {Offset} to the dead-letter topic; offset remains uncommitted")]
    public static partial void DeadLetterFailed(ILogger logger, string offset, Exception exception);

    [LoggerMessage(EventId = 2010, Level = LogLevel.Warning, Message = "Failed to commit Kafka offset {Offset}; Inbox will prevent duplicate processing")]
    public static partial void CommitFailed(ILogger logger, string offset, Exception exception);
}
