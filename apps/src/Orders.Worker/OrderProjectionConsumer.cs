using System.Diagnostics;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Orders.Worker;

public sealed class OrderProjectionConsumer(
    IOptions<OrderProjectionOptions> options,
    IOptions<MessageProcessingOptions> processingOptions,
    OrderProjectionProcessor processor,
    IOrderProjectionDeadLetterPublisher deadLetterPublisher,
    ILogger<OrderProjectionConsumer> logger) : BackgroundService
{
    private readonly OrderProjectionOptions _options = options.Value;
    private readonly MessageProcessingOptions _processingOptions = processingOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ConsumerGroup,
            ClientId = _options.ClientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1_000,
            EnableAutoOffsetStore = false,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = 10_000
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe([_options.OrderCreatedTopic, _options.PaymentResultTopic]);
        WorkerLog.StartedMultiTopic(logger, _options.OrderCreatedTopic, _options.PaymentResultTopic, _options.ConsumerGroup);

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
                    consumer.StoreOffset(consumeResult);
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
