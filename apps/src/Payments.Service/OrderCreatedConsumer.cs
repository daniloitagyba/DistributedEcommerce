using System.Diagnostics;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Payments.Service;

public sealed class OrderCreatedConsumer(
    IOptions<PaymentsKafkaOptions> options,
    IOptions<MessageProcessingOptions> processingOptions,
    PaymentMessageProcessor processor,
    IDeadLetterPublisher deadLetterPublisher,
    ILogger<OrderCreatedConsumer> logger) : BackgroundService
{
    private readonly PaymentsKafkaOptions _options = options.Value;
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

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(_options.OrderCreatedTopic);
        PaymentsLog.Started(logger, _options.OrderCreatedTopic, _options.ConsumerGroup);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]> consumeResult;
                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException exception)
                {
                    PaymentsLog.ConsumeFailed(logger, exception.Error.Reason, exception);
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
                    PaymentsLog.CommitFailed(logger, consumeResult.TopicPartitionOffset.ToString(), exception);
                    await DelayForInfrastructureAsync(stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            PaymentsLog.Stopping(logger);
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task<bool> ProcessWithRetriesAsync(
        ConsumeResult<string, byte[]> consumeResult,
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
                PaymentsLog.InfrastructureFailure(logger, consumeResult.TopicPartitionOffset.ToString(), exception);
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
                    PaymentsLog.RetryScheduled(
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
        ConsumeResult<string, byte[]> consumeResult,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        try
        {
            await deadLetterPublisher.PublishAsync(consumeResult, exception, attemptCount, cancellationToken);
            OrdersTelemetry.RecordProcessed("dead-letter");
            OrdersTelemetry.RecordDeadLetter(_options.DeadLetterTopic);
            PaymentsLog.DeadLettered(
                logger,
                consumeResult.TopicPartitionOffset.ToString(),
                _options.DeadLetterTopic,
                attemptCount);
            return true;
        }
        catch (Exception deadLetterException) when (deadLetterException is not OperationCanceledException)
        {
            PaymentsLog.DeadLetterFailed(logger, consumeResult.TopicPartitionOffset.ToString(), deadLetterException);
            await DelayForInfrastructureAsync(cancellationToken);
            return false;
        }
    }

    private Task DelayForInfrastructureAsync(CancellationToken cancellationToken)
    {
        return Task.Delay(_processingOptions.InfrastructureRetryDelayMilliseconds, cancellationToken);
    }
}
