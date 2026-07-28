using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

public sealed class OrderSagaReplyConsumer(
    IOptions<SagaOrchestrationOptions> options,
    SagaOrchestrationTracker tracker,
    ILogger<OrderSagaReplyConsumer> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly SagaOrchestrationOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.ReplyConsumerGroup,
            ClientId = $"{_options.ClientId}-reply",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1_000,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_options.DecisionRepliedTopic);
        SagaOrchestratorLog.Started(logger, _options.DecisionRepliedTopic, _options.ReplyConsumerGroup);

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
                    SagaOrchestratorLog.ConsumeFailed(logger, exception.Error.Reason, exception);
                    await Task.Delay(1_000, stoppingToken);
                    continue;
                }

                HandleReply(consumeResult.Message.Value);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            SagaOrchestratorLog.Stopping(logger);
        }
        finally
        {
            consumer.Close();
        }
    }

    private void HandleReply(string payload)
    {
        PaymentDecisionReplied reply;
        try
        {
            reply = JsonSerializer.Deserialize<PaymentDecisionReplied>(payload, SerializerOptions)
                ?? throw new JsonException("The reply payload deserialized to null.");
        }
        catch (JsonException exception)
        {
            SagaOrchestratorLog.InvalidMessage(logger, exception);
            return;
        }

        if (!tracker.TryCompleteReplied(reply.OrderId, out var saga))
        {
            SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
            return;
        }

        var latencyMs = (reply.DecidedAt - saga.RequestedAt).TotalMilliseconds;
        SagaOrchestratorLog.SagaCompleted(logger, reply.OrderId, reply.Approved, latencyMs, saga.CorrelationId);
    }
}
