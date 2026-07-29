using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

public sealed class OrderSagaReplyConsumer(
    IOptions<SagaOrchestrationOptions> options,
    SagaOrchestrationStore store,
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

                await HandleReplyAsync(consumeResult.Message.Value, stoppingToken);
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

    private async Task HandleReplyAsync(string payload, CancellationToken cancellationToken)
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

        var saga = await store.TryCompleteRepliedAsync(reply.OrderId, cancellationToken);
        if (saga is null)
        {
            SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
            return;
        }

        var latencyMs = (reply.DecidedAt - saga.RequestedAt).TotalMilliseconds;
        SagaOrchestratorLog.SagaCompleted(logger, reply.OrderId, reply.Approved, latencyMs, saga.CorrelationId);
    }
}
