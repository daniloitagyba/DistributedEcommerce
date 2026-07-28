using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Payments.Service;

/// <summary>
/// Milestone 22's orchestrated-flow counterpart to OrderCreatedConsumer -
/// the same amount-threshold decision, but reacting to an explicit request
/// from the orchestrator rather than autonomously deciding on seeing
/// OrderCreated. Deliberately stateless: no database write, no outbox - the
/// orchestrator owns the saga's state, so this side of the comparison
/// doesn't need to. That's the coupling difference this milestone set out
/// to demonstrate, not just describe.
/// </summary>
public sealed class PaymentDecisionRequestHandler(
    IOptions<PaymentDecisionRequestOptions> requestOptions,
    IOptions<PaymentDecisionOptions> decisionOptions,
    IProducer<string, string> producer,
    ILogger<PaymentDecisionRequestHandler> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly PaymentDecisionRequestOptions _requestOptions = requestOptions.Value;
    private readonly PaymentDecisionOptions _decisionOptions = decisionOptions.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _requestOptions.BootstrapServers,
            GroupId = _requestOptions.ConsumerGroup,
            ClientId = _requestOptions.ClientId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1_000,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_requestOptions.DecisionRequestedTopic);
        PaymentDecisionRequestLog.Started(logger, _requestOptions.DecisionRequestedTopic, _requestOptions.ConsumerGroup);

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
                    PaymentDecisionRequestLog.ConsumeFailed(logger, exception.Error.Reason, exception);
                    await Task.Delay(1_000, stoppingToken);
                    continue;
                }

                await ReplyAsync(consumeResult.Message.Value, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            PaymentDecisionRequestLog.Stopping(logger);
        }
        finally
        {
            consumer.Close();
        }
    }

    private async Task ReplyAsync(string payload, CancellationToken cancellationToken)
    {
        PaymentDecisionRequested request;
        try
        {
            request = JsonSerializer.Deserialize<PaymentDecisionRequested>(payload, SerializerOptions)
                ?? throw new JsonException("The request payload deserialized to null.");
        }
        catch (JsonException exception)
        {
            PaymentDecisionRequestLog.InvalidMessage(logger, exception);
            return;
        }

        var approved = request.Amount <= _decisionOptions.DeclineAmountThreshold;
        var reply = new PaymentDecisionReplied(request.OrderId, approved, request.CorrelationId, DateTimeOffset.UtcNow);

        var message = new Message<string, string>
        {
            Key = request.OrderId.ToString("N"),
            Value = JsonSerializer.Serialize(reply, SerializerOptions)
        };

        try
        {
            await producer.ProduceAsync(_requestOptions.DecisionRepliedTopic, message, cancellationToken);
            PaymentDecisionRequestLog.Replied(logger, request.OrderId, approved);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            PaymentDecisionRequestLog.ReplyPublishFailed(logger, request.OrderId, exception);
        }
    }
}

public sealed partial class PaymentDecisionRequestLog
{
    [LoggerMessage(EventId = 7000, Level = LogLevel.Information, Message = "Payment decision request handler subscribed to topic {Topic} with consumer group {GroupId}")]
    public static partial void Started(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 7001, Level = LogLevel.Information, Message = "Payment decision request handler is stopping gracefully")]
    public static partial void Stopping(ILogger logger);

    [LoggerMessage(EventId = 7002, Level = LogLevel.Error, Message = "Payment decision request handler Kafka consume failed: {Reason}")]
    public static partial void ConsumeFailed(ILogger logger, string reason, Exception exception);

    [LoggerMessage(EventId = 7003, Level = LogLevel.Warning, Message = "Payment decision request handler received an invalid message")]
    public static partial void InvalidMessage(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 7004, Level = LogLevel.Information, Message = "Replied to decision request for order {OrderId} approved={Approved}")]
    public static partial void Replied(ILogger logger, Guid orderId, bool approved);

    [LoggerMessage(EventId = 7005, Level = LogLevel.Error, Message = "Payment decision request handler failed to publish reply for order {OrderId}")]
    public static partial void ReplyPublishFailed(ILogger logger, Guid orderId, Exception exception);
}
