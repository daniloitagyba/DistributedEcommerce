using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

/// <summary>
/// Milestone 22's explicit compensation half, extended in Milestone 43 from
/// a single request/reply pair into the driver of a 4-step saga. Only one
/// reply is ever outstanding per order at a time, so this one consumer
/// subscribing to all four reply topics and dispatching by topic name is
/// simpler than four separate consumer classes each only knowing how to
/// advance one specific transition.
///
/// State machine:
///   ReserveInventory  --(reserved)-->     DecidePayment   --(approved)--> CommitInventory --> done (Confirmed)
///   ReserveInventory  --(insufficient)--> done (RejectedInsufficientStock)
///   DecidePayment     --(declined)-->     ReleaseInventory (the compensation) --> done (RejectedPaymentDeclined)
/// </summary>
public sealed class OrderSagaReplyConsumer(
    IOptions<SagaOrchestrationOptions> options,
    IProducer<string, string> producer,
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
        consumer.Subscribe(
        [
            _options.ReservationRepliedTopic,
            _options.DecisionRepliedTopic,
            _options.CommitRepliedTopic,
            _options.ReleaseRepliedTopic
        ]);
        SagaOrchestratorLog.Started(logger, _options.ReplyConsumerGroup, _options.ReplyConsumerGroup);

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

                await DispatchAsync(consumeResult, stoppingToken);
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

    private Task DispatchAsync(ConsumeResult<string, string> consumeResult, CancellationToken cancellationToken)
    {
        return consumeResult.Topic switch
        {
            _ when consumeResult.Topic == _options.ReservationRepliedTopic => HandleReservationRepliedAsync(consumeResult.Message.Value, cancellationToken),
            _ when consumeResult.Topic == _options.DecisionRepliedTopic => HandlePaymentDecisionRepliedAsync(consumeResult.Message.Value, cancellationToken),
            _ when consumeResult.Topic == _options.CommitRepliedTopic => HandleCommitRepliedAsync(consumeResult.Message.Value, cancellationToken),
            _ when consumeResult.Topic == _options.ReleaseRepliedTopic => HandleReleaseRepliedAsync(consumeResult.Message.Value, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandleReservationRepliedAsync(string payload, CancellationToken cancellationToken)
    {
        var reply = Deserialize<InventoryReservationReplied>(payload);
        if (reply is null)
        {
            return;
        }

        if (!reply.Reserved)
        {
            var completed = await store.TryCompleteAsync(reply.OrderId, SagaStep.ReserveInventory, cancellationToken);
            if (completed is null)
            {
                SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
                return;
            }

            var latencyMs = (reply.DecidedAt - completed.RequestedAt).TotalMilliseconds;
            SagaOrchestratorLog.SagaCompleted(logger, reply.OrderId, "RejectedInsufficientStock", latencyMs, completed.CorrelationId);
            return;
        }

        var now = DateTimeOffset.UtcNow;
        var advanced = await store.TryAdvanceAsync(reply.OrderId, SagaStep.ReserveInventory, SagaStep.DecidePayment, now, cancellationToken);
        if (advanced is null)
        {
            SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
            return;
        }

        SagaOrchestratorLog.Advanced(logger, reply.OrderId, SagaStep.DecidePayment, advanced.CorrelationId);

        var request = new PaymentDecisionRequested(reply.OrderId, advanced.Amount, advanced.Currency, advanced.CorrelationId, now);
        await PublishNextStepAsync(_options.DecisionRequestedTopic, reply.OrderId.ToString("N"), request, reply.OrderId, cancellationToken);
    }

    private async Task HandlePaymentDecisionRepliedAsync(string payload, CancellationToken cancellationToken)
    {
        var reply = Deserialize<PaymentDecisionReplied>(payload);
        if (reply is null)
        {
            return;
        }

        var now = DateTimeOffset.UtcNow;

        if (reply.Approved)
        {
            var advanced = await store.TryAdvanceAsync(reply.OrderId, SagaStep.DecidePayment, SagaStep.CommitInventory, now, cancellationToken);
            if (advanced is null)
            {
                SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
                return;
            }

            SagaOrchestratorLog.Advanced(logger, reply.OrderId, SagaStep.CommitInventory, advanced.CorrelationId);
            var request = new InventoryReservationCommitRequested(advanced.ReservationId, reply.OrderId, advanced.Sku, advanced.Quantity, advanced.CorrelationId, now);
            await PublishNextStepAsync(_options.CommitRequestedTopic, advanced.Sku, request, reply.OrderId, cancellationToken);
        }
        else
        {
            // The compensating transaction: the reservation from step 1 was
            // never the problem, payment was - so it gets undone rather than
            // left dangling against the order it will now never fulfill.
            var advanced = await store.TryAdvanceAsync(reply.OrderId, SagaStep.DecidePayment, SagaStep.ReleaseInventory, now, cancellationToken);
            if (advanced is null)
            {
                SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
                return;
            }

            SagaOrchestratorLog.Advanced(logger, reply.OrderId, SagaStep.ReleaseInventory, advanced.CorrelationId);
            var request = new InventoryReservationReleaseRequested(advanced.ReservationId, reply.OrderId, advanced.Sku, advanced.Quantity, advanced.CorrelationId, now);
            await PublishNextStepAsync(_options.ReleaseRequestedTopic, advanced.Sku, request, reply.OrderId, cancellationToken);
        }
    }

    private async Task HandleCommitRepliedAsync(string payload, CancellationToken cancellationToken)
    {
        var reply = Deserialize<InventoryReservationCommitReplied>(payload);
        if (reply is null)
        {
            return;
        }

        var completed = await store.TryCompleteAsync(reply.OrderId, SagaStep.CommitInventory, cancellationToken);
        if (completed is null)
        {
            SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
            return;
        }

        var outcome = reply.Committed ? "Confirmed" : "ConfirmedButCommitFailed";
        var latencyMs = (reply.DecidedAt - completed.RequestedAt).TotalMilliseconds;
        SagaOrchestratorLog.SagaCompleted(logger, reply.OrderId, outcome, latencyMs, completed.CorrelationId);
    }

    private async Task HandleReleaseRepliedAsync(string payload, CancellationToken cancellationToken)
    {
        var reply = Deserialize<InventoryReservationReleaseReplied>(payload);
        if (reply is null)
        {
            return;
        }

        var completed = await store.TryCompleteAsync(reply.OrderId, SagaStep.ReleaseInventory, cancellationToken);
        if (completed is null)
        {
            SagaOrchestratorLog.UnknownReply(logger, reply.OrderId);
            return;
        }

        var outcome = reply.Released ? "RejectedPaymentDeclined" : "RejectedPaymentDeclinedButReleaseFailed";
        var latencyMs = (reply.DecidedAt - completed.RequestedAt).TotalMilliseconds;
        SagaOrchestratorLog.SagaCompleted(logger, reply.OrderId, outcome, latencyMs, completed.CorrelationId);
    }

    private async Task PublishNextStepAsync<TRequest>(
        string topic,
        string key,
        TRequest request,
        Guid orderId,
        CancellationToken cancellationToken)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = JsonSerializer.Serialize(request, SerializerOptions)
        };

        try
        {
            await producer.ProduceAsync(topic, message, cancellationToken);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            SagaOrchestratorLog.NextStepPublishFailed(logger, orderId, exception);
        }
    }

    private static T? Deserialize<T>(string payload)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(payload, SerializerOptions);
        }
        catch (JsonException)
        {
            return default;
        }
    }
}
