using System.Diagnostics;
using System.Text.Json;
using Avro.Generic;
using BuildingBlocks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;

namespace Orders.Worker;

/// <summary>
/// Milestone 22: an explicit orchestrator for the same order-created event
/// the choreographed saga already reacts to (a second, independent consumer
/// group on orders.created.v1 - additive, the existing choreography is
/// completely untouched). Where Payments.Service's choreographed consumer
/// autonomously decides on seeing OrderCreated, this orchestrator explicitly
/// requests a decision and owns the timeout if one never arrives -
/// something choreography has no equivalent for.
/// </summary>
public sealed class OrderSagaOrchestrator(
    IOptions<SagaOrchestrationOptions> options,
    IProducer<string, string> producer,
    ISchemaRegistryClient schemaRegistryClient,
    SagaOrchestrationTracker tracker,
    ILogger<OrderSagaOrchestrator> logger) : BackgroundService
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly SagaOrchestrationOptions _options = options.Value;
    private readonly AvroDeserializer<GenericRecord> _avroDeserializer = new(schemaRegistryClient);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _options.RequestConsumerGroup,
            ClientId = _options.ClientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            AutoCommitIntervalMs = 1_000,
            AllowAutoCreateTopics = false
        };

        using var consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        consumer.Subscribe(_options.OrderCreatedTopic);
        SagaOrchestratorLog.Started(logger, _options.OrderCreatedTopic, _options.RequestConsumerGroup);

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
                    SagaOrchestratorLog.ConsumeFailed(logger, exception.Error.Reason, exception);
                    await Task.Delay(1_000, stoppingToken);
                    continue;
                }

                await RequestDecisionAsync(consumeResult, stoppingToken);
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

    private async Task RequestDecisionAsync(ConsumeResult<string, byte[]> consumeResult, CancellationToken cancellationToken)
    {
        OrderCreated orderCreated;
        try
        {
            var context = new SerializationContext(MessageComponentType.Value, consumeResult.Topic, consumeResult.Message.Headers);
            var record = await _avroDeserializer.DeserializeAsync(consumeResult.Message.Value, isNull: false, context)
                .WaitAsync(cancellationToken);
            orderCreated = OrderCreatedAvroSchema.FromGenericRecord(record);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            SagaOrchestratorLog.InvalidMessage(logger, exception);
            return;
        }

        var requestedAt = DateTimeOffset.UtcNow;
        tracker.TrackRequested(orderCreated.OrderId, orderCreated.CorrelationId, requestedAt);

        var request = new PaymentDecisionRequested(
            orderCreated.OrderId,
            orderCreated.Amount,
            orderCreated.Currency,
            orderCreated.CorrelationId,
            requestedAt);

        using var activity = OrdersTelemetry.StartActivity("saga.orchestrator.request", ActivityKind.Producer, null, null);
        activity?.SetTag("order.id", orderCreated.OrderId);
        activity?.SetTag("correlation.id", orderCreated.CorrelationId);

        var message = new Message<string, string>
        {
            Key = orderCreated.OrderId.ToString("N"),
            Value = JsonSerializer.Serialize(request, SerializerOptions)
        };

        try
        {
            await producer.ProduceAsync(_options.DecisionRequestedTopic, message, cancellationToken);
            SagaOrchestratorLog.DecisionRequested(logger, orderCreated.OrderId, orderCreated.CorrelationId);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            SagaOrchestratorLog.RequestPublishFailed(logger, orderCreated.OrderId, exception);
        }
    }
}

public sealed partial class SagaOrchestratorLog
{
    [LoggerMessage(EventId = 6000, Level = LogLevel.Information, Message = "Saga orchestrator subscribed to topic {Topic} with consumer group {GroupId}")]
    public static partial void Started(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 6001, Level = LogLevel.Information, Message = "Saga orchestrator is stopping gracefully")]
    public static partial void Stopping(ILogger logger);

    [LoggerMessage(EventId = 6002, Level = LogLevel.Error, Message = "Saga orchestrator Kafka consume failed: {Reason}")]
    public static partial void ConsumeFailed(ILogger logger, string reason, Exception exception);

    [LoggerMessage(EventId = 6003, Level = LogLevel.Warning, Message = "Saga orchestrator received an invalid OrderCreated message")]
    public static partial void InvalidMessage(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 6004, Level = LogLevel.Information, Message = "OrchestratedSagaRequested order {OrderId} correlation {CorrelationId}")]
    public static partial void DecisionRequested(ILogger logger, Guid orderId, string correlationId);

    [LoggerMessage(EventId = 6005, Level = LogLevel.Error, Message = "Saga orchestrator failed to publish decision request for order {OrderId}")]
    public static partial void RequestPublishFailed(ILogger logger, Guid orderId, Exception exception);

    [LoggerMessage(EventId = 6006, Level = LogLevel.Information, Message = "OrchestratedSagaCompleted order {OrderId} approved={Approved} latencyMs={LatencyMs} correlation {CorrelationId}")]
    public static partial void SagaCompleted(ILogger logger, Guid orderId, bool approved, double latencyMs, string correlationId);

    [LoggerMessage(EventId = 6007, Level = LogLevel.Warning, Message = "OrchestratedSagaTimedOut order {OrderId} after {TimeoutSeconds}s correlation {CorrelationId}")]
    public static partial void SagaTimedOut(ILogger logger, Guid orderId, int timeoutSeconds, string correlationId);

    [LoggerMessage(EventId = 6008, Level = LogLevel.Warning, Message = "Saga orchestrator received a reply for an unknown or already-settled order {OrderId}")]
    public static partial void UnknownReply(ILogger logger, Guid orderId);
}
