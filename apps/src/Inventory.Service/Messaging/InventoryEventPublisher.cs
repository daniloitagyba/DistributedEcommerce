using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Inventory.Service;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace Inventory.Service.Messaging;

public interface IInventoryEventPublisher
{
    Task PublishAsync(InventoryReservationReplied reply, CancellationToken cancellationToken);

    Task PublishAsync(InventoryReservationCommitReplied reply, CancellationToken cancellationToken);

    Task PublishAsync(InventoryReservationReleaseReplied reply, CancellationToken cancellationToken);
}

public sealed class KafkaInventoryEventPublisher(
    IProducer<string, string> producer,
    IOptions<InventoryKafkaOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider) : IInventoryEventPublisher
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly Polly.ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.KafkaProducerPipeline);

    public Task PublishAsync(InventoryReservationReplied reply, CancellationToken cancellationToken)
    {
        return PublishInternalAsync(options.Value.ReservationRepliedTopic, reply.OrderId, reply.CorrelationId, reply, cancellationToken);
    }

    public Task PublishAsync(InventoryReservationCommitReplied reply, CancellationToken cancellationToken)
    {
        return PublishInternalAsync(options.Value.CommitRepliedTopic, reply.OrderId, reply.CorrelationId, reply, cancellationToken);
    }

    public Task PublishAsync(InventoryReservationReleaseReplied reply, CancellationToken cancellationToken)
    {
        return PublishInternalAsync(options.Value.ReleaseRepliedTopic, reply.OrderId, reply.CorrelationId, reply, cancellationToken);
    }

    private async Task PublishInternalAsync<TReply>(
        string topic,
        Guid orderId,
        string correlationId,
        TReply reply,
        CancellationToken cancellationToken)
    {
        var headers = new Headers();
        AddHeader(headers, MessagingHeaders.CorrelationId, correlationId);
        AddHeader(headers, MessagingHeaders.TraceParent, Activity.Current?.Id);
        AddHeader(headers, MessagingHeaders.TraceState, Activity.Current?.TraceStateString);

        var message = new Message<string, string>
        {
            Key = orderId.ToString("N"),
            Value = JsonSerializer.Serialize(reply, SerializerOptions),
            Headers = headers
        };

        await _pipeline.ExecuteAsync(
            async ct => await producer.ProduceAsync(topic, message, ct).WaitAsync(ct),
            cancellationToken);
    }

    private static void AddHeader(Headers headers, string key, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }
    }
}
