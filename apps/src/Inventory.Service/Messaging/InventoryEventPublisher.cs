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
}

public sealed class KafkaInventoryEventPublisher(
    IProducer<string, string> producer,
    IOptions<InventoryKafkaOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider) : IInventoryEventPublisher
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly Polly.ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.KafkaProducerPipeline);

    public async Task PublishAsync(InventoryReservationReplied reply, CancellationToken cancellationToken)
    {
        var headers = new Headers();
        AddHeader(headers, MessagingHeaders.CorrelationId, reply.CorrelationId);
        AddHeader(headers, MessagingHeaders.TraceParent, Activity.Current?.Id);
        AddHeader(headers, MessagingHeaders.TraceState, Activity.Current?.TraceStateString);

        var message = new Message<string, string>
        {
            Key = reply.OrderId.ToString("N"),
            Value = JsonSerializer.Serialize(reply, SerializerOptions),
            Headers = headers
        };

        await _pipeline.ExecuteAsync(
            async ct => await producer.ProduceAsync(options.Value.ReservationRepliedTopic, message, ct).WaitAsync(ct),
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
