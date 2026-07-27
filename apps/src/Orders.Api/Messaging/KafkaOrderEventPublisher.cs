using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Orders.Api.Messaging;

public interface IOrderEventPublisher
{
    Task PublishAsync(OrderCreated orderCreated, CancellationToken cancellationToken);
}

public sealed class KafkaOrderEventPublisher(IProducer<string, string> producer, IOptions<KafkaOptions> options) : IOrderEventPublisher
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public async Task PublishAsync(OrderCreated orderCreated, CancellationToken cancellationToken)
    {
        var headers = new Headers();
        AddHeader(headers, MessagingHeaders.CorrelationId, orderCreated.CorrelationId);
        AddHeader(headers, MessagingHeaders.TraceParent, Activity.Current?.Id);
        AddHeader(headers, MessagingHeaders.TraceState, Activity.Current?.TraceStateString);

        var message = new Message<string, string>
        {
            Key = orderCreated.OrderId.ToString("N"),
            Value = JsonSerializer.Serialize(orderCreated, SerializerOptions),
            Headers = headers
        };

        await producer.ProduceAsync(options.Value.OrderCreatedTopic, message, cancellationToken);
    }

    private static void AddHeader(Headers headers, string key, string? value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }
    }
}
