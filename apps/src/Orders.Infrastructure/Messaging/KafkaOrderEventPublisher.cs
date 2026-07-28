using System.Diagnostics;
using System.Text;
using Avro.Generic;
using BuildingBlocks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace Orders.Infrastructure.Messaging;

public interface IOrderEventPublisher
{
    Task PublishAsync(OrderCreated orderCreated, CancellationToken cancellationToken);
}

public sealed class KafkaOrderEventPublisher(
    IProducer<string, byte[]> producer,
    ISchemaRegistryClient schemaRegistryClient,
    IOptions<KafkaOptions> options,
    ResiliencePipelineProvider<string> pipelineProvider) : IOrderEventPublisher
{
    private readonly Polly.ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.KafkaProducerPipeline);
    private readonly AvroSerializer<GenericRecord> _avroSerializer =
        new(schemaRegistryClient, new AvroSerializerConfig { AutoRegisterSchemas = true });

    public async Task PublishAsync(OrderCreated orderCreated, CancellationToken cancellationToken)
    {
        var headers = new Headers();
        AddHeader(headers, MessagingHeaders.CorrelationId, orderCreated.CorrelationId);
        AddHeader(headers, MessagingHeaders.TraceParent, Activity.Current?.Id);
        AddHeader(headers, MessagingHeaders.TraceState, Activity.Current?.TraceStateString);

        var record = OrderCreatedAvroSchema.ToGenericRecord(orderCreated);
        var context = new SerializationContext(MessageComponentType.Value, options.Value.OrderCreatedTopic, headers);
        var value = await _avroSerializer.SerializeAsync(record, context);

        var message = new Message<string, byte[]>
        {
            Key = orderCreated.OrderId.ToString("N"),
            Value = value,
            Headers = headers
        };

        await _pipeline.ExecuteAsync(
            async ct => await producer.ProduceAsync(options.Value.OrderCreatedTopic, message, ct).WaitAsync(ct),
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
