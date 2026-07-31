using System.Diagnostics;
using System.Text;
using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Payments.Service;

public interface IDeadLetterPublisher
{
    Task PublishAsync(
        ConsumeResult<string, byte[]> consumeResult,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken);
}

// OriginalPayload is base64 of the raw message bytes (Milestone 19:
// orders.created.v1 now carries Avro, not JSON).
public sealed record DeadLetterEnvelope(
    Guid DeadLetterId,
    string OriginalTopic,
    int OriginalPartition,
    long OriginalOffset,
    string? OriginalKey,
    string OriginalPayload,
    string FailureType,
    string FailureMessage,
    int AttemptCount,
    DateTimeOffset FailedAt);

public sealed class KafkaDeadLetterPublisher(
    IProducer<string, string> producer,
    IOptions<PaymentsKafkaOptions> options) : IDeadLetterPublisher
{
    private const int MaximumFailureMessageLength = 2_000;
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly PaymentsKafkaOptions _options = options.Value;

    public async Task PublishAsync(
        ConsumeResult<string, byte[]> consumeResult,
        Exception exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        var failureMessage = exception.Message.Length <= MaximumFailureMessageLength
            ? exception.Message
            : exception.Message[..MaximumFailureMessageLength];
        var envelope = new DeadLetterEnvelope(
            Guid.NewGuid(),
            consumeResult.Topic,
            consumeResult.Partition.Value,
            consumeResult.Offset.Value,
            consumeResult.Message.Key,
            Convert.ToBase64String(consumeResult.Message.Value),
            exception.GetType().Name,
            failureMessage,
            attemptCount,
            DateTimeOffset.UtcNow);

        var traceParent = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceParent);
        var traceState = GetHeader(consumeResult.Message.Headers, MessagingHeaders.TraceState);
        using var activity = OrdersTelemetry.StartActivity(
            "payments.dead_letter.publish",
            ActivityKind.Producer,
            traceParent,
            traceState);
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination.name", _options.DeadLetterTopic);
        activity?.SetTag("messaging.operation.type", "publish");

        var headers = new Headers();
        CopyHeader(consumeResult.Message.Headers, headers, MessagingHeaders.CorrelationId);
        CopyHeader(consumeResult.Message.Headers, headers, MessagingHeaders.TraceParent);
        CopyHeader(consumeResult.Message.Headers, headers, MessagingHeaders.TraceState);
        CopyHeader(consumeResult.Message.Headers, headers, MessagingHeaders.RedriveCount);
        AddHeader(headers, MessagingHeaders.OriginalTopic, consumeResult.Topic);
        AddHeader(headers, MessagingHeaders.OriginalPartition, consumeResult.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        AddHeader(headers, MessagingHeaders.OriginalOffset, consumeResult.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        AddHeader(headers, MessagingHeaders.FailureType, envelope.FailureType);
        AddHeader(headers, MessagingHeaders.AttemptCount, attemptCount.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var message = new Message<string, string>
        {
            Key = consumeResult.Message.Key ?? $"{consumeResult.Topic}-{consumeResult.Partition.Value}-{consumeResult.Offset.Value}",
            Value = JsonSerializer.Serialize(envelope, SerializerOptions),
            Headers = headers
        };

        await producer.ProduceAsync(_options.DeadLetterTopic, message, cancellationToken);
    }

    private static void CopyHeader(Headers source, Headers destination, string key)
    {
        var header = source.LastOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        if (header is not null)
        {
            destination.Add(key, header.GetValueBytes());
        }
    }

    private static string? GetHeader(Headers headers, string key)
    {
        var header = headers.LastOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        return header is null ? null : Encoding.UTF8.GetString(header.GetValueBytes());
    }

    private static void AddHeader(Headers headers, string key, string value)
    {
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }
}
