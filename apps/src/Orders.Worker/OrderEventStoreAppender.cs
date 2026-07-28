using BuildingBlocks;
using Npgsql;
using NpgsqlTypes;
using Polly;
using Polly.Registry;

namespace Orders.Worker;

public sealed class OrderEventStoreAppender(NpgsqlDataSource dataSource, ResiliencePipelineProvider<string> pipelineProvider)
{
    private const string AppendSql = """
        INSERT INTO order_events (order_id, event_type, payload, occurred_at)
        VALUES (@order_id, @event_type, @payload, @occurred_at);
        """;

    private readonly ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.PostgresPipeline);

    public Task AppendAsync(
        Guid orderId,
        string eventType,
        string payload,
        DateTimeOffset occurredAt,
        CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(AppendSql);
            command.Parameters.AddWithValue("order_id", NpgsqlDbType.Uuid, orderId);
            command.Parameters.AddWithValue("event_type", NpgsqlDbType.Varchar, eventType);
            command.Parameters.AddWithValue("payload", NpgsqlDbType.Jsonb, payload);
            command.Parameters.AddWithValue("occurred_at", NpgsqlDbType.TimestampTz, occurredAt);

            await command.ExecuteNonQueryAsync(ct);
        }, cancellationToken).AsTask();
    }
}
