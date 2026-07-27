using Npgsql;
using NpgsqlTypes;

namespace Orders.Worker;

public sealed class InboxStore(NpgsqlDataSource dataSource)
{
    private const string InsertSql = """
        INSERT INTO inbox_messages
            (consumer_name, event_id, topic, partition, "offset", correlation_id, processed_at)
        VALUES
            (@consumer_name, @event_id, @topic, @partition, @offset, @correlation_id, @processed_at)
        ON CONFLICT DO NOTHING;
        """;

    public async Task<bool> TryRecordAsync(
        string consumerName,
        Guid eventId,
        string topic,
        int partition,
        long offset,
        string correlationId,
        DateTimeOffset processedAt,
        CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(InsertSql);
        command.Parameters.AddWithValue("consumer_name", NpgsqlDbType.Varchar, consumerName);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Uuid, eventId);
        command.Parameters.AddWithValue("topic", NpgsqlDbType.Varchar, topic);
        command.Parameters.AddWithValue("partition", NpgsqlDbType.Integer, partition);
        command.Parameters.AddWithValue("offset", NpgsqlDbType.Bigint, offset);
        command.Parameters.AddWithValue("correlation_id", NpgsqlDbType.Varchar, correlationId);
        command.Parameters.AddWithValue("processed_at", NpgsqlDbType.TimestampTz, processedAt);

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
