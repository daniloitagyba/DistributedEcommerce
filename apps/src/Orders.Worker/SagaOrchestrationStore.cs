using BuildingBlocks;
using Npgsql;
using NpgsqlTypes;
using Polly;
using Polly.Registry;

namespace Orders.Worker;

public sealed record SagaOrchestrationRecord(string CorrelationId, DateTimeOffset RequestedAt);

/// <summary>
/// Milestone 36: Postgres-backed replacement for the Milestone 22
/// in-memory SagaOrchestrationTracker (a ConcurrentDictionary that didn't
/// survive a pod restart or KEDA scale-in). Mirrors OrderEventStoreAppender/
/// OrderProjectionStore's raw-Npgsql style rather than going through EF -
/// EF only owns this table's schema (see Orders.Domain.SagaOrchestrationState
/// and the AddSagaOrchestrationStates migration).
/// </summary>
public sealed class SagaOrchestrationStore(NpgsqlDataSource dataSource, ResiliencePipelineProvider<string> pipelineProvider)
{
    private const string TrackRequestedSql = """
        INSERT INTO saga_orchestration_states (order_id, correlation_id, requested_at)
        VALUES (@order_id, @correlation_id, @requested_at)
        ON CONFLICT (order_id) DO NOTHING;
        """;

    private const string TryCompleteRepliedSql = """
        DELETE FROM saga_orchestration_states
        WHERE order_id = @order_id
        RETURNING correlation_id, requested_at;
        """;

    // FOR UPDATE SKIP LOCKED here is a belt-and-suspenders measure, not the
    // primary correctness mechanism - leader election (LeaderElectionService)
    // already ensures only one orders-worker replica actively sweeps at a
    // time. It guards against the brief window during a leadership handoff
    // where two replicas might both believe they're leading.
    private const string ClaimTimedOutSql = """
        DELETE FROM saga_orchestration_states
        WHERE order_id IN (
            SELECT order_id
            FROM saga_orchestration_states
            WHERE requested_at <= @cutoff
            ORDER BY requested_at
            LIMIT @batch_size
            FOR UPDATE SKIP LOCKED
        )
        RETURNING order_id, correlation_id, requested_at;
        """;

    private readonly ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.PostgresPipeline);

    public Task TrackRequestedAsync(
        Guid orderId,
        string correlationId,
        DateTimeOffset requestedAt,
        CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(TrackRequestedSql);
            command.Parameters.AddWithValue("order_id", NpgsqlDbType.Uuid, orderId);
            command.Parameters.AddWithValue("correlation_id", NpgsqlDbType.Varchar, correlationId);
            command.Parameters.AddWithValue("requested_at", NpgsqlDbType.TimestampTz, requestedAt);
            await command.ExecuteNonQueryAsync(ct);
        }, cancellationToken).AsTask();
    }

    public Task<SagaOrchestrationRecord?> TryCompleteRepliedAsync(Guid orderId, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(TryCompleteRepliedSql);
            command.Parameters.AddWithValue("order_id", NpgsqlDbType.Uuid, orderId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return null;
            }

            return new SagaOrchestrationRecord(reader.GetString(0), reader.GetFieldValue<DateTimeOffset>(1));
        }, cancellationToken).AsTask();
    }

    public Task<IReadOnlyList<(Guid OrderId, SagaOrchestrationRecord Saga)>> ClaimTimedOutAsync(
        TimeSpan timeout,
        DateTimeOffset now,
        int batchSize,
        CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(ClaimTimedOutSql);
            command.Parameters.AddWithValue("cutoff", NpgsqlDbType.TimestampTz, now - timeout);
            command.Parameters.AddWithValue("batch_size", NpgsqlDbType.Integer, batchSize);

            var claimed = new List<(Guid, SagaOrchestrationRecord)>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                claimed.Add((
                    reader.GetFieldValue<Guid>(0),
                    new SagaOrchestrationRecord(reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2))));
            }

            return (IReadOnlyList<(Guid, SagaOrchestrationRecord)>)claimed;
        }, cancellationToken).AsTask();
    }
}
