using BuildingBlocks;
using Npgsql;
using NpgsqlTypes;
using Polly;
using Polly.Registry;

namespace Orders.Worker;

public sealed class OrderStatusStore(NpgsqlDataSource dataSource, ResiliencePipelineProvider<string> pipelineProvider)
{
    private const string UpdateSql = """
        UPDATE orders
        SET status = @status
        WHERE id = @id AND status = @expected_status;
        """;

    private readonly ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.PostgresPipeline);

    public Task<bool> TryConfirmAsync(Guid orderId, CancellationToken cancellationToken)
        => TryTransitionAsync(orderId, "Created", "Confirmed", cancellationToken);

    public Task<bool> TryCancelAsync(Guid orderId, CancellationToken cancellationToken)
        => TryTransitionAsync(orderId, "Created", "Cancelled", cancellationToken);

    private async Task<bool> TryTransitionAsync(
        Guid orderId,
        string fromStatus,
        string toStatus,
        CancellationToken cancellationToken)
    {
        return await _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(UpdateSql);
            command.Parameters.AddWithValue("status", NpgsqlDbType.Varchar, toStatus);
            command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, orderId);
            command.Parameters.AddWithValue("expected_status", NpgsqlDbType.Varchar, fromStatus);

            return await command.ExecuteNonQueryAsync(ct) == 1;
        }, cancellationToken);
    }
}
