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

    public async Task<bool> TryConfirmAsync(Guid orderId, CancellationToken cancellationToken)
    {
        return await _pipeline.ExecuteAsync(async ct =>
        {
            await using var command = dataSource.CreateCommand(UpdateSql);
            command.Parameters.AddWithValue("status", NpgsqlDbType.Varchar, "Confirmed");
            command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, orderId);
            command.Parameters.AddWithValue("expected_status", NpgsqlDbType.Varchar, "Created");

            return await command.ExecuteNonQueryAsync(ct) == 1;
        }, cancellationToken);
    }
}
