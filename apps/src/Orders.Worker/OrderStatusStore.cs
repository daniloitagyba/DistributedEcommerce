using Npgsql;
using NpgsqlTypes;

namespace Orders.Worker;

public sealed class OrderStatusStore(NpgsqlDataSource dataSource)
{
    private const string UpdateSql = """
        UPDATE orders
        SET status = @status
        WHERE id = @id AND status = @expected_status;
        """;

    public async Task<bool> TryConfirmAsync(Guid orderId, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(UpdateSql);
        command.Parameters.AddWithValue("status", NpgsqlDbType.Varchar, "Confirmed");
        command.Parameters.AddWithValue("id", NpgsqlDbType.Uuid, orderId);
        command.Parameters.AddWithValue("expected_status", NpgsqlDbType.Varchar, "Created");

        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
