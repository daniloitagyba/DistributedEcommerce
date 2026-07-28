using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Orders.Api.Data;
using Orders.Worker;
using Polly.Registry;
using Testcontainers.PostgreSql;

namespace Orders.IntegrationTests;

public sealed class OrderProjectionStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("orders_projection_test")
        .WithUsername("test_user")
        .WithPassword("test-password-not-a-secret")
        .Build();

    private NpgsqlDataSource _dataSource = null!;
    private ResiliencePipelineProvider<string> _pipelineProvider = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var options = new DbContextOptionsBuilder<OrdersDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;
        await using (var dbContext = new OrdersDbContext(options))
        {
            await dbContext.Database.MigrateAsync();
        }

        _dataSource = NpgsqlDataSource.Create(_postgres.GetConnectionString());
        _pipelineProvider = new ServiceCollection()
            .AddOrdersResilience()
            .BuildServiceProvider()
            .GetRequiredService<ResiliencePipelineProvider<string>>();
    }

    public async Task DisposeAsync()
    {
        await _dataSource.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task OrderCreatedThenPaymentDecidedProducesAFullyPopulatedRow()
    {
        var store = new OrderProjectionStore(_dataSource, _pipelineProvider);
        var orderId = Guid.NewGuid();
        var createdAt = DateTimeOffset.UtcNow;

        await store.ProjectOrderCreatedAsync(orderId, "customer-a", 49.90m, "BRL", createdAt, DateTimeOffset.UtcNow, CancellationToken.None);
        await store.ProjectPaymentDecidedAsync(orderId, "Confirmed", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);

        var summary = await ReadSummaryAsync(orderId);

        Assert.Equal("customer-a", summary.CustomerId);
        Assert.Equal(49.90m, summary.Amount);
        Assert.Equal("BRL", summary.Currency);
        Assert.Equal("Confirmed", summary.Status);
        Assert.NotNull(summary.OrderCreatedAt);
        Assert.NotNull(summary.DecidedAt);
    }

    [Fact]
    public async Task PaymentDecidedArrivingBeforeOrderCreatedStillConvergesToAFullRow()
    {
        var store = new OrderProjectionStore(_dataSource, _pipelineProvider);
        var orderId = Guid.NewGuid();

        // Simulates the projector's two independent topic subscriptions racing:
        // the payment decision reaches the projector before the order-created
        // projection does. Neither write should be lost, and the decided
        // status must not be clobbered once the OrderCreated projection lands.
        await store.ProjectPaymentDecidedAsync(orderId, "Cancelled", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);
        var afterDecisionOnly = await ReadSummaryAsync(orderId);
        Assert.Null(afterDecisionOnly.CustomerId);
        Assert.Equal("Cancelled", afterDecisionOnly.Status);

        await store.ProjectOrderCreatedAsync(orderId, "customer-b", 1500.00m, "BRL", DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, CancellationToken.None);

        var summary = await ReadSummaryAsync(orderId);
        Assert.Equal("customer-b", summary.CustomerId);
        Assert.Equal(1500.00m, summary.Amount);
        Assert.Equal("Cancelled", summary.Status);
    }

    private async Task<(string? CustomerId, decimal? Amount, string? Currency, string Status, DateTimeOffset? OrderCreatedAt, DateTimeOffset? DecidedAt)> ReadSummaryAsync(Guid orderId)
    {
        await using var command = _dataSource.CreateCommand(
            "SELECT customer_id, amount, currency, status, order_created_at, decided_at FROM order_summaries WHERE order_id = @order_id");
        command.Parameters.AddWithValue("order_id", orderId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        return (
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetDecimal(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
            reader.IsDBNull(5) ? null : reader.GetFieldValue<DateTimeOffset>(5));
    }
}
