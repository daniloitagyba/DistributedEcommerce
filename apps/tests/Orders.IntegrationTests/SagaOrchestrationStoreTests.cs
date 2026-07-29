using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Orders.Infrastructure.Data;
using Orders.Worker;
using Polly.Registry;
using Testcontainers.PostgreSql;

namespace Orders.IntegrationTests;

public sealed class SagaOrchestrationStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("orders_test")
        .WithUsername("test_user")
        .WithPassword("test-password-not-a-secret")
        .Build();

    private NpgsqlDataSource? _dataSource;
    private SagaOrchestrationStore? _store;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var options = new DbContextOptionsBuilder<OrdersDbContext>()
            .UseNpgsql(_postgres.GetConnectionString())
            .Options;
        await using (var context = new OrdersDbContext(options))
        {
            await context.Database.MigrateAsync();
        }

        var pipelineProvider = new ServiceCollection()
            .AddOrdersResilience()
            .BuildServiceProvider()
            .GetRequiredService<ResiliencePipelineProvider<string>>();

        _dataSource = NpgsqlDataSource.Create(_postgres.GetConnectionString());
        _store = new SagaOrchestrationStore(_dataSource, pipelineProvider);
    }

    public async Task DisposeAsync()
    {
        if (_dataSource is not null)
        {
            await _dataSource.DisposeAsync();
        }

        await _postgres.DisposeAsync();
    }

    [Fact]
    public async Task TrackedSagaCanBeCompletedByReplyAndDisappearsAfterward()
    {
        var orderId = Guid.NewGuid();
        var requestedAt = DateTimeOffset.UtcNow;

        await _store!.TrackRequestedAsync(orderId, "correlation-1", requestedAt, CancellationToken.None);
        var completed = await _store.TryCompleteRepliedAsync(orderId, CancellationToken.None);
        var secondAttempt = await _store.TryCompleteRepliedAsync(orderId, CancellationToken.None);

        Assert.NotNull(completed);
        Assert.Equal("correlation-1", completed!.CorrelationId);
        Assert.Equal(requestedAt.ToUnixTimeMilliseconds(), completed.RequestedAt.ToUnixTimeMilliseconds());
        Assert.Null(secondAttempt);
    }

    [Fact]
    public async Task TryCompleteRepliedAsyncReturnsNullForAnUntrackedOrder()
    {
        var result = await _store!.TryCompleteRepliedAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task ClaimTimedOutAsyncOnlyClaimsSagasPastTheCutoffAndRemovesThem()
    {
        var timeout = TimeSpan.FromMinutes(5);
        var now = DateTimeOffset.UtcNow;
        var staleOrderId = Guid.NewGuid();
        var freshOrderId = Guid.NewGuid();

        await _store!.TrackRequestedAsync(staleOrderId, "stale-correlation", now - timeout - TimeSpan.FromSeconds(1), CancellationToken.None);
        await _store.TrackRequestedAsync(freshOrderId, "fresh-correlation", now, CancellationToken.None);

        var firstClaim = await _store.ClaimTimedOutAsync(timeout, now, batchSize: 100, CancellationToken.None);
        var secondClaim = await _store.ClaimTimedOutAsync(timeout, now, batchSize: 100, CancellationToken.None);
        var freshStillPending = await _store.TryCompleteRepliedAsync(freshOrderId, CancellationToken.None);

        Assert.Single(firstClaim);
        Assert.Equal(staleOrderId, firstClaim[0].OrderId);
        Assert.Equal("stale-correlation", firstClaim[0].Saga.CorrelationId);
        Assert.Empty(secondClaim);
        Assert.NotNull(freshStillPending);
    }
}
