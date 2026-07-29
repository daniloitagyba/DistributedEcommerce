using BuildingBlocks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orders.Application.Ports;
using Orders.Infrastructure.Idempotency;
using Polly.Registry;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Orders.IntegrationTests;

public sealed class RedisIdempotencyStoreTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder("redis:7.4-alpine").Build();
    private readonly ResiliencePipelineProvider<string> _pipelineProvider = new ServiceCollection()
        .AddOrdersResilience()
        .BuildServiceProvider()
        .GetRequiredService<ResiliencePipelineProvider<string>>();
    private ConnectionMultiplexer? _connectionMultiplexer;

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
    }

    public async Task DisposeAsync()
    {
        if (_connectionMultiplexer is not null)
        {
            await _connectionMultiplexer.DisposeAsync();
        }

        await _redis.DisposeAsync();
    }

    [Fact]
    public async Task GetOrCreateAsyncReplaysTheFirstResultForTheSameIdempotencyKey()
    {
        var store = new RedisIdempotencyStore(_connectionMultiplexer!, _pipelineProvider, Options.Create(new IdempotencyOptions()));
        var idempotencyKey = Guid.NewGuid().ToString();
        var orderId = Guid.NewGuid();
        var factoryCalls = 0;

        Task<CachedOrder> Factory(CancellationToken cancellationToken)
        {
            factoryCalls++;
            return Task.FromResult(new CachedOrder(orderId, "integration-customer", 49.90m, "BRL", "Created", DateTimeOffset.UtcNow));
        }

        var first = await store.GetOrCreateAsync(idempotencyKey, Factory, CancellationToken.None);
        var second = await store.GetOrCreateAsync(idempotencyKey, Factory, CancellationToken.None);

        Assert.False(first.WasReplayed);
        Assert.True(second.WasReplayed);
        Assert.Equal(1, factoryCalls);
        Assert.Equal(first.Order, second.Order);
    }

    [Fact]
    public async Task GetOrCreateAsyncTreatsDifferentIdempotencyKeysIndependently()
    {
        var store = new RedisIdempotencyStore(_connectionMultiplexer!, _pipelineProvider, Options.Create(new IdempotencyOptions()));
        var factoryCalls = 0;

        Task<CachedOrder> Factory(CancellationToken cancellationToken)
        {
            factoryCalls++;
            return Task.FromResult(new CachedOrder(Guid.NewGuid(), "integration-customer", 10m, "BRL", "Created", DateTimeOffset.UtcNow));
        }

        var first = await store.GetOrCreateAsync(Guid.NewGuid().ToString(), Factory, CancellationToken.None);
        var second = await store.GetOrCreateAsync(Guid.NewGuid().ToString(), Factory, CancellationToken.None);

        Assert.False(first.WasReplayed);
        Assert.False(second.WasReplayed);
        Assert.Equal(2, factoryCalls);
        Assert.NotEqual(first.Order!.Id, second.Order!.Id);
    }
}
