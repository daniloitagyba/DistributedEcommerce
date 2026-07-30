using BuildingBlocks;
using Cart.Service.Data;
using Cart.Service.Domain;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Polly.Registry;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Cart.IntegrationTests;

public sealed class CartStoreTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder("redis:7.4-alpine").Build();
    private readonly ResiliencePipelineProvider<string> _pipelineProvider = new ServiceCollection()
        .AddOrdersResilience()
        .BuildServiceProvider()
        .GetRequiredService<ResiliencePipelineProvider<string>>();
    private ConnectionMultiplexer? _connectionMultiplexer;
    private CartStore _store = null!;

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();
        _connectionMultiplexer = await ConnectionMultiplexer.ConnectAsync(_redis.GetConnectionString());
        _store = new CartStore(_connectionMultiplexer, _pipelineProvider, Options.Create(new CartOptions { TimeToLiveSeconds = 60 }));
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
    public async Task UpsertThenGetReturnsTheSameItemAndSetsATtl()
    {
        var cartId = Guid.NewGuid().ToString("N");
        var item = new CartLineItem("SKU-1", 2, 49.90m, "BRL", "Widget", DateTimeOffset.UtcNow);

        await _store.UpsertItemAsync(cartId, item, CancellationToken.None);
        var items = await _store.GetAsync(cartId, CancellationToken.None);
        var ttl = await _store.GetTimeToLiveAsync(cartId, CancellationToken.None);

        Assert.Single(items);
        Assert.Equal(item, items[0]);
        Assert.NotNull(ttl);
        Assert.True(ttl!.Value.TotalSeconds is > 0 and <= 60);
    }

    [Fact]
    public async Task UpsertWithTheSameSkuOverwritesQuantityRatherThanDuplicating()
    {
        var cartId = Guid.NewGuid().ToString("N");
        var addedAt = DateTimeOffset.UtcNow;
        var item = new CartLineItem("SKU-1", 1, 10m, "BRL", "Widget", addedAt);

        await _store.UpsertItemAsync(cartId, item, CancellationToken.None);
        await _store.UpsertItemAsync(cartId, item.WithQuantity(5), CancellationToken.None);
        var items = await _store.GetAsync(cartId, CancellationToken.None);

        Assert.Single(items);
        Assert.Equal(5, items[0].Quantity);
    }

    [Fact]
    public async Task RemoveItemAsyncDeletesOnlyTheGivenSkuAndClearAsyncDeletesTheWholeCart()
    {
        var cartId = Guid.NewGuid().ToString("N");
        await _store.UpsertItemAsync(cartId, new CartLineItem("SKU-1", 1, 10m, "BRL", "A", DateTimeOffset.UtcNow), CancellationToken.None);
        await _store.UpsertItemAsync(cartId, new CartLineItem("SKU-2", 1, 20m, "BRL", "B", DateTimeOffset.UtcNow), CancellationToken.None);

        var removed = await _store.RemoveItemAsync(cartId, "SKU-1", CancellationToken.None);
        var afterRemove = await _store.GetAsync(cartId, CancellationToken.None);

        Assert.True(removed);
        Assert.Single(afterRemove);
        Assert.Equal("SKU-2", afterRemove[0].Sku);

        var cleared = await _store.ClearAsync(cartId, CancellationToken.None);
        var afterClear = await _store.GetAsync(cartId, CancellationToken.None);

        Assert.True(cleared);
        Assert.Empty(afterClear);
    }

    [Fact]
    public async Task GetAsyncOnAnUnknownCartReturnsAnEmptyListRatherThanThrowing()
    {
        var items = await _store.GetAsync(Guid.NewGuid().ToString("N"), CancellationToken.None);

        Assert.Empty(items);
    }
}
