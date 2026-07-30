using System.Text.Json;
using BuildingBlocks;
using Cart.Service.Domain;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;
using StackExchange.Redis;

namespace Cart.Service.Data;

/// <summary>
/// Redis IS the system of record here, not a cache in front of one -
/// there is no Postgres fallback and no cache-aside factory delegate like
/// Orders.Api's RedisOrderCache. If this data is lost, the cart is simply
/// gone; that is an acceptable trade for ephemeral, reconstructable,
/// low-value state, unlike orders or payments. A cart is a single Redis
/// Hash (field = Sku, value = JSON-encoded CartLineItem) so the whole
/// cart can be read, and the whole cart's TTL refreshed, in one round trip.
/// </summary>
public sealed class CartStore(
    IConnectionMultiplexer connectionMultiplexer,
    ResiliencePipelineProvider<string> pipelineProvider,
    IOptions<CartOptions> options)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly CartOptions _options = options.Value;
    private readonly ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.RedisPipeline);

    public Task<IReadOnlyList<CartLineItem>> GetAsync(string cartId, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            var entries = await database.HashGetAllAsync(CartKey(cartId)).WaitAsync(ct);
            return (IReadOnlyList<CartLineItem>)entries
                .Select(entry => Deserialize(entry.Value!))
                .OrderBy(item => item.AddedAt)
                .ToList();
        }, cancellationToken).AsTask();
    }

    public Task<CartLineItem?> GetItemAsync(string cartId, string sku, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            var value = await database.HashGetAsync(CartKey(cartId), sku).WaitAsync(ct);
            return value.HasValue ? Deserialize(value!) : null;
        }, cancellationToken).AsTask();
    }

    public Task UpsertItemAsync(string cartId, CartLineItem item, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            var key = CartKey(cartId);
            var payload = JsonSerializer.Serialize(item, SerializerOptions);
            await database.HashSetAsync(key, item.Sku, payload).WaitAsync(ct);
            await database.KeyExpireAsync(key, TimeSpan.FromSeconds(_options.TimeToLiveSeconds)).WaitAsync(ct);
        }, cancellationToken).AsTask();
    }

    public Task<bool> RemoveItemAsync(string cartId, string sku, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            var key = CartKey(cartId);
            var removed = await database.HashDeleteAsync(key, sku).WaitAsync(ct);
            if (removed && await database.HashLengthAsync(key).WaitAsync(ct) > 0)
            {
                await database.KeyExpireAsync(key, TimeSpan.FromSeconds(_options.TimeToLiveSeconds)).WaitAsync(ct);
            }

            return removed;
        }, cancellationToken).AsTask();
    }

    public Task<bool> ClearAsync(string cartId, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            return await database.KeyDeleteAsync(CartKey(cartId)).WaitAsync(ct);
        }, cancellationToken).AsTask();
    }

    public Task<TimeSpan?> GetTimeToLiveAsync(string cartId, CancellationToken cancellationToken)
    {
        return _pipeline.ExecuteAsync(async ct =>
        {
            var database = connectionMultiplexer.GetDatabase();
            return await database.KeyTimeToLiveAsync(CartKey(cartId)).WaitAsync(ct);
        }, cancellationToken).AsTask();
    }

    private static RedisKey CartKey(string cartId) => $"cart:{cartId}";

    private static CartLineItem Deserialize(RedisValue value)
    {
        return JsonSerializer.Deserialize<CartLineItem>((string)value!, SerializerOptions)
            ?? throw new InvalidOperationException("A cart line item value deserialized to null.");
    }
}
