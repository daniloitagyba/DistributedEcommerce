using System.Text.Json;
using BuildingBlocks;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Orders.Api.Caching;

public sealed class RedisOrderCache(
    IConnectionMultiplexer connectionMultiplexer,
    IOptions<CacheOptions> options) : IOrderCache
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private static readonly RedisValue LockToken = Environment.MachineName;
    private readonly CacheOptions _options = options.Value;

    public async Task<CacheLookup> GetOrCreateAsync(
        Guid id,
        Func<CancellationToken, Task<CachedOrder?>> factory,
        CancellationToken cancellationToken)
    {
        var database = connectionMultiplexer.GetDatabase();
        var cacheKey = OrderCacheKeys.Key(id);

        var cached = await TryReadAsync(database, cacheKey);
        if (cached is not null)
        {
            OrdersTelemetry.RecordCacheHit();
            return new CacheLookup(cached, CacheLookupResult.Hit);
        }

        var lockKey = OrderCacheKeys.LockKey(id);
        var lockTimeout = TimeSpan.FromMilliseconds(_options.LockTimeoutMilliseconds);
        var acquiredLock = await database.LockTakeAsync(lockKey, LockToken, lockTimeout);

        if (!acquiredLock)
        {
            for (var attempt = 0; attempt < _options.LockRetryAttempts; attempt++)
            {
                await Task.Delay(_options.LockRetryDelayMilliseconds, cancellationToken);
                cached = await TryReadAsync(database, cacheKey);
                if (cached is not null)
                {
                    OrdersTelemetry.RecordCacheHit();
                    return new CacheLookup(cached, CacheLookupResult.Hit);
                }
            }

            OrdersTelemetry.RecordCacheMiss();
            var uncached = await factory(cancellationToken);
            return new CacheLookup(uncached, CacheLookupResult.Miss);
        }

        try
        {
            var value = await factory(cancellationToken);
            if (value is not null)
            {
                var payload = JsonSerializer.Serialize(value, SerializerOptions);
                await database.StringSetAsync(cacheKey, payload, TimeSpan.FromSeconds(_options.TimeToLiveSeconds));
            }

            OrdersTelemetry.RecordCacheMiss();
            return new CacheLookup(value, CacheLookupResult.Miss);
        }
        finally
        {
            await database.LockReleaseAsync(lockKey, LockToken);
        }
    }

    private static async Task<CachedOrder?> TryReadAsync(IDatabase database, string cacheKey)
    {
        var value = await database.StringGetAsync(cacheKey);
        if (!value.HasValue)
        {
            return null;
        }

        return JsonSerializer.Deserialize<CachedOrder>((string)value!, SerializerOptions);
    }
}
