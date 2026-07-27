using BuildingBlocks;
using StackExchange.Redis;

namespace Orders.Worker;

public interface IOrderCacheInvalidator
{
    Task InvalidateAsync(Guid orderId, CancellationToken cancellationToken);
}

public sealed class RedisOrderCacheInvalidator(IConnectionMultiplexer connectionMultiplexer) : IOrderCacheInvalidator
{
    public async Task InvalidateAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var database = connectionMultiplexer.GetDatabase();
        await database.KeyDeleteAsync(OrderCacheKeys.Key(orderId));
    }
}
