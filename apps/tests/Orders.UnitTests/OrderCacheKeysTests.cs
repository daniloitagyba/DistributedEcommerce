using BuildingBlocks;

namespace Orders.UnitTests;

public sealed class OrderCacheKeysTests
{
    [Fact]
    public void KeyIncludesTheOrderIdentifier()
    {
        var orderId = Guid.Parse("11111111-1111-1111-1111-111111111111");

        Assert.Equal("orders:cache:11111111-1111-1111-1111-111111111111", OrderCacheKeys.Key(orderId));
    }

    [Fact]
    public void LockKeyIsDistinctFromTheCacheKey()
    {
        var orderId = Guid.NewGuid();

        Assert.NotEqual(OrderCacheKeys.Key(orderId), OrderCacheKeys.LockKey(orderId));
        Assert.EndsWith(orderId.ToString(), OrderCacheKeys.LockKey(orderId), StringComparison.Ordinal);
    }
}
