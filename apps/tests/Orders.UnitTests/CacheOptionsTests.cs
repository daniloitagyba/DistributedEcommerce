using Orders.Infrastructure.Caching;

namespace Orders.UnitTests;

public sealed class CacheOptionsTests
{
    [Fact]
    public void DefaultsAreWithinValidBounds()
    {
        var options = new CacheOptions();

        Assert.True(options.TimeToLiveSeconds > 0);
        Assert.True(options.LockTimeoutMilliseconds > 0);
        Assert.True(options.LockRetryAttempts >= 0);
        Assert.True(options.LockRetryDelayMilliseconds > 0);
    }
}
