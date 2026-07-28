using Orders.Api.RateLimiting;

namespace Orders.UnitTests;

public sealed class RateLimitOptionsTests
{
    [Fact]
    public void DefaultsAreWithinValidBounds()
    {
        var options = new RateLimitOptions();

        Assert.True(options.TokenLimit > 0);
        Assert.True(options.TokensPerPeriod > 0);
        Assert.True(options.ReplenishmentPeriodSeconds > 0);
    }
}
