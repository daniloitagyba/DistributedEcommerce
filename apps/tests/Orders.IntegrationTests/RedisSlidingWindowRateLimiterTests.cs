using BuildingBlocks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orders.Infrastructure.RateLimiting;
using Polly.Registry;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Orders.IntegrationTests;

public sealed class RedisSlidingWindowRateLimiterTests : IAsyncLifetime
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
    public async Task TryAcquireAsyncAllowsRequestsUpToTheLimitThenRejects()
    {
        var limiter = new RedisSlidingWindowRateLimiter(
            _connectionMultiplexer!,
            _pipelineProvider,
            Options.Create(new DistributedRateLimitOptions { Limit = 3, WindowSeconds = 10 }));
        var key = $"test:{Guid.NewGuid():N}";

        var first = await limiter.TryAcquireAsync(key, CancellationToken.None);
        var second = await limiter.TryAcquireAsync(key, CancellationToken.None);
        var third = await limiter.TryAcquireAsync(key, CancellationToken.None);
        var fourth = await limiter.TryAcquireAsync(key, CancellationToken.None);

        Assert.True(first.Allowed);
        Assert.True(second.Allowed);
        Assert.True(third.Allowed);
        Assert.False(fourth.Allowed);
        Assert.Equal(3, fourth.Limit);
    }

    [Fact]
    public async Task TryAcquireAsyncTreatsDifferentKeysIndependently()
    {
        var limiter = new RedisSlidingWindowRateLimiter(
            _connectionMultiplexer!,
            _pipelineProvider,
            Options.Create(new DistributedRateLimitOptions { Limit = 1, WindowSeconds = 10 }));

        var firstKeyFirstCall = await limiter.TryAcquireAsync($"test-a:{Guid.NewGuid():N}", CancellationToken.None);
        var secondKeyFirstCall = await limiter.TryAcquireAsync($"test-b:{Guid.NewGuid():N}", CancellationToken.None);

        Assert.True(firstKeyFirstCall.Allowed);
        Assert.True(secondKeyFirstCall.Allowed);
    }

    [Fact]
    public async Task TryAcquireAsyncAllowsRequestsAgainAfterTheWindowSlidesPast()
    {
        var limiter = new RedisSlidingWindowRateLimiter(
            _connectionMultiplexer!,
            _pipelineProvider,
            Options.Create(new DistributedRateLimitOptions { Limit = 1, WindowSeconds = 1 }));
        var key = $"test:{Guid.NewGuid():N}";

        var first = await limiter.TryAcquireAsync(key, CancellationToken.None);
        var secondImmediately = await limiter.TryAcquireAsync(key, CancellationToken.None);
        await Task.Delay(TimeSpan.FromSeconds(1.2));
        var thirdAfterWindow = await limiter.TryAcquireAsync(key, CancellationToken.None);

        Assert.True(first.Allowed);
        Assert.False(secondImmediately.Allowed);
        Assert.True(thirdAfterWindow.Allowed);
    }
}
