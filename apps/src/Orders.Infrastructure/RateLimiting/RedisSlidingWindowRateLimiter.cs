using BuildingBlocks;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Registry;
using StackExchange.Redis;

namespace Orders.Infrastructure.RateLimiting;

public sealed record RateLimitDecision(bool Allowed, int Count, int Limit);

/// <summary>
/// Milestone 38: a cluster-wide sliding-window-log rate limiter, contrasted
/// with Milestone 11's per-pod in-memory token bucket. With 3 orders-api
/// replicas, M11's limiter enforces its configured limit independently on
/// each pod - the real, effective cluster-wide ceiling is (replica count *
/// per-pod limit), not the configured number, and it silently changes
/// every time the Rollout scales. This limiter shares state in Redis
/// (a single sorted set per key, member = a per-request GUID, score = the
/// request's timestamp) so the limit means what it says regardless of how
/// many replicas are running. Sliding-window-log rather than a simpler
/// fixed-window counter deliberately - fixed windows allow up to 2x burst
/// right at a window boundary; a sorted set with ZREMRANGEBYSCORE avoids
/// that by tracking exact request timestamps, at the cost of one sorted
/// set entry per allowed request instead of a single counter.
///
/// Applied as a second, independent layer alongside (not replacing) M11's
/// limiter - a fast local check catches obvious abuse without a Redis
/// round-trip on every request; this is the authoritative cluster-wide cap.
/// On Redis unavailability, fails OPEN (allows the request) rather than
/// closed - the same philosophy as RedisOrderCache/RedisIdempotencyStore:
/// this is defense-in-depth on top of the always-available local limiter,
/// not the sole protection.
/// </summary>
public sealed class RedisSlidingWindowRateLimiter(
    IConnectionMultiplexer connectionMultiplexer,
    ResiliencePipelineProvider<string> pipelineProvider,
    IOptions<DistributedRateLimitOptions> options)
{
    private const string SlidingWindowScript = """
        local key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_ms = tonumber(ARGV[2])
        local limit = tonumber(ARGV[3])
        local member = ARGV[4]

        redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window_ms)
        local count = redis.call('ZCARD', key)

        if count < limit then
            redis.call('ZADD', key, now, member)
            redis.call('PEXPIRE', key, window_ms)
            return count + 1
        end

        return -1 - count
        """;

    private readonly DistributedRateLimitOptions _options = options.Value;
    private readonly ResiliencePipeline _pipeline = pipelineProvider.GetPipeline(ResilienceExtensions.RedisPipeline);

    public async Task<RateLimitDecision> TryAcquireAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            return await _pipeline.ExecuteAsync(async ct =>
            {
                var database = connectionMultiplexer.GetDatabase();
                var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                var windowMilliseconds = (long)TimeSpan.FromSeconds(_options.WindowSeconds).TotalMilliseconds;

                var result = (long)await database.ScriptEvaluateAsync(
                    SlidingWindowScript,
                    [key],
                    [now, windowMilliseconds, _options.Limit, Guid.NewGuid().ToString("N")]).WaitAsync(ct);

                return result >= 0
                    ? new RateLimitDecision(Allowed: true, Count: (int)result, _options.Limit)
                    : new RateLimitDecision(Allowed: false, Count: (int)(-result - 1), _options.Limit);
            }, cancellationToken);
        }
        catch (Exception exception) when (ResilienceExtensions.IsInfrastructureFault(exception))
        {
            OrdersTelemetry.RecordDistributedRateLimitBypass();
            return new RateLimitDecision(Allowed: true, Count: -1, _options.Limit);
        }
    }
}
