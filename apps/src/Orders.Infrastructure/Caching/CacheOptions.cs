namespace Orders.Infrastructure.Caching;

public sealed class CacheOptions
{
    public const string SectionName = "Cache";

    public int TimeToLiveSeconds { get; init; } = 30;

    public int LockTimeoutMilliseconds { get; init; } = 2_000;

    public int LockRetryAttempts { get; init; } = 3;

    public int LockRetryDelayMilliseconds { get; init; } = 50;
}
