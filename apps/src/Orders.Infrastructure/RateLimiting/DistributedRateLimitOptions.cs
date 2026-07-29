namespace Orders.Infrastructure.RateLimiting;

public sealed class DistributedRateLimitOptions
{
    public const string SectionName = "DistributedRateLimit";

    public int Limit { get; init; } = 150;

    public int WindowSeconds { get; init; } = 10;
}
