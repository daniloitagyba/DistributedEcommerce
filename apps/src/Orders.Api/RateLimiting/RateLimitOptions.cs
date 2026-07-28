namespace Orders.Api.RateLimiting;

public sealed class RateLimitOptions
{
    public const string SectionName = "RateLimit";

    public int TokenLimit { get; init; } = 80;

    public int TokensPerPeriod { get; init; } = 75;

    public int ReplenishmentPeriodSeconds { get; init; } = 1;

    public int QueueLimit { get; init; } = 30;
}
