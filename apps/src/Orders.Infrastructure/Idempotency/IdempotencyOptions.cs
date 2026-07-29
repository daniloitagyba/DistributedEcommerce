namespace Orders.Infrastructure.Idempotency;

public sealed class IdempotencyOptions
{
    public const string SectionName = "Idempotency";

    public int TimeToLiveHours { get; init; } = 24;

    public int LockTimeoutMilliseconds { get; init; } = 2_000;

    public int LockRetryAttempts { get; init; } = 5;

    public int LockRetryDelayMilliseconds { get; init; } = 100;
}
