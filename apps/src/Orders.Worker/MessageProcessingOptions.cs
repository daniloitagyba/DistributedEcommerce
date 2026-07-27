namespace Orders.Worker;

public sealed class MessageProcessingOptions
{
    public const string SectionName = "MessageProcessing";

    public int MaximumAttempts { get; init; } = 3;

    public int InitialRetryDelayMilliseconds { get; init; } = 250;

    public int MaximumRetryDelayMilliseconds { get; init; } = 5_000;

    public int InfrastructureRetryDelayMilliseconds { get; init; } = 1_000;
}

public static class RetryDelayCalculator
{
    public static TimeSpan Calculate(int completedAttempt, int initialDelayMilliseconds, int maximumDelayMilliseconds)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(completedAttempt, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(initialDelayMilliseconds, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maximumDelayMilliseconds, 1);

        var exponent = Math.Min(completedAttempt - 1, 10);
        var delay = Math.Min(maximumDelayMilliseconds, initialDelayMilliseconds * (1 << exponent));
        return TimeSpan.FromMilliseconds(delay);
    }
}
