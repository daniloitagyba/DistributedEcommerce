using Orders.Worker;

namespace Orders.UnitTests;

public sealed class RetryDelayCalculatorTests
{
    [Theory]
    [InlineData(1, 250)]
    [InlineData(2, 500)]
    [InlineData(3, 1000)]
    [InlineData(8, 1000)]
    public void CalculateUsesCappedExponentialBackoff(int completedAttempt, int expectedMilliseconds)
    {
        var delay = RetryDelayCalculator.Calculate(completedAttempt, 250, 1_000);

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), delay);
    }

    [Fact]
    public void CalculateRejectsNonPositiveAttempts()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RetryDelayCalculator.Calculate(0, 250, 1_000));
    }
}
