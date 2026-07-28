using Payments.Service;

namespace Orders.UnitTests;

public sealed class PaymentDecisionOptionsTests
{
    [Fact]
    public void DefaultsAreWithinValidBounds()
    {
        var options = new PaymentDecisionOptions();

        Assert.True(options.DeclineAmountThreshold > 0);
    }
}
