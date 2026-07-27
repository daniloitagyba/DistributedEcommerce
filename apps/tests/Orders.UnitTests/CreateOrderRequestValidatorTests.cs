using Orders.Api.Contracts;

namespace Orders.UnitTests;

public sealed class CreateOrderRequestValidatorTests
{
    [Fact]
    public void ValidateReturnsNoErrorsForAValidRequest()
    {
        var request = new CreateOrderRequest("customer-42", 19.95m, "brl");

        var errors = CreateOrderRequestValidator.Validate(request);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateReturnsAllRelevantErrorsForAnInvalidRequest()
    {
        var request = new CreateOrderRequest(" ", 0m, "12");

        var errors = CreateOrderRequestValidator.Validate(request);

        Assert.Contains(nameof(request.CustomerId), errors.Keys);
        Assert.Contains(nameof(request.Amount), errors.Keys);
        Assert.Contains(nameof(request.Currency), errors.Keys);
    }

    [Fact]
    public void NormalizationTrimsTheCustomerAndUppercasesTheCurrency()
    {
        var customerId = CreateOrderRequestValidator.NormalizeCustomerId("  customer-42  ");
        var currency = CreateOrderRequestValidator.NormalizeCurrency(" brl ");

        Assert.Equal("customer-42", customerId);
        Assert.Equal("BRL", currency);
    }
}
