using Orders.Application.UseCases.CreateOrder;

namespace Orders.UnitTests;

public sealed class CreateOrderCommandValidatorTests
{
    [Fact]
    public void ValidateReturnsNoErrorsForAValidCommand()
    {
        var command = new CreateOrderCommand("customer-42", 19.95m, "brl", "correlation-1", "instance-1");

        var errors = CreateOrderCommandValidator.Validate(command);

        Assert.Empty(errors);
    }

    [Fact]
    public void ValidateReturnsAllRelevantErrorsForAnInvalidCommand()
    {
        var command = new CreateOrderCommand(" ", 0m, "12", "correlation-1", "instance-1");

        var errors = CreateOrderCommandValidator.Validate(command);

        Assert.Contains(nameof(command.CustomerId), errors.Keys);
        Assert.Contains(nameof(command.Amount), errors.Keys);
        Assert.Contains(nameof(command.Currency), errors.Keys);
    }

    [Fact]
    public void NormalizationTrimsTheCustomerAndUppercasesTheCurrency()
    {
        var customerId = CreateOrderCommandValidator.NormalizeCustomerId("  customer-42  ");
        var currency = CreateOrderCommandValidator.NormalizeCurrency(" brl ");

        Assert.Equal("customer-42", customerId);
        Assert.Equal("BRL", currency);
    }
}
