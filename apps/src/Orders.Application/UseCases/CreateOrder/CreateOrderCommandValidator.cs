namespace Orders.Application.UseCases.CreateOrder;

public static class CreateOrderCommandValidator
{
    public static Dictionary<string, string[]> Validate(CreateOrderCommand command)
    {
        var errors = new Dictionary<string, string[]>(StringComparer.Ordinal);

        if (string.IsNullOrWhiteSpace(command.CustomerId) || command.CustomerId.Length > 100)
        {
            errors[nameof(command.CustomerId)] = ["CustomerId is required and must not exceed 100 characters."];
        }

        if (command.Amount <= 0)
        {
            errors[nameof(command.Amount)] = ["Amount must be greater than zero."];
        }

        if (string.IsNullOrWhiteSpace(command.Currency))
        {
            errors[nameof(command.Currency)] = ["Currency is required."];
        }
        else if (command.Currency.Length != 3 || command.Currency.Any(character => !char.IsLetter(character)))
        {
            errors[nameof(command.Currency)] = ["Currency must be a three-letter code."];
        }

        return errors;
    }

    public static string NormalizeCustomerId(string customerId)
    {
        return customerId.Trim();
    }

    public static string NormalizeCurrency(string currency)
    {
        return currency.Trim().ToUpperInvariant();
    }
}
