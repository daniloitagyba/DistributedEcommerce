namespace Orders.Api.Contracts;

public sealed record CreateOrderRequest(
    string? CustomerId,
    decimal Amount,
    string? Currency);

public sealed record OrderResponse(
    Guid Id,
    string CustomerId,
    decimal Amount,
    string Currency,
    string Status,
    DateTimeOffset CreatedAt,
    string CorrelationId,
    string InstanceId);

/// <summary>
/// Read-model projection (CQRS query side). Built asynchronously by the
/// projector in Orders.Worker; fields can be null for a short window if this
/// row was seeded by a PaymentDecided event that arrived before the
/// corresponding OrderCreated event was projected.
/// </summary>
public sealed record OrderSummaryResponse(
    Guid OrderId,
    string? CustomerId,
    decimal? Amount,
    string? Currency,
    string Status,
    DateTimeOffset? OrderCreatedAt,
    DateTimeOffset? DecidedAt,
    DateTimeOffset ProjectedAt);

public static class CreateOrderRequestValidator
{
    public static Dictionary<string, string[]> Validate(CreateOrderRequest request)
    {
        var errors = new Dictionary<string, string[]>(StringComparer.Ordinal);

        if (string.IsNullOrWhiteSpace(request.CustomerId) || request.CustomerId.Length > 100)
        {
            errors[nameof(request.CustomerId)] = ["CustomerId is required and must not exceed 100 characters."];
        }

        if (request.Amount <= 0)
        {
            errors[nameof(request.Amount)] = ["Amount must be greater than zero."];
        }

        if (string.IsNullOrWhiteSpace(request.Currency))
        {
            errors[nameof(request.Currency)] = ["Currency is required."];
        }
        else if (request.Currency.Length != 3 || request.Currency.Any(character => !char.IsLetter(character)))
        {
            errors[nameof(request.Currency)] = ["Currency must be a three-letter code."];
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
