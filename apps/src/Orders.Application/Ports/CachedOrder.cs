namespace Orders.Application.Ports;

public sealed record CachedOrder(
    Guid Id,
    string CustomerId,
    decimal Amount,
    string Currency,
    string Status,
    DateTimeOffset CreatedAt);
