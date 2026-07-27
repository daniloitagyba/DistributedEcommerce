namespace Orders.Api.Caching;

public sealed record CachedOrder(
    Guid Id,
    string CustomerId,
    decimal Amount,
    string Currency,
    string Status,
    DateTimeOffset CreatedAt);
