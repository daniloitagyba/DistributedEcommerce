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
