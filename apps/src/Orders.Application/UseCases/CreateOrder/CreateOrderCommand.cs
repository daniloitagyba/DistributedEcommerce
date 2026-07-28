namespace Orders.Application.UseCases.CreateOrder;

public sealed record CreateOrderCommand(
    string? CustomerId,
    decimal Amount,
    string? Currency,
    string CorrelationId,
    string InstanceId);
