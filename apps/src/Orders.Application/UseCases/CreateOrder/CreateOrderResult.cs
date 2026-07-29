using Orders.Domain;

namespace Orders.Application.UseCases.CreateOrder;

public sealed record CreateOrderResult(
    Order? Order,
    Guid EventId,
    IReadOnlyDictionary<string, string[]> ValidationErrors,
    bool WasReplayed = false)
{
    public bool IsValid => ValidationErrors.Count == 0;
}
