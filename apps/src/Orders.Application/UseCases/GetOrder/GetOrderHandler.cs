using Orders.Application.Ports;
using Orders.Domain;

namespace Orders.Application.UseCases.GetOrder;

public sealed class GetOrderHandler(IOrderCache cache, IOrderRepository repository)
{
    public async Task<GetOrderResult> HandleAsync(Guid id, CancellationToken cancellationToken)
    {
        var lookup = await cache.GetOrCreateAsync(
            id,
            async ct =>
            {
                var order = await repository.FindByIdAsync(id, ct);
                return order is null ? null : ToCachedOrder(order);
            },
            cancellationToken);

        return new GetOrderResult(lookup.Order, lookup.Result);
    }

    private static CachedOrder ToCachedOrder(Order order)
    {
        return new CachedOrder(
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.Status,
            order.CreatedAt);
    }
}
