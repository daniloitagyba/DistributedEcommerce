using Orders.Domain;

namespace Orders.Application.Ports;

public interface IOrderEventStoreRepository
{
    Task<IReadOnlyList<OrderEvent>> ListEventsAsync(Guid orderId, DateTimeOffset? asOf, CancellationToken cancellationToken);
}
