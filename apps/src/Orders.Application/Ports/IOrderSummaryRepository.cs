using Orders.Domain;

namespace Orders.Application.Ports;

public interface IOrderSummaryRepository
{
    Task<IReadOnlyList<OrderSummary>> ListAsync(string? status, int limit, CancellationToken cancellationToken);
}
