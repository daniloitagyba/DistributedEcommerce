using Orders.Application.Ports;
using Orders.Domain;

namespace Orders.Application.UseCases.ListOrderSummaries;

public sealed class ListOrderSummariesHandler(IOrderSummaryRepository repository)
{
    private const int MaximumLimit = 100;

    public Task<IReadOnlyList<OrderSummary>> HandleAsync(string? status, int? limit, CancellationToken cancellationToken)
    {
        var effectiveLimit = Math.Clamp(limit ?? 20, 1, MaximumLimit);
        return repository.ListAsync(status, effectiveLimit, cancellationToken);
    }
}
