using Microsoft.EntityFrameworkCore;
using Orders.Application.Ports;
using Orders.Domain;
using Orders.Infrastructure.Data;

namespace Orders.Infrastructure.Persistence;

public sealed class EfOrderSummaryRepository(OrdersDbContext dbContext) : IOrderSummaryRepository
{
    public async Task<IReadOnlyList<OrderSummary>> ListAsync(string? status, int limit, CancellationToken cancellationToken)
    {
        var query = dbContext.OrderSummaries.AsNoTracking();
        if (!string.IsNullOrWhiteSpace(status))
        {
            query = query.Where(item => item.Status == status);
        }

        return await query
            .OrderByDescending(item => item.ProjectedAt)
            .Take(limit)
            .ToListAsync(cancellationToken);
    }
}
