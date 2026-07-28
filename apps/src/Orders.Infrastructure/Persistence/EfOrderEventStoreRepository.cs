using Microsoft.EntityFrameworkCore;
using Orders.Application.Ports;
using Orders.Domain;
using Orders.Infrastructure.Data;

namespace Orders.Infrastructure.Persistence;

public sealed class EfOrderEventStoreRepository(OrdersDbContext dbContext) : IOrderEventStoreRepository
{
    public async Task<IReadOnlyList<OrderEvent>> ListEventsAsync(Guid orderId, DateTimeOffset? asOf, CancellationToken cancellationToken)
    {
        var query = dbContext.OrderEvents.AsNoTracking().Where(item => item.OrderId == orderId);
        if (asOf is { } boundary)
        {
            query = query.Where(item => item.OccurredAt <= boundary);
        }

        return await query.OrderBy(item => item.Id).ToListAsync(cancellationToken);
    }
}
