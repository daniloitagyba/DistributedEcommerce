using BuildingBlocks;
using StackExchange.Redis;

namespace Orders.Worker;

public interface IBestsellersStore
{
    Task RecordSaleAsync(string sku, string? categorySlug, int quantity, CancellationToken cancellationToken);
}

/// <summary>
/// Milestone 44: the payoff for choosing MongoDB over Redis for the
/// catalog back in Milestone 40 - "most sold" is an ordered, frequently
/// incremented, top-N read, exactly what a Redis sorted set is for.
/// ZINCRBY, not a MongoDB aggregation pipeline or a read-modify-write
/// counter row in Postgres. Best-effort and side-channel: a failure here
/// must never fail the saga completion it's reacting to, so no resilience
/// pipeline, no retry, no throw - see OrderSagaReplyConsumer's catch.
/// </summary>
public sealed class RedisBestsellersStore(IConnectionMultiplexer connectionMultiplexer) : IBestsellersStore
{
    public async Task RecordSaleAsync(string sku, string? categorySlug, int quantity, CancellationToken cancellationToken)
    {
        var database = connectionMultiplexer.GetDatabase();
        await database.SortedSetIncrementAsync(BestsellersKeys.Global, sku, quantity).WaitAsync(cancellationToken);

        if (!string.IsNullOrWhiteSpace(categorySlug))
        {
            await database.SortedSetIncrementAsync(BestsellersKeys.Category(categorySlug), sku, quantity).WaitAsync(cancellationToken);
        }
    }
}
