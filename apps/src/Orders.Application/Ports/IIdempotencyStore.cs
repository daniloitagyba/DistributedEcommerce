namespace Orders.Application.Ports;

public sealed record IdempotencyLookup(CachedOrder? Order, bool WasReplayed);

public interface IIdempotencyStore
{
    Task<IdempotencyLookup> GetOrCreateAsync(
        string idempotencyKey,
        Func<CancellationToken, Task<CachedOrder>> factory,
        CancellationToken cancellationToken);
}
