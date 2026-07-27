namespace Orders.Api.Caching;

public enum CacheLookupResult
{
    Hit,
    Miss
}

public sealed record CacheLookup(CachedOrder? Order, CacheLookupResult Result);

public interface IOrderCache
{
    Task<CacheLookup> GetOrCreateAsync(
        Guid id,
        Func<CancellationToken, Task<CachedOrder?>> factory,
        CancellationToken cancellationToken);
}
