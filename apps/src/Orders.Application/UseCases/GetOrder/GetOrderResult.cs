using Orders.Application.Ports;

namespace Orders.Application.UseCases.GetOrder;

public sealed record GetOrderResult(CachedOrder? Order, CacheLookupResult LookupResult);
