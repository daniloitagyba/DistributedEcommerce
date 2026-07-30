namespace Storefront.Service;

public sealed class CatalogProxyOptions
{
    public const string SectionName = "CatalogProxy";

    public string BaseUrl { get; init; } = "http://localhost:5080";
}

public sealed class CartProxyOptions
{
    public const string SectionName = "CartProxy";

    public string BaseUrl { get; init; } = "http://localhost:5290";
}

public sealed class OrdersProxyOptions
{
    public const string SectionName = "OrdersProxy";

    public string BaseUrl { get; init; } = "http://localhost:5000";
}

public sealed class InventoryProxyOptions
{
    public const string SectionName = "InventoryProxy";

    public string BaseUrl { get; init; } = "http://localhost:5170";
}

/// <summary>
/// Milestone 54: the product-summary endpoint fans out to Catalog and
/// Inventory in parallel and waits for both - a fan-out's tail latency is
/// at least as bad as its slowest leg, and gets worse the more legs there
/// are (Dean &amp; Barroso's "Tail At Scale"). HedgeDelayMilliseconds, when
/// greater than 0, fires a second parallel request to Inventory if the
/// first hasn't answered within that delay, taking whichever answers
/// first - the same mitigation Milestone 39 proved for GET /orders/{id},
/// applied here at the point the slow leg is actually called rather than
/// by the outer caller (this fan-out happens server-side, unlike
/// Milestone 39's, so the hedge has to live here too). 0 (the default)
/// disables hedging entirely.
/// </summary>
public sealed class ProductSummaryOptions
{
    public const string SectionName = "ProductSummary";

    public int HedgeDelayMilliseconds { get; init; }
}
