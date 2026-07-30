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
