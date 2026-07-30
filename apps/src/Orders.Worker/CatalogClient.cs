using System.Net;
using System.Net.Http.Json;

namespace Orders.Worker;

public sealed record CatalogProductSnapshot(string Id, string Name, decimal Price, string Currency, string Sku, string CategorySlug);

public interface ICatalogClient
{
    Task<string?> FindCategorySlugBySkuAsync(string sku, CancellationToken cancellationToken);
}

/// <summary>
/// Milestone 44's reason to exist: recording a category-scoped bestseller
/// entry needs to know which category a Sku belongs to, and Orders.Worker
/// has no reason to duplicate Catalog.Service's own data for that. Reuses
/// the GET /products/by-sku/{sku} endpoint Milestone 42 added for
/// Cart.Service. This is a background worker calling another service
/// synchronously and tolerating its absence - see BestsellersStore's
/// "best-effort" note; a Catalog outage degrades to global-only tracking,
/// not a failed saga.
/// </summary>
public sealed class CatalogClient(HttpClient httpClient) : ICatalogClient
{
    public async Task<string?> FindCategorySlugBySkuAsync(string sku, CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync($"/products/by-sku/{Uri.EscapeDataString(sku)}", cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        var product = await response.Content.ReadFromJsonAsync<CatalogProductSnapshot>(cancellationToken);
        return product?.CategorySlug;
    }
}
