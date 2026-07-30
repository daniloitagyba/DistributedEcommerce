using System.Net;
using System.Net.Http.Json;

namespace Cart.Service.Clients;

public sealed record ProductSnapshot(string Id, string Name, decimal Price, string Currency, string Sku);

public interface ICatalogClient
{
    Task<ProductSnapshot?> FindBySkuAsync(string sku, CancellationToken cancellationToken);
}

/// <summary>
/// Milestone 42's genuinely new communication pattern for this lab:
/// every other inter-service call so far has been async over Kafka
/// (Payments, Inventory, the orchestrated saga). Adding a SKU to a cart
/// needs the current name/price synchronously, right now, to snapshot it -
/// there is no sensible way to do that as a fire-and-forget event. Uses
/// Microsoft.Extensions.Http.Resilience's AddStandardResilienceHandler
/// rather than this codebase's hand-rolled Polly ResiliencePipelineProvider
/// (used for Postgres/Kafka/Redis elsewhere): that provider predates the
/// dedicated HttpClient resilience package, and the dedicated package
/// wraps the identical Polly v8 engine specifically for HttpClient via
/// DelegatingHandlers - this is the idiomatic choice for HTTP, not a
/// foreign import.
/// </summary>
public sealed class CatalogClient(HttpClient httpClient) : ICatalogClient
{
    public async Task<ProductSnapshot?> FindBySkuAsync(string sku, CancellationToken cancellationToken)
    {
        using var response = await httpClient.GetAsync($"/products/by-sku/{Uri.EscapeDataString(sku)}", cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<ProductSnapshot>(cancellationToken);
    }
}
