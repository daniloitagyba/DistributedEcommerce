using System.Net;
using Microsoft.Extensions.Options;

namespace Storefront.Service;

internal static partial class StorefrontLog
{
    [LoggerMessage(EventId = 7001, Level = LogLevel.Warning, Message = "{Dependency} unavailable for sku {Sku}, degrading response")]
    public static partial void DependencyUnavailable(ILogger logger, string dependency, string sku, Exception exception);
}

/// <summary>
/// Milestone 54: this lab's first genuine BFF fan-out - every other
/// Storefront.Service endpoint (ProxyEndpoints) is a 1:1 reverse proxy.
/// GET /api/storefront/products/{sku} calls Catalog and Inventory in
/// parallel and waits for both, which means its own tail latency is at
/// least as bad as whichever of the two is having a slow moment - see
/// ProductSummaryOptions for why the Inventory leg can be hedged.
/// </summary>
public static class StorefrontEndpoints
{
    public static IEndpointRouteBuilder MapStorefrontEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/api/storefront/products/{sku}", GetProductSummaryAsync).WithTags("Storefront");
        return endpoints;
    }

    private static async Task<IResult> GetProductSummaryAsync(
        string sku,
        IHttpClientFactory httpClientFactory,
        IOptions<ProductSummaryOptions> options,
        ILoggerFactory loggerFactory,
        CancellationToken cancellationToken)
    {
        var logger = loggerFactory.CreateLogger("Storefront.Service.StorefrontEndpoints");
        var catalogClient = httpClientFactory.CreateClient("catalog");
        var inventoryClient = httpClientFactory.CreateClient("inventory");
        var hedgeDelayMs = options.Value.HedgeDelayMilliseconds;

        var catalogTask = GetJsonOrNullAsync(catalogClient, $"/products/by-sku/{Uri.EscapeDataString(sku)}", cancellationToken);
        var inventoryTask = hedgeDelayMs > 0
            ? GetHedgedJsonOrNullAsync(inventoryClient, $"/inventory/{Uri.EscapeDataString(sku)}", hedgeDelayMs, cancellationToken)
            : GetJsonOrNullAsync(inventoryClient, $"/inventory/{Uri.EscapeDataString(sku)}", cancellationToken);

        // Milestone 64: awaited separately, not via Task.WhenAll - Inventory
        // is an enrichment of an otherwise-complete Catalog response, not a
        // second required source. Task.WhenAll propagates whichever task
        // faults first and fails the whole request even when Catalog (the
        // side that actually determines whether this SKU exists) answered
        // fine - the exact opposite of what a BFF fanning out to several
        // backends should do on a partial failure. Both legs are still
        // always awaited to completion here, regardless of which one
        // faults first, so neither is ever left unobserved.
        object? product;
        bool catalogUnavailable;
        try
        {
            product = await catalogTask;
            catalogUnavailable = false;
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
        {
            StorefrontLog.DependencyUnavailable(logger, "catalog", sku, exception);
            product = null;
            catalogUnavailable = true;
        }

        object? inventory;
        bool inventoryUnavailable;
        try
        {
            inventory = await inventoryTask;
            inventoryUnavailable = false;
        }
        catch (Exception exception) when (exception is HttpRequestException or TaskCanceledException)
        {
            StorefrontLog.DependencyUnavailable(logger, "inventory", sku, exception);
            inventory = null;
            inventoryUnavailable = true;
        }

        if (catalogUnavailable)
        {
            return Results.Problem(
                detail: "The catalog service is currently unavailable.",
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        if (product is null)
        {
            return Results.NotFound(new { message = $"No product with sku '{sku}' was found in the catalog." });
        }

        return Results.Ok(new { product, inventory, degraded = inventoryUnavailable });
    }

    /// <summary>
    /// Fires the primary request; if it hasn't answered within
    /// <paramref name="hedgeDelayMs"/>, fires a second, independent request
    /// to the same logical backend (the K8s Service load-balances new
    /// connections across replicas, so a hedge has a real chance of
    /// landing on a different, non-lagging pod) and takes whichever
    /// finishes first. The loser is cancelled, not left to run to
    /// completion in the background - Milestone 54 measured this matters,
    /// not just tidiness: under a sustained tail of slow responses,
    /// uncancelled losers pile up and contend for the same HttpClient's
    /// connection pool, inflating p99/max even as hedging correctly
    /// improves p95. Safe to cancel here because this is a read-only
    /// GET - cancelling a write mid-flight would risk aborting one the
    /// server is still going to act on, which this endpoint never does.
    /// </summary>
    private static async Task<object?> GetHedgedJsonOrNullAsync(
        HttpClient client,
        string requestUri,
        int hedgeDelayMs,
        CancellationToken cancellationToken)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var primary = GetJsonOrNullAsync(client, requestUri, linkedCts.Token);
        var delay = Task.Delay(hedgeDelayMs, cancellationToken);

        var firstToFinish = await Task.WhenAny(primary, delay);
        if (firstToFinish == primary)
        {
            return await primary;
        }

        var hedge = GetJsonOrNullAsync(client, requestUri, linkedCts.Token);
        var winner = await Task.WhenAny(primary, hedge);
        var result = await winner;
        await linkedCts.CancelAsync();
        return result;
    }

    private static async Task<object?> GetJsonOrNullAsync(HttpClient client, string requestUri, CancellationToken cancellationToken)
    {
        using var response = await client.GetAsync(requestUri, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<object>(cancellationToken);
    }
}
