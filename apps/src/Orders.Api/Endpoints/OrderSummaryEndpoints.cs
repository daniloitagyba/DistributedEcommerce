using Microsoft.EntityFrameworkCore;
using Orders.Api.Contracts;
using Orders.Api.Data;
using Orders.Api.RateLimiting;

namespace Orders.Api.Endpoints;

/// <summary>
/// CQRS query side: reads exclusively from the order_summaries projection
/// built asynchronously by Orders.Worker's projector. Never touches the
/// orders write table or the Redis cache used by GetByIdAsync.
/// </summary>
public static class OrderSummaryEndpoints
{
    private const int MaximumLimit = 100;

    public static IEndpointRouteBuilder MapOrderSummaryEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/orders/summary", ListAsync)
            .WithTags("Orders")
            .RequireRateLimiting(RateLimitingExtensions.OrdersPolicy);

        return endpoints;
    }

    private static async Task<IResult> ListAsync(
        OrdersDbContext dbContext,
        string? status,
        int? limit,
        CancellationToken cancellationToken)
    {
        var effectiveLimit = Math.Clamp(limit ?? 20, 1, MaximumLimit);

        var query = dbContext.OrderSummaries.AsNoTracking();
        if (!string.IsNullOrWhiteSpace(status))
        {
            query = query.Where(item => item.Status == status);
        }

        var summaries = await query
            .OrderByDescending(item => item.ProjectedAt)
            .Take(effectiveLimit)
            .Select(item => new OrderSummaryResponse(
                item.OrderId,
                item.CustomerId,
                item.Amount,
                item.Currency,
                item.Status,
                item.OrderCreatedAt,
                item.DecidedAt,
                item.ProjectedAt))
            .ToListAsync(cancellationToken);

        return Results.Ok(summaries);
    }
}
