using Orders.Api.Authorization;
using Orders.Api.Contracts;
using Orders.Api.RateLimiting;
using Orders.Application.UseCases.ListOrderSummaries;

namespace Orders.Api.Endpoints;

/// <summary>
/// CQRS query side: reads exclusively from the order_summaries projection
/// built asynchronously by Orders.Worker's projector. Never touches the
/// orders write table or the Redis cache used by GetByIdAsync.
/// </summary>
public static class OrderSummaryEndpoints
{
    public static IEndpointRouteBuilder MapOrderSummaryEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/orders/summary", ListAsync)
            .WithTags("Orders")
            .RequireRateLimiting(RateLimitingExtensions.OrdersPolicy)
            .RequireAuthorization(OrdersAuthorizationPolicies.Read);

        return endpoints;
    }

    private static async Task<IResult> ListAsync(
        ListOrderSummariesHandler handler,
        string? status,
        int? limit,
        CancellationToken cancellationToken)
    {
        var summaries = await handler.HandleAsync(status, limit, cancellationToken);

        var response = summaries.Select(item => new OrderSummaryResponse(
            item.OrderId,
            item.CustomerId,
            item.Amount,
            item.Currency,
            item.Status,
            item.OrderCreatedAt,
            item.DecidedAt,
            item.ProjectedAt));

        return Results.Ok(response);
    }
}
