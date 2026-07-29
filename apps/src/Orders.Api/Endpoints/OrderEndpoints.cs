using Orders.Api.Authorization;
using Orders.Api.Contracts;
using Orders.Api.Middleware;
using Orders.Api.RateLimiting;
using Orders.Application.Exceptions;
using Orders.Application.Ports;
using Orders.Application.UseCases.CreateOrder;
using Orders.Application.UseCases.GetOrder;
using Orders.Domain;

namespace Orders.Api.Endpoints;

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/orders").WithTags("Orders").RequireRateLimiting(RateLimitingExtensions.OrdersPolicy);

        group.MapPost("", CreateAsync).RequireAuthorization(OrdersAuthorizationPolicies.Write);
        group.MapGet("/{id:guid}", GetByIdAsync).RequireAuthorization(OrdersAuthorizationPolicies.Read);

        return endpoints;
    }

    private static async Task<IResult> CreateAsync(
        CreateOrderRequest request,
        CreateOrderHandler handler,
        IConfiguration configuration,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        var correlationId = httpContext.GetCorrelationId();
        var instanceId = configuration["InstanceId"] ?? Environment.MachineName;
        var command = new CreateOrderCommand(request.CustomerId, request.Amount, request.Currency, correlationId, instanceId);

        CreateOrderResult result;
        try
        {
            result = await handler.HandleAsync(command, cancellationToken);
        }
        catch (InfrastructureUnavailableException)
        {
            return ServiceUnavailable(httpContext, "PostgreSQL is currently unavailable.");
        }

        if (!result.IsValid)
        {
            return Results.ValidationProblem(result.ValidationErrors);
        }

        var response = ToResponse(result.Order!, correlationId, instanceId);
        return Results.Created($"/orders/{result.Order!.Id}", response);
    }

    private static async Task<IResult> GetByIdAsync(
        Guid id,
        GetOrderHandler handler,
        IConfiguration configuration,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        GetOrderResult result;
        try
        {
            result = await handler.HandleAsync(id, cancellationToken);
        }
        catch (InfrastructureUnavailableException)
        {
            return ServiceUnavailable(httpContext, "PostgreSQL is currently unavailable.");
        }

        if (result.Order is null)
        {
            return Results.NotFound();
        }

        httpContext.Response.Headers["X-Cache"] = result.LookupResult switch
        {
            CacheLookupResult.Hit => "HIT",
            CacheLookupResult.Bypassed => "BYPASS",
            _ => "MISS"
        };

        return Results.Ok(ToResponse(
            result.Order,
            httpContext.GetCorrelationId(),
            configuration["InstanceId"] ?? Environment.MachineName));
    }

    private static IResult ServiceUnavailable(HttpContext httpContext, string detail)
    {
        httpContext.Response.Headers["Retry-After"] = "5";
        return Results.Problem(
            detail: detail,
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Service Unavailable");
    }

    private static OrderResponse ToResponse(
        Order order,
        string correlationId,
        string instanceId)
    {
        return new OrderResponse(
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.Status,
            order.CreatedAt,
            correlationId,
            instanceId);
    }

    private static OrderResponse ToResponse(
        CachedOrder order,
        string correlationId,
        string instanceId)
    {
        return new OrderResponse(
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.Status,
            order.CreatedAt,
            correlationId,
            instanceId);
    }
}
