using System.Diagnostics;
using System.Text.Json;
using BuildingBlocks;
using Microsoft.EntityFrameworkCore;
using Orders.Api.Caching;
using Orders.Api.Contracts;
using Orders.Api.Data;
using Orders.Api.Domain;
using Orders.Api.Middleware;

namespace Orders.Api.Endpoints;

public static class OrderEndpoints
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/orders").WithTags("Orders");

        group.MapPost("", CreateAsync);
        group.MapGet("/{id:guid}", GetByIdAsync);

        return endpoints;
    }

    private static async Task<IResult> CreateAsync(
        CreateOrderRequest request,
        OrdersDbContext dbContext,
        IConfiguration configuration,
        HttpContext httpContext,
        ILogger<OrderEndpointLog> logger,
        CancellationToken cancellationToken)
    {
        var errors = CreateOrderRequestValidator.Validate(request);
        if (errors.Count > 0)
        {
            return Results.ValidationProblem(errors);
        }

        var customerId = CreateOrderRequestValidator.NormalizeCustomerId(request.CustomerId!);
        var currency = CreateOrderRequestValidator.NormalizeCurrency(request.Currency!);
        var createdAt = DateTimeOffset.UtcNow;
        var order = Order.Create(customerId, request.Amount, currency, createdAt);
        var correlationId = httpContext.GetCorrelationId();
        var instanceId = configuration["InstanceId"] ?? Environment.MachineName;
        var orderCreated = new OrderCreated(
            Guid.NewGuid(),
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.CreatedAt,
            correlationId);
        var outboxMessage = OutboxMessage.Create(
            orderCreated.EventId,
            nameof(OrderCreated),
            JsonSerializer.Serialize(orderCreated, SerializerOptions),
            orderCreated.OccurredAt,
            correlationId,
            Activity.Current?.Id,
            Activity.Current?.TraceStateString);

        Activity.Current?.SetTag("order.id", order.Id);
        Activity.Current?.SetTag("order.currency", order.Currency);
        Activity.Current?.SetTag("messaging.message.id", orderCreated.EventId);
        Activity.Current?.SetTag("service.instance.id", instanceId);

        await using var transaction = await dbContext.Database.BeginTransactionAsync(cancellationToken);
        dbContext.Orders.Add(order);
        dbContext.OutboxMessages.Add(outboxMessage);
        await dbContext.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        OrdersTelemetry.RecordCreated(order.Currency);
        OrderEndpointLog.OrderAccepted(
            logger,
            order.Id,
            orderCreated.EventId,
            instanceId,
            correlationId);

        var response = ToResponse(order, correlationId, instanceId);
        return Results.Created($"/orders/{order.Id}", response);
    }

    private static async Task<IResult> GetByIdAsync(
        Guid id,
        OrdersDbContext dbContext,
        IOrderCache cache,
        IConfiguration configuration,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        var lookup = await cache.GetOrCreateAsync(
            id,
            async token =>
            {
                var order = await dbContext.Orders.AsNoTracking().SingleOrDefaultAsync(item => item.Id == id, token);
                return order is null ? null : ToCachedOrder(order);
            },
            cancellationToken);

        if (lookup.Order is null)
        {
            return Results.NotFound();
        }

        httpContext.Response.Headers["X-Cache"] = lookup.Result == CacheLookupResult.Hit ? "HIT" : "MISS";

        return Results.Ok(ToResponse(
            lookup.Order,
            httpContext.GetCorrelationId(),
            configuration["InstanceId"] ?? Environment.MachineName));
    }

    private static CachedOrder ToCachedOrder(Order order)
    {
        return new CachedOrder(
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.Status,
            order.CreatedAt);
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

public sealed partial class OrderEndpointLog
{
    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "Stored order {OrderId} and queued outbox event {EventId} from instance {InstanceId} with correlation {CorrelationId}")]
    public static partial void OrderAccepted(ILogger logger, Guid orderId, Guid eventId, string instanceId, string correlationId);
}
