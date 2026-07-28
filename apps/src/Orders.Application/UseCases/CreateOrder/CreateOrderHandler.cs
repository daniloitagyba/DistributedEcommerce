using System.Diagnostics;
using System.Text.Json;
using BuildingBlocks;
using Microsoft.Extensions.Logging;
using Orders.Application.Ports;
using Orders.Domain;

namespace Orders.Application.UseCases.CreateOrder;

public sealed class CreateOrderHandler(IOrderRepository repository, ILogger<CreateOrderHandler> logger)
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public async Task<CreateOrderResult> HandleAsync(CreateOrderCommand command, CancellationToken cancellationToken)
    {
        var errors = CreateOrderCommandValidator.Validate(command);
        if (errors.Count > 0)
        {
            return new CreateOrderResult(null, Guid.Empty, errors);
        }

        var customerId = CreateOrderCommandValidator.NormalizeCustomerId(command.CustomerId!);
        var currency = CreateOrderCommandValidator.NormalizeCurrency(command.Currency!);
        var createdAt = DateTimeOffset.UtcNow;
        var order = Order.Create(customerId, command.Amount, currency, createdAt);

        var orderCreated = new OrderCreated(
            Guid.NewGuid(),
            order.Id,
            order.CustomerId,
            order.Amount,
            order.Currency,
            order.CreatedAt,
            command.CorrelationId);
        var outboxMessage = OutboxMessage.Create(
            orderCreated.EventId,
            nameof(OrderCreated),
            JsonSerializer.Serialize(orderCreated, SerializerOptions),
            orderCreated.OccurredAt,
            command.CorrelationId,
            Activity.Current?.Id,
            Activity.Current?.TraceStateString);

        Activity.Current?.SetTag("order.id", order.Id);
        Activity.Current?.SetTag("order.currency", order.Currency);
        Activity.Current?.SetTag("messaging.message.id", orderCreated.EventId);
        Activity.Current?.SetTag("service.instance.id", command.InstanceId);

        await repository.AddAsync(order, outboxMessage, cancellationToken);

        OrdersTelemetry.RecordCreated(order.Currency);
        CreateOrderLog.OrderAccepted(
            logger,
            order.Id,
            orderCreated.EventId,
            command.InstanceId,
            command.CorrelationId);

        return new CreateOrderResult(order, orderCreated.EventId, errors);
    }
}

public sealed partial class CreateOrderLog
{
    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "Stored order {OrderId} and queued outbox event {EventId} from instance {InstanceId} with correlation {CorrelationId}")]
    public static partial void OrderAccepted(ILogger logger, Guid orderId, Guid eventId, string instanceId, string correlationId);
}
