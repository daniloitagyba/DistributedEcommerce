using System.Text.Json;
using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Payments.Service;
using Payments.Service.Data;
using Testcontainers.PostgreSql;

namespace Orders.IntegrationTests;

public sealed class PaymentMessageProcessorTests : IAsyncLifetime
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("payments_test")
        .WithUsername("test_user")
        .WithPassword("test-password-not-a-secret")
        .Build();

    private ServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        await _postgres.StartAsync();

        var services = new ServiceCollection();
        services.AddDbContext<PaymentsDbContext>(options => options.UseNpgsql(_postgres.GetConnectionString()));
        _serviceProvider = services.BuildServiceProvider();

        await using var scope = _serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
        await dbContext.Database.MigrateAsync();
    }

    public async Task DisposeAsync()
    {
        await _serviceProvider.DisposeAsync();
        await _postgres.DisposeAsync();
    }

    [Theory]
    [InlineData(49.90, true)]
    [InlineData(5000.00, false)]
    public async Task ProcessAsyncDecidesBasedOnAmountThreshold(decimal amount, bool expectedApproved)
    {
        var processor = CreateProcessor(declineAmountThreshold: 1_000m);
        var consumeResult = CreateConsumeResult(Guid.NewGuid(), Guid.NewGuid(), amount);

        var result = await processor.ProcessAsync(consumeResult, CancellationToken.None);

        Assert.Equal(MessageProcessingResult.Processed, result);

        await using var scope = _serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
        var payment = await dbContext.Payments.SingleAsync();
        var outboxMessage = await dbContext.OutboxMessages.SingleAsync();

        Assert.Equal(expectedApproved, payment.Approved);
        Assert.Equal(nameof(PaymentDecided), outboxMessage.EventType);
    }

    [Fact]
    public async Task ProcessAsyncSkipsDuplicateEvents()
    {
        var processor = CreateProcessor(declineAmountThreshold: 1_000m);
        var eventId = Guid.NewGuid();
        var orderId = Guid.NewGuid();

        var firstResult = await processor.ProcessAsync(CreateConsumeResult(eventId, orderId, 49.90m), CancellationToken.None);
        var secondResult = await processor.ProcessAsync(CreateConsumeResult(eventId, orderId, 49.90m), CancellationToken.None);

        Assert.Equal(MessageProcessingResult.Processed, firstResult);
        Assert.Equal(MessageProcessingResult.Duplicate, secondResult);

        await using var scope = _serviceProvider.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<PaymentsDbContext>();
        Assert.Equal(1, await dbContext.Payments.CountAsync());
    }

    private PaymentMessageProcessor CreateProcessor(decimal declineAmountThreshold)
    {
        var kafkaOptions = Options.Create(new PaymentsKafkaOptions());
        var decisionOptions = Options.Create(new PaymentDecisionOptions { DeclineAmountThreshold = declineAmountThreshold });
        return new PaymentMessageProcessor(
            _serviceProvider.GetRequiredService<IServiceScopeFactory>(),
            kafkaOptions,
            decisionOptions,
            NullLogger<PaymentMessageProcessor>.Instance);
    }

    private static ConsumeResult<string, string> CreateConsumeResult(Guid eventId, Guid orderId, decimal amount)
    {
        var orderCreated = new OrderCreated(
            eventId,
            orderId,
            "integration-customer",
            amount,
            "BRL",
            DateTimeOffset.UtcNow,
            "integration-correlation");

        return new ConsumeResult<string, string>
        {
            Topic = "orders.created.v1",
            Partition = new Partition(0),
            Offset = new Offset(0),
            Message = new Message<string, string>
            {
                Key = orderId.ToString("N"),
                Value = JsonSerializer.Serialize(orderCreated, SerializerOptions),
                Headers = new Headers()
            }
        };
    }
}
