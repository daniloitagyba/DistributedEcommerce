using Avro.Generic;
using BuildingBlocks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Payments.Service;
using Payments.Service.Data;
using Testcontainers.PostgreSql;

namespace Orders.IntegrationTests;

public sealed class PaymentMessageProcessorTests : IAsyncLifetime, IDisposable
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("payments_test")
        .WithUsername("test_user")
        .WithPassword("test-password-not-a-secret")
        .Build();

    // Confluent.SchemaRegistry 2.15.0 ships no mock/in-memory client (unlike
    // Testcontainers for Postgres/Redis above), and ISchemaRegistryClient has
    // 24+ members - hand-rolling a fake risks subtly wrong behavior around
    // schema IDs. This lab runs on a single host where the real Karapace
    // instance (Milestone 19) is always up on the Compose network, so this
    // points at it directly rather than faking it.
    private readonly CachedSchemaRegistryClient _schemaRegistryClient =
        new(new SchemaRegistryConfig { Url = "http://172.30.0.16:8081" });

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

    public void Dispose()
    {
        _schemaRegistryClient.Dispose();
    }

    [Theory]
    [InlineData(49.90, true)]
    [InlineData(5000.00, false)]
    public async Task ProcessAsyncDecidesBasedOnAmountThreshold(decimal amount, bool expectedApproved)
    {
        var processor = CreateProcessor(declineAmountThreshold: 1_000m);
        var consumeResult = await CreateConsumeResultAsync(Guid.NewGuid(), Guid.NewGuid(), amount);

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

        var firstResult = await processor.ProcessAsync(await CreateConsumeResultAsync(eventId, orderId, 49.90m), CancellationToken.None);
        var secondResult = await processor.ProcessAsync(await CreateConsumeResultAsync(eventId, orderId, 49.90m), CancellationToken.None);

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
            _schemaRegistryClient,
            kafkaOptions,
            decisionOptions,
            NullLogger<PaymentMessageProcessor>.Instance);
    }

    private async Task<ConsumeResult<string, byte[]>> CreateConsumeResultAsync(Guid eventId, Guid orderId, decimal amount)
    {
        var orderCreated = new OrderCreated(
            eventId,
            orderId,
            "integration-customer",
            amount,
            "BRL",
            DateTimeOffset.UtcNow,
            "integration-correlation");

        var record = OrderCreatedAvroSchema.ToGenericRecord(orderCreated);
        var serializer = new AvroSerializer<GenericRecord>(_schemaRegistryClient, new AvroSerializerConfig { AutoRegisterSchemas = true });
        var context = new SerializationContext(MessageComponentType.Value, "orders.created.v1");
        var value = await serializer.SerializeAsync(record, context);

        return new ConsumeResult<string, byte[]>
        {
            Topic = "orders.created.v1",
            Partition = new Partition(0),
            Offset = new Offset(0),
            Message = new Message<string, byte[]>
            {
                Key = orderId.ToString("N"),
                Value = value,
                Headers = new Headers()
            }
        };
    }
}
