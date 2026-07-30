using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Npgsql;
using Orders.Worker;

var builder = WebApplication.CreateBuilder(args);
var instanceId = builder.Configuration["InstanceId"] ?? Environment.MachineName;

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options =>
{
    options.IncludeScopes = true;
    options.UseUtcTimestamp = true;
});
builder.Logging.AddOrdersOpenTelemetryLogging("orders-worker", instanceId, builder.Environment.EnvironmentName);

builder.Services.AddOrdersObservability("orders-worker", instanceId, builder.Environment.EnvironmentName);

builder.Services.AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka bootstrap servers are required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.OrderCreatedTopic), "Kafka topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeadLetterTopic), "Kafka dead-letter topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka consumer group is required.")
    .ValidateOnStart();
builder.Services.AddOptions<MessageProcessingOptions>()
    .Bind(builder.Configuration.GetSection(MessageProcessingOptions.SectionName))
    .Validate(options => options.MaximumAttempts is > 0 and <= 10, "Maximum attempts must be between 1 and 10.")
    .Validate(options => options.InitialRetryDelayMilliseconds > 0, "Initial retry delay must be positive.")
    .Validate(options => options.MaximumRetryDelayMilliseconds >= options.InitialRetryDelayMilliseconds, "Maximum retry delay must not be less than the initial delay.")
    .Validate(options => options.InfrastructureRetryDelayMilliseconds > 0, "Infrastructure retry delay must be positive.")
    .ValidateOnStart();
builder.Services.AddOptions<PaymentResultKafkaOptions>()
    .Bind(builder.Configuration.GetSection(PaymentResultKafkaOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka bootstrap servers are required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.PaymentResultTopic), "Payment result topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeadLetterTopic), "Kafka dead-letter topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka consumer group is required.")
    .ValidateOnStart();
builder.Services.AddOptions<OrderProjectionOptions>()
    .Bind(builder.Configuration.GetSection(OrderProjectionOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka bootstrap servers are required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.OrderCreatedTopic), "Order-created topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.PaymentResultTopic), "Payment result topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DeadLetterTopic), "Kafka dead-letter topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka consumer group is required.")
    .ValidateOnStart();
builder.Services.AddOptions<SagaOrchestrationOptions>()
    .Bind(builder.Configuration.GetSection(SagaOrchestrationOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka bootstrap servers are required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.OrderCreatedTopic), "Order-created topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DecisionRequestedTopic), "Decision-requested topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.DecisionRepliedTopic), "Decision-replied topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ReservationRequestedTopic), "Reservation-requested topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ReservationRepliedTopic), "Reservation-replied topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.CommitRequestedTopic), "Commit-requested topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.CommitRepliedTopic), "Commit-replied topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ReleaseRequestedTopic), "Release-requested topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ReleaseRepliedTopic), "Release-replied topic is required.")
    .Validate(options => options.TimeoutSeconds > 0, "Saga timeout must be positive.")
    .ValidateOnStart();
builder.Services.AddOptions<OrderEventStoreOptions>()
    .Bind(builder.Configuration.GetSection(OrderEventStoreOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BootstrapServers), "Kafka bootstrap servers are required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.OrderCreatedTopic), "Order-created topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.PaymentResultTopic), "Payment result topic is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ConsumerGroup), "Kafka consumer group is required.")
    .ValidateOnStart();
builder.Services.AddOptions<LeaderElectionOptions>()
    .Bind(builder.Configuration.GetSection(LeaderElectionOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.Namespace), "Leader election namespace is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.LeaseName), "Leader election lease name is required.")
    .Validate(options => options.LeaseDurationSeconds > options.RenewDeadlineSeconds, "Lease duration must exceed the renew deadline.")
    .ValidateOnStart();

var connectionString = builder.Configuration.GetConnectionString("Orders")
    ?? throw new InvalidOperationException("Connection string 'Orders' is required.");
builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));
builder.Services.AddOrdersResilience();
builder.Services.AddOrdersSchemaRegistry(builder.Configuration);
builder.Services.AddSingleton<InboxStore>();
builder.Services.AddSingleton<OrderStatusStore>();
builder.Services.AddSingleton<OrderMessageProcessor>();
builder.Services.AddSingleton<PaymentResultProcessor>();
builder.Services.AddSingleton<OrderProjectionStore>();
builder.Services.AddSingleton<OrderProjectionProcessor>();
builder.Services.AddOrdersRedis(builder.Configuration);
builder.Services.AddSingleton<IOrderCacheInvalidator, RedisOrderCacheInvalidator>();

builder.Services.AddSingleton<IProducer<string, string>>(serviceProvider =>
{
    var options = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = options.BootstrapServers,
        ClientId = $"{options.ClientId}-dlq",
        Acks = Acks.All,
        EnableIdempotence = true,
        MessageTimeoutMs = 10_000,
        SocketTimeoutMs = 10_000
    };

    return new ProducerBuilder<string, string>(config).Build();
});
builder.Services.AddSingleton<IAdminClient>(serviceProvider =>
{
    var options = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
    var config = new AdminClientConfig { BootstrapServers = options.BootstrapServers };
    return new AdminClientBuilder(config).Build();
});
builder.Services.AddSingleton<IDeadLetterPublisher, KafkaDeadLetterPublisher>();
builder.Services.AddSingleton<IPaymentResultDeadLetterPublisher, PaymentResultDeadLetterPublisher>();
builder.Services.AddSingleton<IOrderProjectionDeadLetterPublisher, OrderProjectionDeadLetterPublisher>();
builder.Services.AddHostedService<OrderCreatedConsumer>();
builder.Services.AddHostedService<PaymentResultConsumer>();
builder.Services.AddHostedService<OrderProjectionConsumer>();

builder.Services.AddSingleton<SagaOrchestrationStore>();
builder.Services.AddHostedService<OrderSagaOrchestrator>();
builder.Services.AddHostedService<OrderSagaReplyConsumer>();

builder.Services.AddSingleton<LeaderElectionService>();
builder.Services.AddHostedService(serviceProvider => serviceProvider.GetRequiredService<LeaderElectionService>());
builder.Services.AddHostedService<SagaTimeoutSweeper>();

builder.Services.AddSingleton<OrderEventStoreAppender>();
builder.Services.AddHostedService<OrderEventStoreProjector>();
builder.Services.AddHealthChecks()
    .AddCheck<KafkaHealthCheck>("kafka", tags: ["ready"])
    .AddCheck<PostgresHealthCheck>("postgres", tags: ["ready"])
    .AddCheck<RedisHealthCheck>("redis", tags: ["ready"]);

var app = builder.Build();

app.MapHealthChecks("/health/live", new HealthCheckOptions
{
    Predicate = _ => false
});
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = registration => registration.Tags.Contains("ready")
});
app.MapGet("/", () => Results.Ok(new { service = "Orders.Worker", instanceId }));

await app.RunAsync();
