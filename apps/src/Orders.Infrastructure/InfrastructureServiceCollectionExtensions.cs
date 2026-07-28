using BuildingBlocks;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orders.Application.Ports;
using Orders.Infrastructure.Caching;
using Orders.Infrastructure.Data;
using Orders.Infrastructure.Messaging;

namespace Orders.Infrastructure;

public static class InfrastructureServiceCollectionExtensions
{
    public static IServiceCollection AddOrdersInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration,
        string connectionString,
        string instanceId)
    {
        services.AddOptions<CacheOptions>()
            .Bind(configuration.GetSection(CacheOptions.SectionName))
            .Validate(options => options.TimeToLiveSeconds > 0, "Cache time-to-live must be positive.")
            .Validate(options => options.LockTimeoutMilliseconds > 0, "Cache lock timeout must be positive.")
            .Validate(options => options.LockRetryAttempts >= 0, "Cache lock retry attempts must not be negative.")
            .Validate(options => options.LockRetryDelayMilliseconds > 0, "Cache lock retry delay must be positive.")
            .ValidateOnStart();
        services.AddOptions<OutboxOptions>()
            .Bind(configuration.GetSection(OutboxOptions.SectionName))
            .Validate(options => options.BatchSize is > 0 and <= 100, "Outbox batch size must be between 1 and 100.")
            .Validate(options => options.PollIntervalMilliseconds >= 100, "Outbox poll interval must be at least 100 milliseconds.")
            .Validate(options => options.MaximumRetryDelaySeconds > 0, "Outbox maximum retry delay must be positive.")
            .ValidateOnStart();

        services.AddDbContext<OrdersDbContext>(options => options.UseNpgsql(connectionString));
        services.AddOrdersSchemaRegistry(configuration);

        services.AddSingleton<IProducer<string, byte[]>>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                ClientId = $"{options.ClientId}-{instanceId}",
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageTimeoutMs = 10_000,
                SocketTimeoutMs = 10_000
            };

            return new ProducerBuilder<string, byte[]>(config).Build();
        });
        services.AddSingleton<IAdminClient>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var config = new AdminClientConfig { BootstrapServers = options.BootstrapServers };
            return new AdminClientBuilder(config).Build();
        });

        services.AddOrdersResilience();
        services.AddOrdersRedis(configuration);

        services.AddScoped<IOrderRepository, Persistence.EfOrderRepository>();
        services.AddScoped<IOrderSummaryRepository, Persistence.EfOrderSummaryRepository>();
        services.AddScoped<IOrderEventStoreRepository, Persistence.EfOrderEventStoreRepository>();
        services.AddSingleton<IOrderCache, RedisOrderCache>();
        services.AddSingleton<IOrderEventPublisher, KafkaOrderEventPublisher>();
        services.AddHostedService<OutboxPublisher>();

        return services;
    }
}
