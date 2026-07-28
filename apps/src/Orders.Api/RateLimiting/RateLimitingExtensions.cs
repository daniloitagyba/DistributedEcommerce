using System.Globalization;
using System.Threading.RateLimiting;
using BuildingBlocks;
using Microsoft.AspNetCore.RateLimiting;

namespace Orders.Api.RateLimiting;

public static class RateLimitingExtensions
{
    public const string OrdersPolicy = "orders";

    public static IServiceCollection AddOrdersRateLimiting(this IServiceCollection services, IConfiguration configuration)
    {
        var rateLimitOptions = configuration.GetSection(RateLimitOptions.SectionName).Get<RateLimitOptions>()
            ?? new RateLimitOptions();

        if (rateLimitOptions.TokenLimit <= 0)
        {
            throw new InvalidOperationException("RateLimit token limit must be positive.");
        }

        if (rateLimitOptions.TokensPerPeriod <= 0)
        {
            throw new InvalidOperationException("RateLimit tokens-per-period must be positive.");
        }

        if (rateLimitOptions.ReplenishmentPeriodSeconds <= 0)
        {
            throw new InvalidOperationException("RateLimit replenishment period must be positive.");
        }

        if (rateLimitOptions.QueueLimit < 0)
        {
            throw new InvalidOperationException("RateLimit queue limit must not be negative.");
        }

        services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            options.AddTokenBucketLimiter(OrdersPolicy, bucket =>
            {
                bucket.TokenLimit = rateLimitOptions.TokenLimit;
                bucket.TokensPerPeriod = rateLimitOptions.TokensPerPeriod;
                bucket.ReplenishmentPeriod = TimeSpan.FromSeconds(rateLimitOptions.ReplenishmentPeriodSeconds);
                bucket.QueueLimit = rateLimitOptions.QueueLimit;
                bucket.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
                bucket.AutoReplenishment = true;
            });

            options.OnRejected = async (context, cancellationToken) =>
            {
                OrdersTelemetry.RecordRateLimited();

                var retryAfterSeconds = "1";
                if (context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var retryAfter))
                {
                    retryAfterSeconds = Math.Ceiling(retryAfter.TotalSeconds).ToString(CultureInfo.InvariantCulture);
                }

                context.HttpContext.Response.Headers["Retry-After"] = retryAfterSeconds;
                await context.HttpContext.Response.WriteAsJsonAsync(
                    new
                    {
                        title = "Too Many Requests",
                        status = StatusCodes.Status429TooManyRequests,
                        detail = "The orders API is shedding load; retry after the indicated delay."
                    },
                    cancellationToken);
            };
        });

        return services;
    }
}
