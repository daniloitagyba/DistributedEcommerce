using System.Diagnostics;
using BuildingBlocks;

namespace Orders.Api.Middleware;

public sealed class CorrelationIdMiddleware(RequestDelegate next, ILogger<CorrelationIdMiddleware> logger)
{
    public const string HeaderName = "X-Correlation-ID";

    private const int MaximumLength = 128;

    public async Task InvokeAsync(HttpContext context)
    {
        var correlationId = GetInboundCorrelationId(context)
            ?? Activity.Current?.TraceId.ToString()
            ?? Guid.NewGuid().ToString("N");

        context.Items[MessagingHeaders.CorrelationId] = correlationId;
        context.Response.Headers[HeaderName] = correlationId;
        Activity.Current?.SetTag("correlation.id", correlationId);

        using var scope = logger.BeginScope(new Dictionary<string, object?>
        {
            ["CorrelationId"] = correlationId
        });

        await next(context);
    }

    private static string? GetInboundCorrelationId(HttpContext context)
    {
        var value = context.Request.Headers[HeaderName].FirstOrDefault();

        return !string.IsNullOrWhiteSpace(value) && value.Length <= MaximumLength
            ? value
            : null;
    }
}

public static class CorrelationContext
{
    public static string GetCorrelationId(this HttpContext context)
    {
        return context.Items[MessagingHeaders.CorrelationId]?.ToString() ?? Guid.NewGuid().ToString("N");
    }
}
