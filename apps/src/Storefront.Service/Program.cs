using BuildingBlocks;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Storefront.Service;

var builder = WebApplication.CreateBuilder(args);
var instanceId = builder.Configuration["InstanceId"] ?? Environment.MachineName;

builder.Logging.ClearProviders();
builder.Logging.AddJsonConsole(options =>
{
    options.IncludeScopes = true;
    options.UseUtcTimestamp = true;
});
builder.Logging.AddOrdersOpenTelemetryLogging("storefront-service", instanceId, builder.Environment.EnvironmentName);

builder.Services.AddOrdersObservability("storefront-service", instanceId, builder.Environment.EnvironmentName);
builder.Services.AddProblemDetails();

builder.Services.AddOptions<CatalogProxyOptions>()
    .Bind(builder.Configuration.GetSection(CatalogProxyOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BaseUrl), "Catalog base URL is required.")
    .ValidateOnStart();
builder.Services.AddOptions<CartProxyOptions>()
    .Bind(builder.Configuration.GetSection(CartProxyOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BaseUrl), "Cart base URL is required.")
    .ValidateOnStart();
builder.Services.AddOptions<OrdersProxyOptions>()
    .Bind(builder.Configuration.GetSection(OrdersProxyOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.BaseUrl), "Orders base URL is required.")
    .ValidateOnStart();
builder.Services.AddOptions<KeycloakOptions>()
    .Bind(builder.Configuration.GetSection(KeycloakOptions.SectionName))
    .Validate(options => !string.IsNullOrWhiteSpace(options.TokenUrl), "Keycloak token URL is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ClientId), "Keycloak client id is required.")
    .Validate(options => !string.IsNullOrWhiteSpace(options.ClientSecret), "Keycloak client secret is required.")
    .ValidateOnStart();

builder.Services.AddHttpClient("catalog", (serviceProvider, client) =>
{
    client.BaseAddress = new Uri(serviceProvider.GetRequiredService<IOptions<CatalogProxyOptions>>().Value.BaseUrl);
    client.Timeout = TimeSpan.FromSeconds(5);
}).AddStandardResilienceHandler();

builder.Services.AddHttpClient("cart", (serviceProvider, client) =>
{
    client.BaseAddress = new Uri(serviceProvider.GetRequiredService<IOptions<CartProxyOptions>>().Value.BaseUrl);
    client.Timeout = TimeSpan.FromSeconds(5);
}).AddStandardResilienceHandler();

builder.Services.AddHttpClient("orders", (serviceProvider, client) =>
{
    client.BaseAddress = new Uri(serviceProvider.GetRequiredService<IOptions<OrdersProxyOptions>>().Value.BaseUrl);
    client.Timeout = TimeSpan.FromSeconds(5);
}).AddStandardResilienceHandler();

builder.Services.AddHttpClient<KeycloakTokenProvider>().AddStandardResilienceHandler();

builder.Services.AddHealthChecks();

var app = builder.Build();

app.UseExceptionHandler();

app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = _ => false });

app.MapProxyEndpoints();

app.UseDefaultFiles();
app.UseStaticFiles();
app.MapFallbackToFile("index.html");

await app.RunAsync();
