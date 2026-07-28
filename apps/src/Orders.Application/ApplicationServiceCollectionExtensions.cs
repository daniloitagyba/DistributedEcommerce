using Microsoft.Extensions.DependencyInjection;
using Orders.Application.UseCases.CreateOrder;
using Orders.Application.UseCases.GetOrder;
using Orders.Application.UseCases.GetOrderHistory;
using Orders.Application.UseCases.ListOrderSummaries;

namespace Orders.Application;

public static class ApplicationServiceCollectionExtensions
{
    public static IServiceCollection AddOrdersApplication(this IServiceCollection services)
    {
        services.AddScoped<CreateOrderHandler>();
        services.AddScoped<GetOrderHandler>();
        services.AddScoped<ListOrderSummariesHandler>();
        services.AddScoped<GetOrderHistoryHandler>();
        return services;
    }
}
