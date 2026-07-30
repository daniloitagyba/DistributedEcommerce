using Inventory.Service.Data;
using Microsoft.EntityFrameworkCore;

namespace Inventory.Service.Endpoints;

public static class InventoryEndpoints
{
    public static IEndpointRouteBuilder MapInventoryEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/inventory/{sku}", GetBySkuAsync).WithTags("Inventory");
        endpoints.MapGet("/inventory", ListAsync).WithTags("Inventory");

        return endpoints;
    }

    private static async Task<IResult> GetBySkuAsync(
        string sku,
        InventoryDbContext dbContext,
        CancellationToken cancellationToken)
    {
        var item = await dbContext.InventoryItems.AsNoTracking().FirstOrDefaultAsync(i => i.Sku == sku, cancellationToken);
        return item is null
            ? Results.NotFound()
            : Results.Ok(new { item.Sku, item.AvailableQuantity, item.ReservedQuantity, item.UpdatedAt });
    }

    private static async Task<IResult> ListAsync(
        InventoryDbContext dbContext,
        CancellationToken cancellationToken)
    {
        var items = await dbContext.InventoryItems
            .AsNoTracking()
            .OrderBy(i => i.Sku)
            .Select(i => new { i.Sku, i.AvailableQuantity, i.ReservedQuantity, i.UpdatedAt })
            .ToListAsync(cancellationToken);

        return Results.Ok(items);
    }
}
