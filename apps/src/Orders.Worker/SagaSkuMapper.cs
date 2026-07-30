namespace Orders.Worker;

/// <summary>
/// Milestone 43: the orchestrated saga demo predates real line items -
/// Order (Milestone 7) is still amount-only, unchanged, and Orders.Api's
/// contract/Avro schema stay untouched by this milestone on purpose (a
/// real line-item rewrite would be a much larger, riskier change than
/// "add a Reserve/Commit/Release step to the existing demo saga", and
/// isn't what this milestone is actually testing). To exercise the new
/// Inventory steps against real seeded stock, each order is deterministically
/// mapped to one of Catalog/Inventory's seeded SKUs by hashing its OrderId -
/// a stand-in for "which product this order is for," clearly a
/// simplification and not a real line-item lookup.
/// </summary>
public static class SagaSkuMapper
{
    private static readonly string[] SeededSkus =
    [
        "SKU-ELEC-001", "SKU-ELEC-002", "SKU-ELEC-003",
        "SKU-BOOK-001", "SKU-BOOK-002",
        "SKU-CLTH-001", "SKU-CLTH-002",
        "SKU-HOME-001", "SKU-HOME-002"
    ];

    public static (string Sku, int Quantity) MapOrderToLineItem(Guid orderId)
    {
        var index = (int)((uint)orderId.GetHashCode() % SeededSkus.Length);
        return (SeededSkus[index], 1);
    }
}
