namespace Inventory.Service.Domain;

/// <summary>
/// Deliberately no optimistic concurrency token and no caller-side row lock.
/// TryReserve is a plain read-then-write mutation - safe only because
/// Inventory.Service's Kafka consumer guarantees at most one in-flight
/// request per Sku at a time (see InventoryContracts.cs). If two requests
/// for the same Sku were ever processed concurrently against the same
/// loaded instance, this would oversell; that is the exact scenario the
/// partitioning is there to make impossible.
/// </summary>
public sealed class InventoryItem
{
    private InventoryItem()
    {
    }

    public string Sku { get; private set; } = string.Empty;

    public int AvailableQuantity { get; private set; }

    public int ReservedQuantity { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }

    public static InventoryItem Create(string sku, int availableQuantity, DateTimeOffset now)
    {
        return new InventoryItem
        {
            Sku = sku,
            AvailableQuantity = availableQuantity,
            ReservedQuantity = 0,
            UpdatedAt = now
        };
    }

    public bool TryReserve(int quantity, DateTimeOffset now)
    {
        if (AvailableQuantity < quantity)
        {
            return false;
        }

        AvailableQuantity -= quantity;
        ReservedQuantity += quantity;
        UpdatedAt = now;
        return true;
    }

    /// <summary>
    /// Turns a temporary hold into a permanent deduction: the stock never
    /// returns to Available. This is the saga's "everything downstream
    /// succeeded" outcome for a reservation.
    /// </summary>
    public bool TryCommit(int quantity, DateTimeOffset now)
    {
        if (ReservedQuantity < quantity)
        {
            return false;
        }

        ReservedQuantity -= quantity;
        UpdatedAt = now;
        return true;
    }

    /// <summary>
    /// The saga's compensating transaction: gives held stock back to
    /// Available. Called when a step downstream of the original reservation
    /// (payment, in this lab) fails - the reservation itself was never the
    /// problem, so it gets undone rather than left dangling.
    /// </summary>
    public bool TryRelease(int quantity, DateTimeOffset now)
    {
        if (ReservedQuantity < quantity)
        {
            return false;
        }

        ReservedQuantity -= quantity;
        AvailableQuantity += quantity;
        UpdatedAt = now;
        return true;
    }
}
