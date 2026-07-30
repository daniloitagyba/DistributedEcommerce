namespace Cart.Service.Data;

public sealed class CartOptions
{
    public const string SectionName = "Cart";

    /// <summary>
    /// Sliding TTL applied to the whole cart on every write. A cart with no
    /// activity for this long is abandoned and Redis reclaims it on its
    /// own - there is no separate cleanup job, because there is nothing to
    /// clean up: expiry IS the cart's deletion mechanism.
    /// </summary>
    public int TimeToLiveSeconds { get; init; } = 1_800;
}
