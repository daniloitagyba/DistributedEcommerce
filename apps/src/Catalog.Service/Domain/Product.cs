namespace Catalog.Service.Domain;

/// <summary>
/// Milestone 40: the reason this catalog is MongoDB-backed rather than
/// another table in the existing Postgres database - product attributes
/// are genuinely heterogeneous per category (a t-shirt has size/color, a
/// laptop has RAM/CPU, a book has an ISBN/author). Modeling that in a
/// relational schema means EAV, a JSONB column, or one table per category -
/// all worse fits than a document whose shape simply varies by Category.
/// Attributes stays a flat string/string map rather than nested BSON to
/// keep the HTTP API surface simple - real catalogs often go further
/// (typed attribute schemas per category), deliberately not attempted here.
///
/// Milestone 61: the Id-as-ObjectId wire representation used to be declared
/// here via [BsonId]/[BsonRepresentation] - moved to a BsonClassMap in
/// Catalog.Service.Data (see MongoClassMaps.Register) so this type carries
/// no MongoDB.Bson dependency at all, matching the same domain-purity rule
/// Orders.Domain has enforced since Milestone 60.
/// </summary>
public sealed class Product
{
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Description { get; set; } = string.Empty;

    public string CategorySlug { get; set; } = string.Empty;

    public decimal Price { get; set; }

    public string Currency { get; set; } = "BRL";

    public string Sku { get; set; } = string.Empty;

    public Dictionary<string, string> Attributes { get; set; } = [];

    public IReadOnlyList<string> Images { get; set; } = [];

    public DateTimeOffset CreatedAt { get; set; }
}
