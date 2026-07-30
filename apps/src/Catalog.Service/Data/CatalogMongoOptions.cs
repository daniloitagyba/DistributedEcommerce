namespace Catalog.Service.Data;

public sealed class CatalogMongoOptions
{
    public const string SectionName = "Mongo";

    public string ConnectionString { get; init; } = "mongodb://localhost:27017";

    public string DatabaseName { get; init; } = "catalog";
}
