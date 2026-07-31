using Catalog.Service.Domain;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Catalog.Service.Data;

public sealed class ProductRepository
{
    private readonly IMongoCollection<Product> _products;

    static ProductRepository()
    {
        // Must run before the driver serializes/deserializes a Product for
        // the first time - a static constructor guarantees that regardless
        // of whether the caller is Program.cs or a test fixture that never
        // runs application startup.
        MongoClassMaps.Register();
    }

    public ProductRepository(IMongoDatabase database)
    {
        _products = database.GetCollection<Product>("products");
    }

    public async Task<IReadOnlyList<Product>> ListAsync(
        string? categorySlug,
        int skip,
        int limit,
        CancellationToken cancellationToken)
    {
        var filter = string.IsNullOrWhiteSpace(categorySlug)
            ? Builders<Product>.Filter.Empty
            : Builders<Product>.Filter.Eq(product => product.CategorySlug, categorySlug);

        return await _products.Find(filter)
            .SortByDescending(product => product.CreatedAt)
            .Skip(skip)
            .Limit(limit)
            .ToListAsync(cancellationToken);
    }

    public async Task<long> CountAsync(string? categorySlug, CancellationToken cancellationToken)
    {
        var filter = string.IsNullOrWhiteSpace(categorySlug)
            ? Builders<Product>.Filter.Empty
            : Builders<Product>.Filter.Eq(product => product.CategorySlug, categorySlug);

        return await _products.CountDocumentsAsync(filter, cancellationToken: cancellationToken);
    }

    public async Task<Product?> FindByIdAsync(string id, CancellationToken cancellationToken)
    {
        if (!ObjectId.TryParse(id, out _))
        {
            return null;
        }

        return await _products.Find(product => product.Id == id).FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<Product?> FindBySkuAsync(string sku, CancellationToken cancellationToken)
    {
        return await _products.Find(product => product.Sku == sku).FirstOrDefaultAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<Product>> FindByIdsAsync(IReadOnlyCollection<string> ids, CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
        {
            return [];
        }

        var filter = Builders<Product>.Filter.In(product => product.Id, ids);
        return await _products.Find(filter).ToListAsync(cancellationToken);
    }

    public Task InsertAsync(Product product, CancellationToken cancellationToken)
    {
        return _products.InsertOneAsync(product, cancellationToken: cancellationToken);
    }

    public async Task EnsureIndexesAsync(CancellationToken cancellationToken)
    {
        var categoryIndex = new CreateIndexModel<Product>(
            Builders<Product>.IndexKeys.Ascending(product => product.CategorySlug));
        var skuIndex = new CreateIndexModel<Product>(
            Builders<Product>.IndexKeys.Ascending(product => product.Sku),
            new CreateIndexOptions { Unique = true });

        await _products.Indexes.CreateManyAsync([categoryIndex, skuIndex], cancellationToken);
    }
}
