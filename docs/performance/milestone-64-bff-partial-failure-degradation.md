# Milestone 64: BFF Partial-Failure Degradation

## Scope

`GET /api/storefront/products/{sku}` (Milestone 54's fan-out) calls Catalog and Inventory in parallel and `await Task.WhenAll(catalogTask, inventoryTask)`s both. `Task.WhenAll` propagates whichever task faults first - so if Inventory is down while Catalog is answering fine, the whole request fails, even though Catalog alone has everything needed to answer with a product name, price, and description. That's backwards for a BFF: the entire reason to fan out to several backends is that a caller reasonably wants *whatever the BFF can get*, not an all-or-nothing contract stricter than any single backend's own guarantee.

## Design

`GetProductSummaryAsync` now awaits Catalog and Inventory **separately**, each in its own `try`/`catch`, rather than via `Task.WhenAll`:

- **Catalog** is still load-bearing - it's the only source that can say whether the SKU exists at all. A hard failure there (`HttpRequestException`/`TaskCanceledException`, not a 404) now returns a clean `503` (`Results.Problem`) instead of an unhandled exception falling through to the generic exception handler.
- **Inventory** is treated as enrichment. A hard failure there no longer fails the request - the response comes back `200 OK` with `product` fully populated, `inventory: null`, and a new `degraded: true` flag the caller can act on (show a "stock unknown" badge, retry later, whatever's appropriate) instead of losing catalog data it already has.
- Both tasks are always awaited to completion regardless of which one faults first - important because the previous single `Task.WhenAll` guaranteed both were observed; splitting the await into two independent try/catch blocks had to preserve that, or a faulted-but-unawaited task risks being silently dropped.

## What didn't work

**`ILogger<StorefrontEndpoints>` doesn't compile - `StorefrontEndpoints` is a static class, and C# forbids static types as generic type arguments (`CS0718`).** Every other logging call in this codebase hangs off a concrete class name for the category; this is the first log line ever added to `Storefront.Service` (it had zero `ILogger` usage before this milestone), so there was no existing pattern to follow here. Fixed by injecting `ILoggerFactory` into the endpoint handler and calling `CreateLogger("Storefront.Service.StorefrontEndpoints")` directly instead.

**A shared `TryAsync(Task<object?> task, ...)` helper tripped `VSTHRD003` ("avoid awaiting a Task that wasn't started within your context").** The analyzer's heuristic treats any `Task` arriving as a method parameter as suspect, regardless of whether a `SynchronizationContext` is actually in play - and Kestrel/ASP.NET Core minimal APIs never install one, so the warning is a false positive here. Rather than suppress it, inlined the two try/catch blocks directly in `GetProductSummaryAsync` instead of factoring them into a shared helper - the analyzer doesn't flag awaiting a task that was created a few lines earlier in the *same* method, only one crossing a method boundary as a parameter. A few lines of duplication, zero analyzer suppressions.

## Results

Live proof against the deployed cluster, before and after simulating an Inventory outage (`kubectl set env deployment/storefront-service InventoryProxy__BaseUrl=http://inventory-service-unreachable.invalid`, reverted immediately after):

**Healthy path** (unchanged):
```
{"product":{...,"sku":"SKU-ELEC-001",...},"inventory":{"sku":"SKU-ELEC-001","availableQuantity":0,...},"degraded":false}
```

**Inventory unreachable** - `200 OK`, not a failure:
```
$ curl -s -w '\nHTTP_STATUS:%{http_code}\n' .../api/storefront/products/SKU-ELEC-001
{"product":{...,"sku":"SKU-ELEC-001",...},"inventory":null,"degraded":true}
HTTP_STATUS:200
```

Product data (name, price, description, images) served correctly with zero loss, `degraded: true` correctly signaling the enrichment that didn't make it. Reverted the env override; healthy path and the pre-existing 404-for-unknown-SKU path both confirmed unchanged afterward. Argo CD stayed `Synced`/`Healthy` throughout. Full solution (132 tests, 9 projects) passes.

## Running it

```bash
# Simulate an Inventory outage against a real deployment (reversible)
kubectl set env deployment/storefront-service InventoryProxy__BaseUrl=http://unreachable.invalid -n orders-lab
curl -s http://<storefront-service>/api/storefront/products/<sku>
kubectl set env deployment/storefront-service InventoryProxy__BaseUrl- -n orders-lab
```
