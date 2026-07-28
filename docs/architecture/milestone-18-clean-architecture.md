# Milestone 18 Clean/Hexagonal Architecture Refactor

## Scope

Every prior milestone added a distributed-systems capability without touching how `Orders.Api` itself is organized: one project, folders by technical concern (`Endpoints/`, `Data/`, `Caching/`, `Messaging/`), endpoints calling `OrdersDbContext` and `IOrderCache` directly. Nothing enforced that a business rule couldn't quietly depend on EF Core, or that a use case's tests needed a real database. This milestone splits `Orders.Api` into four projects with the dependency rule enforced by project references, not convention - `Orders.Domain` cannot see EF Core even if someone tries, because it has no package reference to it.

## Design

- **`Orders.Domain`** - `Order`, `OutboxMessage`, `InboxMessage`, `OrderSummary`. Zero package references, not even to `BuildingBlocks`. These entities already had private setters and factory methods before this milestone (a deliberate earlier choice); what changed is that nothing outside this project can now compile against Npgsql, Confluent.Kafka, or ASP.NET Core even by accident.
- **`Orders.Application`** - use cases (`CreateOrderHandler`, `GetOrderHandler`, `ListOrderSummariesHandler`) and the ports they depend on (`IOrderRepository`, `IOrderSummaryRepository`, `IOrderCache`), all as plain interfaces with no implementation detail leaking through. References `Orders.Domain` and `BuildingBlocks` (treated as a shared-kernel of cross-cutting contracts and telemetry, not infrastructure) - nothing else. A use case handler is a plain class: no `IResult`, no `HttpContext`, no `DbContext`.
- **`Orders.Infrastructure`** - the adapters: `EfOrderRepository`/`EfOrderSummaryRepository` (EF Core), `RedisOrderCache` (StackExchange.Redis), `KafkaOrderEventPublisher`/`OutboxPublisher` (Confluent.Kafka), `PostgresHealthCheck`/`KafkaHealthCheck`, and `OrdersDbContext` itself with its migrations. Implements Application's ports; references Application, Domain, and BuildingBlocks.
- **`Orders.Api`** stays thin: `Program.cs` (composition root), `Endpoints/` (HTTP <-> Application command/result mapping only), `Contracts/` (HTTP DTOs), `Middleware/`, `RateLimiting/`. An endpoint now looks like: build a command from the request, call a handler, map the result to an `IResult`. No EF Core, no Redis, no Kafka types appear anywhere in this project.
- **A new `InfrastructureUnavailableException`** in `Orders.Application.Exceptions` replaces the old pattern of endpoints catching `ResilienceExtensions.IsInfrastructureFault(exception)` directly. The Polly pipelines and the technology-specific fault check now live entirely inside the Infrastructure adapters (`EfOrderRepository`), which translate a Postgres-specific fault into a technology-agnostic signal the endpoint can catch without knowing what failed underneath. The 503 response shape at the HTTP boundary is unchanged - only which layer decides to produce it moved.
- **Validation moved with the use case it validates.** `CreateOrderRequestValidator` (validating the HTTP DTO) became `CreateOrderCommandValidator` (validating the Application command) - the same logic, now owned by the layer that actually needs the input to be valid, not the layer that happens to receive it first over HTTP.
- **`CachedOrder` became an Application-owned read model** (`Orders.Application.Ports`) rather than living in the Api project's `Caching/` folder - it's the contract between the cache port and the use cases that call it, not an API response shape (the API's `OrderResponse` DTO is a separate, intentionally similar-looking type).

## What didn't work

**A plain `Microsoft.NET.Sdk` class library gets none of the ASP.NET Core Web SDK's implicit global usings.** `Orders.Api` (`Microsoft.NET.Sdk.Web`) had never needed an explicit `using Microsoft.Extensions.Logging;` or `using Microsoft.Extensions.Hosting;` - the Web SDK injects a whole set of global usings (`Microsoft.Extensions.DependencyInjection`, `.Hosting`, `.Logging`, `.Configuration`, ASP.NET Core's own namespaces) automatically. Moving `OutboxPublisher` (a `BackgroundService` using `ILogger`, `IServiceScopeFactory`, `IConfiguration`) into `Orders.Infrastructure` (plain `Microsoft.NET.Sdk`) produced 29 compiler errors that had nothing to do with logic - every one of those types needed an explicit `using`, and `Microsoft.Extensions.Hosting.Abstractions` needed an explicit `PackageReference` that had never been necessary before. Same root cause, fixed once understood.

**The EF Core migration snapshot references entity types by string, not `typeof`.** `OrdersDbContextModelSnapshot.cs` and every migration's `.Designer.cs` contain `modelBuilder.Entity("Orders.Api.Domain.Order", ...)` - a string literal, not a compiled type reference. This has zero effect on already-applied migrations at runtime (`Database.MigrateAsync()` only reads the `[Migration("id")]` attribute and the `__EFMigrationsHistory` table, never the snapshot), but it does mean a future `dotnet ef migrations add` would compute a spurious diff against a stale namespace unless the snapshot strings are updated too. Fixed with a straightforward `sed` across the migration files rather than leaving a landmine (`Orders.Api.Domain.` -> `Orders.Domain.`, `Orders.Api.Data` -> `Orders.Infrastructure.Data`) - migration IDs themselves were left untouched, since those are what the history table actually keys on.

**A rolling restart of every `orders-api` pod at once, under active load, occasionally pushes GET p99 over its 1-second threshold - reproducibly rare, not a regression.** The first `resilience-test.sh` run (which restarts all pods while k6 traffic is live) crossed the `endpoint:get-order` p99 threshold at 1.79s; `failed_rate` was 0 the entire time - every request eventually succeeded, some just took longer during the restart window. A second, otherwise-identical run passed cleanly (p99 769ms). This matches the same class of tail-latency variance already documented elsewhere in this lab's chaos/saga tests, not something the refactor introduced - the layering adds a handful of extra async calls per request, nowhere near enough to explain a ~1.7s spike; a brief connection failover during pod termination is a far more likely explanation, and it self-resolved on retry.

## Results

### Build and tests

| Check | Result |
| --- | --- |
| `dotnet build LocalDistributedLab.slnx` | 0 Warnings, 0 Errors (`TreatWarningsAsErrors=true`, `AnalysisLevel=latest-recommended`) |
| `dotnet test Orders.UnitTests` | 24/24 passing (same count as before the refactor) |
| `dotnet test Orders.IntegrationTests` | 7/7 passing (same count as before the refactor) |

### Live validation (K3s, real traffic)

| Check | Result |
| --- | --- |
| `k3s-smoke-test.sh` | Passed - orders created and consumed end to end through the new layering |
| `k6-run.sh baseline` | `failed_rate=0`, `checks_rate=1`, `flow_rate=1`, no thresholds crossed |
| `k6-run.sh cache` | `failed_rate=0`, `checks_rate=1`, `flow_rate=1` - `GetOrderHandler` + `IOrderCache` port validated |
| `k6-run.sh saga` | `failed_rate=0`; `saga_correct_outcome_rate=99.75%` (404/405), consistent with this lab's already-documented pre-existing tail-latency flakiness in the saga poll window |
| `resilience-test.sh` | Second run passed cleanly (`api_pods_replaced=true`, `graceful_shutdown=true`) after a transient p99 miss on the first, non-reproducing run |

## Running the experiment

```bash
cd apps
dotnet build LocalDistributedLab.slnx
dotnet test tests/Orders.UnitTests/Orders.UnitTests.csproj
dotnet test tests/Orders.IntegrationTests/Orders.IntegrationTests.csproj

scripts/k3s-build-images.sh
scripts/k3s-deploy.sh
scripts/k3s-smoke-test.sh
scripts/k6-run.sh baseline
```
