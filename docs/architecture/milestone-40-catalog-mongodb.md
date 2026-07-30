# Milestone 40: Catalog Service Backed by MongoDB

## Scope

The first milestone of the e-commerce expansion: a `Catalog.Service` that owns products and categories, backed by MongoDB rather than Postgres. This is a deliberate polyglot-persistence decision, not a "let's try Mongo" exercise - the goal is to validate the architecture against a genuinely different data-access pattern than the rest of the lab.

## Why MongoDB here, and not for bestsellers

Two different data problems came up while planning the e-commerce expansion, and they call for two different stores:

- **Product catalog attributes are genuinely heterogeneous per category.** A notebook has `ram`/`storage`/`cpu`; a book has `author`/`pages`/`isbn`; a t-shirt has `size`/`color`/`material`. In Postgres this is either an EAV table (query complexity, no schema safety), a JSONB column (workable, but then why use Postgres's relational strengths at all for this entity), or one table per category (an explosion of migrations for every new category). MongoDB's per-document schema is the natural fit: `Product.Attributes` is a free-form `Dictionary<string,string>`, and adding a new category with its own attribute shape is a data change, not a schema migration.
- **Bestseller rankings are not this milestone's job, and not Mongo's.** Ranking by sales count is an ordered, frequently-updated, top-N read pattern - exactly what Redis sorted sets (`ZINCRBY`, `ZREVRANGE`) are built for, and what Milestone 44 will use. Reaching for MongoDB aggregation pipelines for a leaderboard would be using a document store for a problem a data structure server already solves better in this lab (see Milestone 9's cache-aside and Milestone 38's sliding-window rate limiter for the established pattern of matching Redis's data structures to the problem, not just using it as a generic cache).

The result is intentional polyglot persistence: Postgres for transactional/ACID order and payment state, MongoDB for heterogeneous catalog documents, Redis for cache plus (starting at M42/M44) cart and ranking state.

## Design

- `Catalog.Service` - a new minimal-API service, structured like `Payments.Service` (same Dockerfile/profiler/telemetry bootstrap pattern).
- `Product` / `Category` documents - `Product.Id` is a `[BsonId]` `ObjectId` represented as a `string`; `ProductRepository`/`CategoryRepository` wrap the Mongo driver directly (no repository-of-repositories abstraction - two collections don't need one).
- **Indexes as an explicit step, not implicit.** `ProductRepository.EnsureIndexesAsync` creates a category index (for `ListAsync(categorySlug, ...)`) and a **unique index on `Sku`** - the uniqueness constraint here does real work, see below. `CategoryRepository.EnsureIndexesAsync` creates a unique index on `Slug`.
- **Seeding is idempotent and re-runnable**, matching the migration-job pattern already used for `orders-migrations`/`payments-migrations`: `CatalogSeeder` no-ops if any category already exists, so the seed Job can safely run on every sync rather than needing a one-time flag.
- No auth on `POST /products`/`POST /categories` - a deliberate scope decision for this milestone; write-path auth for the catalog is out of scope until (if) an admin surface is built.

## What broke during live deployment, and why

Three real GitOps issues surfaced getting this milestone live - none were code bugs, all were genuine Argo CD / K8s ordering and configuration gaps this project hadn't hit before because every prior milestone's infrastructure dependencies already existed by the time the dependent resource was added.

### 1. PreSync hook deadlock: the seed Job needed a Service that didn't exist yet

`catalog-seed-m40` was first written as an `argocd.argoproj.io/hook: PreSync` Job, copying the pattern used by `orders-migrations`/`payments-migrations`. It deadlocked: PreSync hooks run *before* the main sync phase that creates ordinary resources, including the brand-new `mongodb` Service the seed Job needs to connect to. This never surfaced for the existing migration Jobs only because Postgres and Kafka's Services already existed from much earlier milestones by the time those PreSync hooks were introduced - the ordering problem was always latent, just never triggered. Fixed by switching to `PostSync`, since seeding only depends on infrastructure existing, not on running before the app starts (unlike a schema migration).

### 2. A stale CDN cache produced a false "the push didn't work"

Immediately after pushing the PreSync→PostSync fix, `curl raw.githubusercontent.com/.../catalog-seed-job.yaml` returned a 404 - looking exactly like the push had failed. It hadn't: `gh api repos/.../contents/...` (uncached, authoritative) confirmed the file was correctly on `main`. `raw.githubusercontent.com` had simply served a stale cached negative for a path that had started existing seconds earlier. Lesson for next time: don't trust the raw CDN for verification in the seconds right after a push.

### 3. Argo CD's retry loop reused a stale manifest snapshot mid-fix

Even after the fix was confirmed on GitHub and `status.sync.revision` showed the right commit, the *live* `catalog-seed-m40` Job still had `PreSync` in its actual annotations. Root cause: the fix was pushed while Argo's automated retry loop was already mid-cycle on the old failing operation, and the retries were reusing the manifest snapshot resolved at the *original* operation start rather than re-rendering from git on each retry. Fixed by deleting the stuck Job, clearing the stuck operation (`kubectl patch application ... -p '{"operation":null}'`), and explicitly triggering a fresh sync operation pinned to `HEAD`.

### 4. The real blocker: EndpointSlice is excluded from Argo CD's watch list entirely

After the above three fixes, the `mongodb` Service existed and Argo reported `Synced`, but both `catalog-service` pods sat at `1/2` Ready indefinitely - well past the startup and readiness probe grace periods. `kubectl logs -c catalog-service` showed the app itself was up and serving `/health/live` (200 OK), but every `/health/ready` call was timing out at exactly 2000ms and returning HTTP 499 (the probe's own client-side timeout firing while `MongoHealthCheck`'s ping was still hanging).

The Mongo connection was hanging because **`kubectl get endpoints mongodb` returned `NotFound`, and no `mongodb-compose` `EndpointSlice` existed either** - the `mongodb` Service had no backend at all, despite DNS resolving `mongodb.orders-lab.svc.cluster.local` to its ClusterIP just fine (DNS resolution and having a routable backend are different things for a headless-selector Service). The `mongodb-compose` EndpointSlice manifest *was* present and correct in `kubernetes/overlays/local/infrastructure-endpoints.yaml`, tracked in the kustomize resource list, and pushed to git - yet it was never applied.

The actual cause: `argocd-cm`'s `resource.exclusions` explicitly excludes `EndpointSlice` (and `Endpoints`) cluster-wide, to cut down on watched-event volume:

```yaml
resource.exclusions: |
  - apiGroups: ['', 'discovery.k8s.io']
    kinds: [Endpoints, EndpointSlice]
```

Argo CD **cannot sync EndpointSlice resources at all**, regardless of what's in the kustomize manifest list. Every other infra `*-compose` EndpointSlice in that same file (`postgres-compose`, `kafka-compose`, `redis-compose`, and so on) only exists live because it was applied manually with `kubectl apply` back when each was introduced in an earlier milestone - a step that was simply missed for `mongodb-compose` this time, and had no automated safety net to catch it because Argo's health/sync status has no visibility into a resource kind it's configured to ignore. Fixed with a one-off `kubectl apply -f` of the EndpointSlice; both pods went `2/2` Ready within 15 seconds (one readiness probe cycle) of the backend actually existing.

**Takeaway for the next infra Service added against a compose-managed dependency:** adding an `EndpointSlice` to `infrastructure-endpoints.yaml` and pushing it is necessary but not sufficient - it must also be applied manually via `kubectl apply`, exactly like every prior one in that file. Argo CD will report `Synced` and never flag the gap, because from its perspective the resource doesn't exist to track.

### 5. Found live: duplicate SKU surfaced as a raw 500

Once the pods were healthy, live validation of `POST /products` with an already-used SKU returned an unhandled ASP.NET `ProblemDetails` 500 instead of a meaningful error - the unique-index `MongoWriteException` was never caught. Fixed by catching `MongoWriteException` where `WriteError.Category == ServerErrorCategory.DuplicateKey` and returning `409 Conflict` with a clear message. Rebuilt (`catalog-service:milestone-40-catalog-dupsku-fix`), redeployed, reverified live.

## Live results

- **Seeded data**: 4 categories (`electronics`, `books`, `clothing`, `home`), 9 products total (3/2/2/2 respectively) with genuinely heterogeneous `Attributes` per category (`ram`/`storage`/`cpu` for electronics, `author`/`pages` for books, `size`/`color` for clothing, `capacity`/`power` for home).
- **`GET /categories`** and **`GET /products?category=electronics`** verified against the live ClusterIP - correct data, correct shapes.
- **Duplicate SKU**: `POST /products` with an existing SKU now returns `409 Conflict` with `{"message":"A product with sku '...' already exists."}` (previously an unhandled 500).
- **Unit/integration tests**: 3/3 passing against real Testcontainers MongoDB (insert+find+list-by-category, malformed-ObjectId returns null instead of throwing, duplicate SKU throws `MongoWriteException` at the repository layer).
- **Regression check**: `scripts/k6-run.sh smoke` post-deploy - `failed_rate=0`, `checks_rate=1`, `flow_rate=1`. Catalog.Service's presence in the cluster (2 pods, MongoDB dependency, new EndpointSlice) has no effect on the existing orders pipeline.

## Running it

```bash
kubectl exec into any node with cluster access, or from the k3s host itself:
curl http://<catalog-service-clusterip>/categories
curl "http://<catalog-service-clusterip>/products?category=electronics&limit=20"
```
