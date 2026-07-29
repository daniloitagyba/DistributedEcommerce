# Feature Flags with Microsoft.FeatureManagement

## Scope

Tier 3 hardening item: gate a real feature behind a real toggle, using a real library - not a homegrown `if (config.GetValue<bool>("SomeFlag"))` scattered through the code, and not a synthetic flag with no actual consequence either way.

## Design

- **`Microsoft.FeatureManagement`**, Microsoft's own official feature-flag library for .NET - widely used, actively maintained, and (unlike Pyroscope's profiler) needs no native components, no separate server, and no extra infrastructure: it's a config-binding library, registered with `services.AddFeatureManagement()` in `Orders.Application/ApplicationServiceCollectionExtensions.cs`.
- **What it gates**: the Idempotency-Key feature (this same milestone's earlier work, `docs/architecture/idempotency-key.md`) - a real feature with a real behavioral difference when toggled, not a placeholder. `FeatureFlags.IdempotencyKey` (`apps/src/Orders.Application/FeatureFlags.cs`) is the flag name constant.
- **Where it's checked**: `CreateOrderHandler.HandleAsync` (`apps/src/Orders.Application/UseCases/CreateOrder/CreateOrderHandler.cs`) injects `IFeatureManager` directly - the interface has no ASP.NET dependency, so this doesn't cross the Application layer's existing clean-architecture boundary the way injecting, say, `HttpContext` would. `var idempotencyEnabled = await featureManager.IsEnabledAsync(FeatureFlags.IdempotencyKey);` - if the flag is off, the handler behaves exactly as it did before Idempotency-Key existed, regardless of whether the client sends an `Idempotency-Key` header.
- **Config-only, no rebuild**: `FeatureManagement__IdempotencyKey` is a standard environment variable using `Microsoft.FeatureManagement`'s own configuration-section binding convention (`FeatureManagement:<FlagName>`) - the same `Section__Key` pattern every other piece of config in this lab already uses (`Cache__*`, `Idempotency__*`, `RateLimit__*`). Flipping it is a `kubectl` env change, not a new image.

## Results

Live validation against the real K3s `Service`, toggling the flag with no rebuild in between:

**Flag ON (`FeatureManagement__IdempotencyKey=true`, the deployed default):**
```
$ curl -X POST http://$SERVICE_IP/orders -H "Idempotency-Key: flag-on-test" ...
{"id":"2b3469cb-...", "status":"Created", ...}
HTTP 201

$ curl -X POST http://$SERVICE_IP/orders -H "Idempotency-Key: flag-on-test" ...   # same key
HTTP/1.1 200 OK
idempotency-replayed: true
```

**Flag OFF**, flipped live via `kubectl patch rollout orders-api --type=json` on the running `Rollout` (no image change):
```
$ curl -X POST http://$POD_IP:8080/orders -H "Idempotency-Key: flag-off-test" ...
{"id":"3f69752e-8d99-454c-ab22-c4e67f13eae2", ...}
HTTP 201

$ curl -X POST http://$POD_IP:8080/orders -H "Idempotency-Key: flag-off-test" ...   # same key, same pod
{"id":"f20e2f50-27bc-473f-ae7c-bdba78620fe0", ...}   # a DIFFERENT order - idempotency genuinely bypassed
HTTP 201
```

Same client, same `Idempotency-Key` header, two different orders created - the flag is a real, load-bearing switch, not cosmetic. Restoring the git-tracked default (`true`) via `kubectl apply -k` brought replay behavior back immediately.

### Regression check

`dotnet test`: 28 unit tests (1 new: `HandleAsyncIgnoresTheIdempotencyKeyWhenTheFeatureFlagIsDisabled`), all passing. `scripts/k6-run.sh smoke` post-deploy: `failed_rate=0`, `checks_rate=1`, `flow_rate=1`.

## Running it / toggling it

```bash
# Check the current value
kubectl get rollout orders-api -n orders-lab \
  -o jsonpath='{range .spec.template.spec.containers[0].env[*]}{.name}={.value}{"\n"}{end}' \
  | grep FeatureManagement

# Toggle live (temporary - reverted on the next `kubectl apply -k` or Argo CD sync,
# since the git-tracked value in kubernetes/base/orders-api.yaml is the source of truth)
kubectl patch rollout orders-api -n orders-lab --type=json \
  -p '[{"op":"replace","path":"/spec/template/spec/containers/0/env/<index>/value","value":"false"}]'

# Toggle permanently: edit FeatureManagement__IdempotencyKey in
# kubernetes/base/orders-api.yaml, commit, push - Argo CD picks it up.
```
