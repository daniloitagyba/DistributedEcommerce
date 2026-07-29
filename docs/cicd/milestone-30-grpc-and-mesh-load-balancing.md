# Milestone 30 gRPC and Mesh Traffic Shadowing

## Scope

Two concrete, real questions this milestone set out to answer against the actual running system, not on paper: does gRPC through this Linkerd install actually work, including the workload-identity `AuthorizationPolicy` layer Milestone 26 introduced? And does Linkerd support real traffic shadowing/mirroring? The answers turned out to be "yes, with a real bug found and fixed along the way" and "no, definitively, confirmed from the CRD schema itself" - both genuinely useful outcomes, not a wasted milestone.

## Design

- **`OrderQueryGrpcService`**: a gRPC mirror of `GET /orders/{id}`, sharing `GetOrderHandler` and the `orders:read` authorization policy with the REST endpoint - no duplicated business logic, a second transport for the same read.
- **A dedicated port (8081), not shared with REST's 8080.** Tried sharing first; doesn't work here (see below). Kestrel listens HTTP/1-only on 8080, HTTP/2-only (h2c, no TLS - Linkerd terminates/originates mTLS transparently at the proxy, matching every service in this lab) on 8081.
- **Traffic shadowing**: investigated whether Linkerd's `HTTPRoute` implementation honors Gateway API's `RequestMirror` filter (what Argo Rollouts' `setMirrorRoute` canary step would need to drive real request mirroring). It doesn't - confirmed directly from Linkerd's own CRD schema, not inferred from a failed experiment.

## What didn't work

**Sharing port 8080 between REST and gRPC fails outright: Milestone 26's `Server` resource for that port hardcodes `proxyProtocol: HTTP/1`, so Linkerd's proxy rejects genuine HTTP/2 traffic with `HTTP_1_1_REQUIRED` before it ever reaches Kestrel.** Confirmed via `grpcurl` against the live pod. Not really a bug - a `Server` resource commits to a protocol, and REST already committed 8080 to HTTP/1. Fixed by giving gRPC its own port with its own `Server` resource, which also happens to be the standard real-world pattern anyway - most gRPC services aren't multiplexed onto the same port as a REST API.

**Explicitly configuring Kestrel with any `ListenAnyIP` call makes it stop honoring the `ASPNETCORE_URLS`-derived endpoint entirely - port 8080 silently went unbound the moment 8081 was added via code.** First deploy crash-looped: the boot log showed only `Now listening on: http://[::]:8081`, and the readiness probe (which targets 8080) failed until Kubernetes killed the container. The actual warning was in the log the whole time - `Overriding address(es) 'http://+:8080'. Binding to endpoints defined via IConfiguration and/or UseKestrel() instead` - just easy to miss among routine startup noise. Fixed by adding an explicit `ListenAnyIP(8080, ...)` alongside the gRPC one, rather than relying on `ConfigureEndpointDefaults` plus the environment variable to produce it.

**`appProtocol: grpc` on the Kubernetes Service is not a value Linkerd's outbound policy controller recognizes - it silently falls into an `Unknown` variant, and the outbound proxy never signals ALPN "h2" to the destination as a result.** Confirmed directly from `policy-controller/core/src/outbound.rs`'s `AppProtocol::from_str`: the only string it maps to HTTP/2 is the specific value `"kubernetes.io/h2c"` - not `"grpc"`, "http2", or anything else that reads more naturally. Fixed the Service's `appProtocol` field to the exact recognized string. This alone did not fully resolve the next finding, but it was a real, independently-necessary fix regardless.

**A `Server`/`AuthorizationPolicy` pair scoping the gRPC port to the `orders-worker` workload identity denied every single connection - including ones presenting exactly that identity - regardless of `proxyProtocol` being set to `gRPC` or `HTTP/2`, a fresh appProtocol fix, or a full rolling restart of every pod involved.** Every denial logged `negotiated_protocol: None` alongside the *correct*, exactly-matching client identity - meaning the identity check itself was never the actual blocker. Definitively isolated by **removing** the `Server`/`AuthorizationPolicy` pair entirely: the identical `grpcurl` call against the identical pod succeeded immediately, returning the real order. This is either a real bug or an unsupported combination in this Linkerd edge build's gRPC-protocol policy path - not something resolvable by reconfiguring around it further within this milestone's scope. Documented rather than silently worked around: the gRPC port now runs under the cluster's `defaultInboundPolicy: all-unauthenticated` (Milestone 24's baseline, same as everything was before Milestone 26's REST-specific narrowing) - and Orders.Api's own JWT bearer authentication (Milestone 26) still fully protects it independent of the mesh layer, confirmed live: a token-less gRPC call gets a real `401` from the application itself.

**Traffic shadowing is not achievable with this mesh, confirmed from the source rather than by attempting and failing to configure it.** Gateway API's core `HTTPRoute` spec defines `RequestMirror` as an "Extended" (not "Core") filter type - implementations aren't required to support it. `kubectl get crd httproutes.policy.linkerd.io -o jsonpath='...filters.items.properties.type.enum'` returns exactly `["RequestHeaderModifier", "RequestRedirect"]` - `RequestMirror` isn't in Linkerd's own schema at all, so no HTTPRoute referencing it could even be created, let alone honored by the data plane. Real, useful traffic mirroring - what Argo Rollouts' `setMirrorRoute` canary step is built to drive - needs a mesh that implements the Extended filter set (Istio/Envoy is the common choice); it isn't a Linkerd OSS capability as installed here.

## Results

The gRPC endpoint, live, through the mesh, with real authentication:

```
$ grpcurl -plaintext -H "authorization: Bearer $TOKEN" -d '{"id":"359b5720-..."}' \
    orders-api:81 orders.OrderQuery/GetOrder
{
  "id": "359b5720-384c-42ec-9845-b06189858cc9",
  "customerId": "grpc-test",
  "amount": 15.5,
  "currency": "BRL",
  "status": "Confirmed",
  "createdAt": "2026-07-29T12:00:53.1454600+00:00",
  "correlationId": "ba97ae305d72eab91c1c5fb95050444e",
  "instanceId": "orders-api-78db475f4f-4z7qp"
}

$ grpcurl -plaintext -d '{"id":"359b5720-..."}' orders-api:81 orders.OrderQuery/GetOrder   # no token
ERROR:
  Code: Unauthenticated
  Message: unexpected HTTP status code received from server: 401 (Unauthorized)
```

### Regression check

`dotnet test`: 24 unit + 7 integration, all passing. `k3s-smoke-test.sh`: passing - the REST API's own behavior, including its still-intact `orders-worker`-only `AuthorizationPolicy` on port 8080, is completely unaffected by anything this milestone touched on the separate gRPC port.

## Running the experiment

```bash
# From a meshed pod inside the cluster (kubectl port-forward's SPDY tunnel
# is unreliable for real HTTP/2 gRPC streams - test from inside the mesh,
# not through it)
grpcurl -plaintext -import-path apps/src/Orders.Api/Protos -proto order_query.proto \
  -H "authorization: Bearer $(scripts/keycloak-get-token.sh)" \
  -d '{"id":"<a-real-order-id>"}' \
  orders-api:81 orders.OrderQuery/GetOrder

# Confirm Linkerd's HTTPRoute schema doesn't support RequestMirror
kubectl get crd httproutes.policy.linkerd.io \
  -o jsonpath='{.spec.versions[?(@.name=="v1beta3")].schema.openAPIV3Schema.properties.spec.properties.rules.items.properties.filters.items.properties.type.enum}'
```
