# Milestone 29 Contract Testing with Pact

## Scope

Milestone 19 gave the asynchronous side of this system (Kafka/Avro) a real contract-evolution story: Schema Registry, `BACKWARD` compatibility, and a genuine live-tested example of a compatible vs. incompatible change. The synchronous side - `orders-api`'s REST surface - had nothing equivalent. This milestone adds consumer-driven contract testing with Pact, and proves both directions live: a real contract verified successfully against the real, deployed, auth-enforcing API, and a deliberately broken contract caught as a real failure against that same live API.

## Design

- **No internal service-to-service HTTP contract exists to test.** Confirmed directly (`grep` across every service for outbound HTTP client usage) - Orders.Worker and Payments.Service coordinate with `orders-api` purely through Kafka, a fact Milestone 24's mesh investigation already established independently. The one real synchronous contract in this system is `orders-api`'s own REST API as seen by whatever calls it directly - this project's own scripts today, a future frontend or mobile client tomorrow.
- **`Orders.ContractTests`** (new test project, added to `LocalDistributedLab.slnx`):
  - `OrdersApiConsumerTests` - defines two interactions (`POST /orders` succeeds; `GET /orders/{unknown-id}` 404s) against PactNet's in-process mock server, producing `apps/pacts/OrdersClient-OrdersApi.json`. Fully hermetic - runs as part of the normal `dotnet test`, including in CI.
  - `OrdersApiProviderTests` - verifies that same pact file against a **real, running** `orders-api`. Deliberately *not* hermetic and deliberately *not* something CI can run (GitHub Actions has no route to this lab's server, the same class of constraint Milestone 25 hit for the schema registry) - but unlike that case, a hermetic stand-in would defeat the actual point of provider verification, which is checking the real deployed service. Reads `ORDERS_API_URL`/`ACCESS_TOKEN` from the environment and does nothing (not a failure) when they're unset, so including it in the default `dotnet test` run never silently costs real coverage - "not configured" and "a real verification failure" stay visibly different outcomes.
- **The `GET` interaction tests "order not found," not "read an existing order back."** The latter needs Pact's provider-state fixture protocol - a callback endpoint the API would expose purely so this test can seed data with an ID matching whatever the recorded interaction expects. Not worth adding test-only surface area to `orders-api` for. A random, never-created ID 404ing is itself a real, worthwhile contract, and needs no fixture at all, hermetic or live.

## What didn't work

**`PactNet.Verifier` doesn't exist as a separate NuGet package in the 5.x line - both consumer and verifier functionality ship in the single `PactNet` package.** First restore failed outright looking for a nonexistent package. Caught by checking NuGet's search API directly rather than assuming the old (v4-era) package split still held.

**`Match.Regex`'s two arguments are `(example, pattern)`, and the first attempt had them backwards** - `Match.Regex("Bearer .+", "Bearer contract-test-token")` instead of the reverse. The tests still passed (mock-server matching happened to tolerate it), but the generated pact file recorded the *pattern string itself* as the literal example value sent by the mock consumer - wrong, if subtle, and would have shipped a slightly nonsensical recorded interaction. Confirmed the actual parameter order directly from the `pact-foundation/pact-net` source (`Match.cs`) rather than guessing from memory, then fixed both occurrences.

**Provider verification would have sent the literal fake example header (`Bearer contract-test-token`) to the real, auth-enforcing `orders-api`** (Milestone 26) and failed with a real 401 - not a contract problem, just a stale credential replayed verbatim. Fixed with `WithCustomHeader("Authorization", $"Bearer {accessToken}")`, which overrides the outgoing header for every request the verifier makes - the documented, intended mechanism (per `IPactVerifierSource`'s own doc comment) for exactly this "the recorded example was never meant to be a real credential" situation.

## Results

**Provider verification against the real, live, deployed `orders-api`, with a real Keycloak-issued token:**

```
$ ORDERS_API_URL=http://127.0.0.1:18110 ACCESS_TOKEN="$(scripts/keycloak-get-token.sh)" \
    dotnet test tests/Orders.ContractTests --filter "FullyQualifiedName~OrdersApiProviderTests"

Passed!  - Failed: 0, Passed: 1, Skipped: 0, Total: 1, Duration: 191 ms
```

**The negative case - proving verification actually verifies something**, not just trivially passing: a field (`totalPriceInCents`) the real API has never returned was injected into the pact file's expected response, and the identical verification command run again against the identical live API:

```
Orders.ContractTests.OrdersApiProviderTests.VerifyOrdersApiAgainstTheGeneratedPact [FAIL]
PactNet.Exceptions.PactVerificationFailedException : Pact verification failed
Failed!  - Failed: 1, Passed: 0, Skipped: 0, Total: 1, Duration: 193 ms
```

The pact file was restored immediately after.

### Regression check

`dotnet test` (full solution, no live-deployment environment variables set): 24 unit + 7 integration + 3 contract (2 consumer + provider no-op), all passing. The provider test's graceful no-op is itself exercised here, not just described.

## Running the experiment

```bash
# Regenerate the contract (hermetic, safe anywhere)
dotnet test apps/tests/Orders.ContractTests --filter "FullyQualifiedName~Consumer"

# Verify it against a real deployment
kubectl port-forward -n orders-lab service/orders-api 18110:80 &
TOKEN=$(scripts/keycloak-get-token.sh)
ORDERS_API_URL=http://127.0.0.1:18110 ACCESS_TOKEN="$TOKEN" \
  dotnet test apps/tests/Orders.ContractTests --filter "FullyQualifiedName~Provider"
```
