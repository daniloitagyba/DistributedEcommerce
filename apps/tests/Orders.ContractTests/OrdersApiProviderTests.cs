using PactNet.Verifier;

namespace Orders.ContractTests;

/// <summary>
/// Verifies the pact OrdersApiConsumerTests generated against a real,
/// running Orders.Api - deliberately not hermetic, and deliberately not
/// wired into `dotnet test`'s default CI run: GitHub Actions runners have
/// no route to this lab's server (the same class of problem Milestone 25
/// hit and fixed for the schema registry - but here, unlike that one,
/// hermetic replacement isn't the right answer, because the entire point
/// of provider verification is checking the *real* deployed service, not
/// a stand-in for it). Run explicitly, against a live deployment, with a
/// real bearer token - see docs/cicd/milestone-29-contract-testing.md.
///
/// Requires ORDERS_API_URL and ACCESS_TOKEN; does nothing (not a failure)
/// when they're unset, so this project stays safe to include in the
/// default `dotnet test` run without ever silently skipping real CI
/// coverage - "not configured" and "verification failed" are visibly
/// different outcomes.
/// </summary>
public sealed class OrdersApiProviderTests
{
    [Fact]
    public void VerifyOrdersApiAgainstTheGeneratedPact()
    {
        var providerUrl = Environment.GetEnvironmentVariable("ORDERS_API_URL");
        var accessToken = Environment.GetEnvironmentVariable("ACCESS_TOKEN");

        if (string.IsNullOrWhiteSpace(providerUrl) || string.IsNullOrWhiteSpace(accessToken))
        {
            Console.WriteLine(
                "Skipping provider verification: ORDERS_API_URL and ACCESS_TOKEN must both be set to a " +
                "live deployment and a valid bearer token. See docs/cicd/milestone-29-contract-testing.md.");
            return;
        }

        var pactPath = Path.Combine("..", "..", "..", "..", "..", "pacts", "OrdersClient-OrdersApi.json");

        // The recorded interactions carry a fake "Bearer contract-test-token"
        // (a Match.Regex example value, not a real credential) - a custom
        // header override is what makes replaying them against the real,
        // auth-enforcing Orders.Api (Milestone 26) actually authenticate.
        new PactVerifier("OrdersApi")
            .WithHttpEndpoint(new Uri(providerUrl))
            .WithFileSource(new FileInfo(pactPath))
            .WithRequestTimeout(TimeSpan.FromSeconds(10))
            .WithCustomHeader("Authorization", $"Bearer {accessToken}")
            .Verify();
    }
}
