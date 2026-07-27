using System.Diagnostics;
using BuildingBlocks;
using OpenTelemetry.Trace;

namespace Orders.UnitTests;

public sealed class OrdersSamplerTests
{
    private static readonly KeyValuePair<string, object?>[] NoTags = [];
    private static readonly ActivityLink[] NoLinks = [];

    [Theory]
    [InlineData("postgresql")]
    [InlineData("CONNECT orders")]
    public void DropsRootDatabaseOperations(string operationName)
    {
        var parameters = CreateParameters(default, operationName, ActivityKind.Client);

        var result = new OrdersSampler().ShouldSample(parameters);

        Assert.Equal(SamplingDecision.Drop, result.Decision);
    }

    [Fact]
    public void SamplesRootHttpOperations()
    {
        var parameters = CreateParameters(default, "POST /orders", ActivityKind.Server);

        var result = new OrdersSampler().ShouldSample(parameters);

        Assert.Equal(SamplingDecision.RecordAndSample, result.Decision);
    }

    [Fact]
    public void SamplesDatabaseOperationsWithSampledParent()
    {
        var parent = new ActivityContext(
            ActivityTraceId.CreateRandom(),
            ActivitySpanId.CreateRandom(),
            ActivityTraceFlags.Recorded);
        var parameters = CreateParameters(parent, "postgresql", ActivityKind.Client);

        var result = new OrdersSampler().ShouldSample(parameters);

        Assert.Equal(SamplingDecision.RecordAndSample, result.Decision);
    }

    private static SamplingParameters CreateParameters(
        ActivityContext parent,
        string name,
        ActivityKind kind)
    {
        return new SamplingParameters(
            parent,
            ActivityTraceId.CreateRandom(),
            name,
            kind,
            NoTags,
            NoLinks);
    }
}
