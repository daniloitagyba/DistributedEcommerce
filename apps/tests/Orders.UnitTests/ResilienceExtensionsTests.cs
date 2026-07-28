using BuildingBlocks;
using Microsoft.Extensions.DependencyInjection;
using Polly.CircuitBreaker;
using Polly.Registry;
using Polly.Timeout;

namespace Orders.UnitTests;

public sealed class ResilienceExtensionsTests
{
    [Theory]
    [InlineData(ResilienceExtensions.PostgresPipeline)]
    [InlineData(ResilienceExtensions.KafkaProducerPipeline)]
    [InlineData(ResilienceExtensions.RedisPipeline)]
    public void AddOrdersResilienceRegistersNamedPipeline(string pipelineName)
    {
        var provider = new ServiceCollection()
            .AddOrdersResilience()
            .BuildServiceProvider()
            .GetRequiredService<ResiliencePipelineProvider<string>>();

        var pipeline = provider.GetPipeline(pipelineName);

        Assert.NotNull(pipeline);
    }

    [Fact]
    public void IsInfrastructureFaultRecognizesCircuitBreakerAndTimeoutExceptions()
    {
        Assert.True(ResilienceExtensions.IsInfrastructureFault(new BrokenCircuitException()));
        Assert.True(ResilienceExtensions.IsInfrastructureFault(new TimeoutRejectedException()));
        Assert.False(ResilienceExtensions.IsInfrastructureFault(new InvalidOperationException()));
    }
}
