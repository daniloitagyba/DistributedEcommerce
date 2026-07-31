using System.Reflection;
using NetArchTest.Rules;
using Orders.Application.Ports;
using Orders.Application.UseCases.CreateOrder;
using Orders.Infrastructure.Caching;

namespace Orders.ArchitectureTests;

// Fitness functions for the ports-and-adapters boundary between Orders.Application and its outer layers.
public class ApplicationLayerTests
{
    private static readonly Assembly ApplicationAssembly = typeof(CreateOrderHandler).Assembly;
    private static readonly Assembly InfrastructureAssembly = typeof(RedisOrderCache).Assembly;

    [Fact]
    public void ApplicationDoesNotDependOnOuterLayers()
    {
        var result = Types.InAssembly(ApplicationAssembly)
            .ShouldNot()
            .HaveDependencyOnAny("Orders.Infrastructure", "Orders.Api", "Orders.Worker")
            .GetResult();

        Assert.True(result.IsSuccessful, Describe(result));
    }

    [Theory]
    [InlineData("Microsoft.EntityFrameworkCore")]
    [InlineData("Npgsql")]
    [InlineData("Confluent.Kafka")]
    [InlineData("StackExchange.Redis")]
    [InlineData("Microsoft.AspNetCore")]
    public void ApplicationDoesNotDependOnAnyInfrastructureFramework(string frameworkNamespace)
    {
        // BuildingBlocks (an Orders.Application project reference) itself
        // depends on EF Core Relational and StackExchange.Redis, so those
        // assemblies are reachable on the reference graph regardless of
        // this rule. This is the real guardrail: nothing else stops a use
        // case handler from reaching for IDatabase/DbContext directly
        // instead of going through an Orders.Application.Ports interface.
        var result = Types.InAssembly(ApplicationAssembly)
            .ShouldNot()
            .HaveDependencyOn(frameworkNamespace)
            .GetResult();

        Assert.True(result.IsSuccessful, Describe(result));
    }

    [Fact]
    public void PortInterfacesFollowTheIPrefixConvention()
    {
        var result = Types.InAssembly(ApplicationAssembly)
            .That()
            .ResideInNamespace("Orders.Application.Ports")
            .And()
            .AreInterfaces()
            .Should()
            .HaveNameStartingWith("I")
            .GetResult();

        Assert.True(result.IsSuccessful, Describe(result));
    }

    [Theory]
    [InlineData(typeof(IOrderRepository))]
    [InlineData(typeof(IOrderCache))]
    [InlineData(typeof(IIdempotencyStore))]
    [InlineData(typeof(IOrderEventStoreRepository))]
    [InlineData(typeof(IOrderSummaryRepository))]
    public void PortImplementationsLiveInInfrastructure(Type portInterface)
    {
        var implementors = Types.InAssembly(InfrastructureAssembly)
            .That()
            .ImplementInterface(portInterface)
            .GetTypes()
            .ToList();

        // A rule with nothing to check passes trivially - guard against a
        // rename/removal silently making this test meaningless.
        Assert.NotEmpty(implementors);

        var result = Types.InAssembly(InfrastructureAssembly)
            .That()
            .ImplementInterface(portInterface)
            .Should()
            .ResideInNamespaceStartingWith("Orders.Infrastructure")
            .GetResult();

        Assert.True(result.IsSuccessful, Describe(result));
    }

    private static string Describe(TestResult result) =>
        result.FailingTypeNames is null ? "no offending types reported" : string.Join(", ", result.FailingTypeNames);
}
