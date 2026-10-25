using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Operations;
using Harmonizer.Worker.DependencyInjection;
using Harmonizer.Worker.Observability;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Harmonizer.UnitTests;

public sealed class HarmonizerRegistrationTests
{
    private static HostApplicationBuilder Builder(int concurrentFarms = 2)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Harmonizer:QueueUrl"] = "https://sqs.example.invalid/harmonizer",
            ["Harmonizer:BlobContainerUrl"] = "https://raw.example.invalid/data",
            ["Harmonizer:OutputBlobContainerUrl"] = "https://harmonized.example.invalid/data",
            ["Harmonizer:PostgresConnectionString"] = "Host=localhost;Database=cat;Username=test;SSL Mode=Disable",
            ["Harmonizer:EventLifecycle:ConcurrentFarms"] = concurrentFarms.ToString()
        });
        builder.AddHarmonizerTelemetry();
        builder.Services.AddHarmonizer(builder.Configuration);
        return builder;
    }

    [Fact]
    public void All_registered_source_and_lifecycle_dependencies_are_valid_and_share_the_telemetry_host()
    {
        var builder = Builder();
        using var services = builder.Services.BuildServiceProvider(new ServiceProviderOptions
        { ValidateOnBuild = true, ValidateScopes = true });
        Assert.NotNull(services.GetRequiredService<HarmonizationDispatcher>());
        Assert.Equal(2, services.GetServices<IHarmonizationHandler>().Count());
        Assert.Equal("Outage", Assert.Single(services.GetServices<IEventLifecycleHandler>()).SourceType);
        Assert.True(services.GetRequiredService<IOptions<HarmonizerOptions>>().Value.EventLifecycle.Enabled);
    }

    [Fact]
    public void Invalid_nested_lifecycle_options_fail_validation()
    {
        var builder = Builder(concurrentFarms: 0);
        using var services = builder.Services.BuildServiceProvider();
        Assert.Throws<OptionsValidationException>(() => services.GetRequiredService<IOptions<HarmonizerOptions>>().Value);
    }
}
