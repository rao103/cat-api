using System.Reflection;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OutageIngestor.Api.Configuration;
using OutageIngestor.Api.Controllers.v1;
using OutageIngestor.Api.Interfaces;
using OutageIngestor.Api.Monitoring;
using OutageIngestor.Api.Triggering;
using OutageIngestor.Configuration;
using OutageIngestor.Interfaces;

namespace OutageIngestor.UnitTests;

public sealed class ApiServicesTests
{
    [Fact]
    public async Task Development_ResolvesLocalServicesAndAuditableIdentity_WithoutKubeconfig()
    {
        var builder = Builder(Environments.Development, azureAd: false, local: true);
        Configure(builder);
        await using var app = builder.Build();
        using var scope = app.Services.CreateScope();
        var services = scope.ServiceProvider;

        Assert.IsType<LocalIngestionJobTrigger>(services.GetRequiredService<IIngestionJobTrigger>());
        Assert.NotNull(services.GetRequiredService<RefreshStatusService>());
        Assert.NotNull(services.GetRequiredService<IExtractionRunHistoryRepository>());
        Assert.IsType<Telemetry<RefreshController>>(services.GetRequiredService<OutageIngestor.Api.Interfaces.ITelemetry<RefreshController>>());
        var context = new DefaultHttpContext { RequestServices = services };
        var authenticated = await services.GetRequiredService<IAuthenticationService>().AuthenticateAsync(context, null);
        Assert.True(authenticated.Succeeded);
        Assert.Equal("00000000-0000-0000-0000-00000000de00", authenticated.Principal!.FindFirst("oid")!.Value);
        // Blank namespace is valid only for the development local transport.
        Assert.NotNull(services.GetRequiredService<IOptions<JobTriggerOptions>>().Value);
    }

    [Fact]
    public async Task Production_WithAzureAd_RegistersKubernetesTrigger_AndNeverDevelopmentAuthentication()
    {
        var builder = Builder(Environments.Production, azureAd: true, local: true);
        Configure(builder);
        var registration = Assert.Single(builder.Services.Where(service => service.ServiceType == typeof(IIngestionJobTrigger)));
        Assert.Equal(typeof(KubernetesIngestionJobTrigger), registration.ImplementationType);
        await using var app = builder.Build();
        var schemes = await app.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetAllSchemesAsync();
        Assert.DoesNotContain(schemes, scheme => scheme.Name == "Development");
        Assert.Contains(schemes, scheme => scheme.Name == "Bearer");
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Staging")]
    public void NonDevelopment_RejectsDisabledAuthentication(string environment)
    {
        var error = Assert.Throws<TargetInvocationException>(() => Configure(Builder(environment, azureAd: false, local: true)));
        Assert.IsType<InvalidOperationException>(error.InnerException);
    }

    [Fact]
    public async Task Options_RequireControlPlaneConnectionString()
    {
        var builder = Builder(Environments.Development, azureAd: false, local: true);
        builder.Configuration["Source:ControlPlaneConnectionString"] = "";
        Configure(builder);
        await using var app = builder.Build();
        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<IOptions<SourceOptions>>().Value);
    }

    [Fact]
    public async Task KubernetesMode_RequiresNamespaceAndCronJob()
    {
        var builder = Builder(Environments.Development, azureAd: false, local: false);
        Configure(builder);
        await using var app = builder.Build();
        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<IOptions<JobTriggerOptions>>().Value);
    }

    private static WebApplicationBuilder Builder(string environment, bool azureAd, bool local)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = environment,
            ApplicationName = typeof(RefreshController).Assembly.GetName().Name,
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["AzureAd:Enabled"] = azureAd.ToString(),
            ["AzureAd:Instance"] = "https://login.microsoftonline.com/",
            ["AzureAd:TenantId"] = "00000000-0000-0000-0000-000000000001",
            ["AzureAd:ClientId"] = "00000000-0000-0000-0000-000000000002",
            ["LocalRefresh:Enabled"] = local.ToString(),
            ["JobTrigger:Namespace"] = "",
            ["JobTrigger:CronJobName"] = "outage-ingestor",
            ["Source:ControlPlaneConnectionString"] = "Host=localhost;Database=unused;Username=unused",
            ["Telemetry:ExportEnabled"] = "false"
        });
        return builder;
    }

    private static void Configure(WebApplicationBuilder builder)
    {
        // Exercise the real composition without broadening production visibility for tests.
        var assembly = typeof(RefreshController).Assembly;
        assembly.GetType("OutageIngestor.Api.Configurators.ServicesConfigurator")!
            .GetMethod("ConfigureServices", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, [builder.WebHost]);
        assembly.GetType("OutageIngestor.Api.Configurators.TelemetryConfigurator")!
            .GetMethod("ConfigureTelemetry", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, [builder]);
    }
}