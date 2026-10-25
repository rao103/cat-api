using System.Reflection;
using Centralize.Api.Configuration;
using Centralize.Api.Controllers.v1;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Centralize.UnitTests;

public sealed class CentralApiServicesTests
{
    [Fact]
    public async Task Development_AuthenticatesLocalIdentityWithoutExternalServices()
    {
        var builder = Builder(Environments.Development, azureAd: false);
        Configure(builder);
        await using var app = builder.Build();
        using var scope = app.Services.CreateScope();
        var context = new DefaultHttpContext { RequestServices = scope.ServiceProvider };

        var result = await context.RequestServices.GetRequiredService<IAuthenticationService>()
            .AuthenticateAsync(context, null);

        Assert.True(result.Succeeded);
        Assert.Equal("00000000-0000-0000-0000-00000000de00", result.Principal!.FindFirst("oid")!.Value);
        var schemes = await app.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetAllSchemesAsync();
        Assert.DoesNotContain(schemes, scheme => scheme.Name == "Bearer");
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Staging")]
    public void NonDevelopment_RejectsDisabledAuthentication(string environment)
    {
        var error = Assert.Throws<TargetInvocationException>(() => Configure(Builder(environment, azureAd: false)));
        Assert.IsType<InvalidOperationException>(error.InnerException);
    }

    [Theory]
    [InlineData("Instance")]
    [InlineData("TenantId")]
    [InlineData("ClientId")]
    public async Task EnabledAuthentication_RejectsMissingConfiguration(string setting)
    {
        var builder = Builder(Environments.Production, azureAd: true);
        builder.Configuration[$"AzureAd:{setting}"] = "";
        Configure(builder);
        await using var app = builder.Build();

        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<IOptions<AzureAdOptions>>().Value);
    }

    [Fact]
    public async Task Production_UsesBearerWithoutDevelopmentAuthentication()
    {
        var builder = Builder(Environments.Production, azureAd: true);
        Configure(builder);
        await using var app = builder.Build();
        var schemes = await app.Services.GetRequiredService<IAuthenticationSchemeProvider>().GetAllSchemesAsync();

        Assert.Contains(schemes, scheme => scheme.Name == "Bearer");
        Assert.DoesNotContain(schemes, scheme => scheme.Name == "Development");
        Assert.True(app.Services.GetRequiredService<IOptions<AzureAdOptions>>().Value.Enabled);
    }

    [Fact]
    public async Task JobServices_RequireNamespaceAndKeepAuthorizedRoutes()
    {
        var builder = Builder(Environments.Development, false);
        builder.Configuration["JobTrigger:Namespace"] = "";
        builder.Configuration["JobTrigger:Sources:Production:CronJobName"] = "";
        Configure(builder);
        await using var app = builder.Build();
        Assert.IsType<Centralize.Core.Repositories.PostgresOperationRepository>(
            app.Services.GetRequiredService<Centralize.Core.Interfaces.IOperationRepository>());
        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<IOptions<JobTriggerOptions>>().Value);
        var routes = app.Services.GetRequiredService<Microsoft.AspNetCore.Mvc.Infrastructure.IActionDescriptorCollectionProvider>()
            .ActionDescriptors.Items.OfType<Microsoft.AspNetCore.Mvc.Controllers.ControllerActionDescriptor>().ToArray();
        Assert.Contains(routes, action => action.AttributeRouteInfo!.Template == "v{version:apiVersion}/windfarms/{windfarmId:int}/refresh");
        Assert.Contains(routes, action => action.AttributeRouteInfo!.Template == "v{version:apiVersion}/windfarms/{windfarmId:int}/sources/{sourceType}/refresh");
        Assert.Contains(routes, action => action.AttributeRouteInfo!.Template == "v{version:apiVersion}/operations/{operationId:guid}");
        Assert.All(routes.Where(action => action.ControllerName != "Health"),
            action => Assert.NotNull(action.ControllerTypeInfo.GetCustomAttribute<Microsoft.AspNetCore.Authorization.AuthorizeAttribute>()));
    }

    [Fact]
    public async Task JobServices_RequireControlPlaneConfiguration()
    {
        var builder = Builder(Environments.Development, false);
        builder.Configuration["Source:ControlPlaneConnectionString"] = "";
        Configure(builder);
        await using var app = builder.Build();
        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<
            IOptions<ProductionIngestor.Configuration.ControlPlaneOptions>>().Value);
    }

    [Fact]
    public async Task AwsIam_ResolvesRepositoriesWithoutStaticPassword()
    {
        var certificate = Path.GetTempFileName();
        try
        {
            var builder = Builder(Environments.Development, false);
            builder.Configuration["Source:ControlPlaneConnectionString"] = "";
            builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ControlPlane:Authentication"] = "AwsIam",
                ["ControlPlane:Host"] = "unit.eu-north-1.rds.amazonaws.com",
                ["ControlPlane:SigningHost"] = "unit.eu-north-1.rds.amazonaws.com",
                ["ControlPlane:Port"] = "55432",
                ["ControlPlane:SigningPort"] = "5432",
                ["ControlPlane:Database"] = "test",
                ["ControlPlane:Username"] = "superuser",
                ["ControlPlane:Region"] = "eu-north-1",
                ["ControlPlane:AwsProfile"] = "superuser",
                ["ControlPlane:RootCertificate"] = certificate
            });
            Configure(builder);
            await using var app = builder.Build();
            var factory = app.Services.GetRequiredService<ProductionIngestor.Factories.PostgresConnectionFactory>();
            await using var connection = factory.CreateConnection();
            var settings = new Npgsql.NpgsqlConnectionStringBuilder(connection.ConnectionString);
            Assert.Equal(55432, settings.Port);
            Assert.Equal(Npgsql.SslMode.VerifyFull, settings.SslMode);
            Assert.True(string.IsNullOrEmpty(settings.Password));
            Assert.IsType<ProductionIngestor.Repositories.PostgresExtractionCursorRepository>(
                app.Services.GetRequiredService<ProductionIngestor.Interfaces.IExtractionCursorRepository>());
            Assert.IsType<Centralize.Core.Repositories.PostgresOperationRepository>(
                app.Services.GetRequiredService<Centralize.Core.Interfaces.IOperationRepository>());
        }
        finally { File.Delete(certificate); }
    }

    [Fact]
    public async Task AwsIam_InvalidConfigurationDoesNotFallBackToStaticPassword()
    {
        var builder = Builder(Environments.Development, false);
        builder.Configuration["ControlPlane:Authentication"] = "AwsIam";
        Configure(builder);
        await using var app = builder.Build();
        Assert.Throws<OptionsValidationException>(() => app.Services.GetRequiredService<
            IOptions<ProductionIngestor.Configuration.ControlPlaneOptions>>().Value);
    }

    private static WebApplicationBuilder Builder(string environment, bool azureAd)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = environment,
            ApplicationName = typeof(HealthController).Assembly.GetName().Name,
            ContentRootPath = AppContext.BaseDirectory
        });
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["AzureAd:Enabled"] = azureAd.ToString(),
            ["AzureAd:Instance"] = "https://login.microsoftonline.com/",
            ["AzureAd:TenantId"] = "00000000-0000-0000-0000-000000000001",
            ["AzureAd:ClientId"] = "00000000-0000-0000-0000-000000000002",
            ["Telemetry:ExportEnabled"] = "false",
            ["Source:ControlPlaneConnectionString"] = "Host=unit.invalid;Database=test;Username=test",
            ["JobTrigger:Namespace"] = "unit",
            ["JobTrigger:Sources:Production:CronJobName"] = "production-ingestor",
            ["JobTrigger:Sources:Production:WorkerContainerName"] = "production-ingestor-worker"
        });
        return builder;
    }

    private static void Configure(WebApplicationBuilder builder)
    {
        typeof(HealthController).Assembly.GetType("Centralize.Api.Configurators.ServicesConfigurator")!
            .GetMethod("ConfigureServices", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, [builder.WebHost]);
    }
}
