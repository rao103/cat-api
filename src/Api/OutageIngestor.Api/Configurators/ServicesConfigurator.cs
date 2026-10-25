using k8s;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Versioning;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Web;
using OutageIngestor.Api.Configuration;
using OutageIngestor.Api.Interfaces;
using OutageIngestor.Api.Monitoring;
using OutageIngestor.Api.Security;
using OutageIngestor.Api.Triggering;
using OutageIngestor.Configuration;
using OutageIngestor.Interfaces;
using OutageIngestor.Operations.Refresh;
using OutageIngestor.Repositories;

namespace OutageIngestor.Api.Configurators;

internal static class ServicesConfigurator
{
    public static IWebHostBuilder ConfigureServices(this IWebHostBuilder builder) =>
        builder.ConfigureServices(ConfigureServices);

    private static void ConfigureServices(WebHostBuilderContext context, IServiceCollection services)
    {
        services.AddOptions();
        services.Configure<AzureAdOptions>(context.Configuration.GetSection(AzureAdOptions.SectionName));
        var localRefresh = context.HostingEnvironment.IsDevelopment() &&
            context.Configuration.GetValue<bool>("LocalRefresh:Enabled");
        services.AddOptions<JobTriggerOptions>()
            .Bind(context.Configuration.GetSection(JobTriggerOptions.SectionName))
            .Validate(options => localRefresh ||
                (!string.IsNullOrWhiteSpace(options.Namespace) && !string.IsNullOrWhiteSpace(options.CronJobName)),
                "JobTrigger Namespace and CronJobName are required.").ValidateOnStart();
        services.AddOptions<SourceOptions>()
            .Bind(context.Configuration.GetSection(SourceOptions.SectionName))
            .Validate(options => !string.IsNullOrWhiteSpace(options.ControlPlaneConnectionString),
                "Source ControlPlaneConnectionString is required.").ValidateOnStart();

        var azureAd = context.Configuration.GetSection(AzureAdOptions.SectionName).Get<AzureAdOptions>();

        services.AddControllers();
        services.AddSingleton(typeof(OutageIngestor.Api.Interfaces.ITelemetry<>), typeof(Telemetry<>));

        services.AddApiVersioning(options =>
        {
            options.DefaultApiVersion = new ApiVersion(1, 0);
            options.AssumeDefaultVersionWhenUnspecified = true;
            options.ApiVersionReader = new UrlSegmentApiVersionReader();
        });

        services.AddVersionedApiExplorer(options =>
        {
            options.GroupNameFormat = "'v'VVV";
            options.SubstituteApiVersionInUrl = true;
        });

        services.AddEndpointsApiExplorer();
        services.AddOpenApiDocuments();

        // Same durable control-plane store the ingestion Job reads/writes; API only calls the read-only method.
        services.AddSingleton<IExtractionCursorRepository>(sp =>
        {
            var source = sp.GetRequiredService<IOptions<SourceOptions>>().Value;
            return new PostgresExtractionCursorRepository(source.ControlPlaneConnectionString);
        });

        services.AddSingleton<OutageRefreshPlanner>();
        services.AddSingleton<IExtractionRunHistoryRepository>(provider =>
            new PostgresExtractionRunHistoryRepository(provider.GetRequiredService<IOptions<SourceOptions>>().Value.ControlPlaneConnectionString));
        // Do not load kubeconfig just to read local worker history.
        services.AddSingleton(provider => new RefreshStatusService(
            localRefresh ? null : provider.GetRequiredService<IKubernetes>(),
            provider.GetRequiredService<IOptions<JobTriggerOptions>>(),
            provider.GetRequiredService<IExtractionRunHistoryRepository>(),
            provider.GetRequiredService<IHostEnvironment>(), context.Configuration));

        services.AddSingleton<IKubernetes>(_ => new Kubernetes(KubernetesClientConfiguration.BuildDefaultConfig()));
        if (localRefresh)
        {
            services.AddHttpClient<IIngestionJobTrigger, LocalIngestionJobTrigger>(client =>
            {
                client.BaseAddress = new Uri("http://localhost:5188/");
                client.Timeout = TimeSpan.FromSeconds(30);
            });
        }
        else
        {
            services.AddSingleton<IIngestionJobTrigger, KubernetesIngestionJobTrigger>();
        }

        if (azureAd?.Enabled == true)
        {
            services.AddAuthentication(auth =>
                {
                    auth.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                    auth.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
                    auth.DefaultForbidScheme = JwtBearerDefaults.AuthenticationScheme;
                })
                .AddMicrosoftIdentityWebApi(context.Configuration.GetSection(AzureAdOptions.SectionName));

        }
        else if (context.HostingEnvironment.IsDevelopment())
        {
            services.AddAuthentication(DevelopmentAuthenticationHandler.SchemeName)
                .AddScheme<AuthenticationSchemeOptions, DevelopmentAuthenticationHandler>(
                    DevelopmentAuthenticationHandler.SchemeName, _ => { });
        }
        else
        {
            throw new InvalidOperationException("AzureAd authentication must be enabled outside Development.");
        }

        services.AddAuthorization();
    }
}
