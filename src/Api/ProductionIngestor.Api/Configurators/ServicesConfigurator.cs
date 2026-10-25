using k8s;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Versioning;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Web;
using ProductionIngestor.Api.Configuration;
using ProductionIngestor.Api.Triggering;
using ProductionIngestor.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Repositories;
using ProductionIngestor.Api.Interfaces;
using ProductionIngestor.Api.Monitoring;
using ProductionIngestor.Operations.Refresh;

namespace ProductionIngestor.Api.Configurators;

internal static class ServicesConfigurator
{
    public static IWebHostBuilder ConfigureServices(this IWebHostBuilder builder) =>
        builder.ConfigureServices(ConfigureServices);

    private static void ConfigureServices(WebHostBuilderContext context, IServiceCollection services)
    {
        services.AddOptions();
        services.Configure<AzureAdOptions>(context.Configuration.GetSection(AzureAdOptions.SectionName));
        services.AddOptions<JobTriggerOptions>()
            .Bind(context.Configuration.GetSection(JobTriggerOptions.SectionName))
            .Validate(options => !string.IsNullOrWhiteSpace(options.Namespace) && !string.IsNullOrWhiteSpace(options.CronJobName),
                "JobTrigger Namespace and CronJobName are required.").ValidateOnStart();
        services.AddOptions<SourceOptions>()
            .Bind(context.Configuration.GetSection(SourceOptions.SectionName))
            .Validate(options => !string.IsNullOrWhiteSpace(options.ControlPlaneConnectionString),
                "Source ControlPlaneConnectionString is required.").ValidateOnStart();

        var azureAd = context.Configuration.GetSection(AzureAdOptions.SectionName).Get<AzureAdOptions>();

        services.AddControllers();
        services.AddSingleton(typeof(Interfaces.ITelemetry<>), typeof(Telemetry<>));

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
        services.AddSingleton<IExtractionCursorRepository>(services =>
        {
            var source = services.GetRequiredService<IOptions<SourceOptions>>().Value;
            return new PostgresExtractionCursorRepository(source.ControlPlaneConnectionString);
        });

        services.AddSingleton<ProductionRefreshPlanner>();
        services.AddSingleton<IExtractionRunHistoryRepository>(provider =>
            new PostgresExtractionRunHistoryRepository(provider.GetRequiredService<IOptions<SourceOptions>>().Value.ControlPlaneConnectionString));
        services.AddSingleton<RefreshStatusService>();

        services.AddSingleton<IKubernetes>(_ =>
            new Kubernetes(KubernetesClientConfiguration.BuildDefaultConfig()));

        if (context.HostingEnvironment.IsDevelopment() &&
            context.Configuration.GetValue<bool>("LocalRefresh:Enabled"))
        {
            services.AddHttpClient<IIngestionJobTrigger, LocalIngestionJobTrigger>(client =>
            {
                client.BaseAddress = new Uri("http://localhost:5187/");
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

            services.AddAuthorization();
        }
    }
}
