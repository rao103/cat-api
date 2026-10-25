using Centralize.Api.Configuration;
using Centralize.Api.Interfaces;
using Centralize.Api.Triggering;
using k8s;
using Centralize.Api.SourceTypes.Production.Triggering;
using Centralize.Core.Interfaces;
using Centralize.Core.Repositories;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Repositories;
using ProductionIngestor.Validators;

namespace Centralize.Api.Configurators;

internal static class ProductionServicesConfigurator
{
    public static void Configure(WebHostBuilderContext context, IServiceCollection services)
    {
        services.AddSingleton<IValidateOptions<ControlPlaneOptions>, ControlPlaneOptionsValidator>();
        services.AddOptions<ControlPlaneOptions>()
            .Configure(options =>
            {
                var section = context.Configuration.GetSection(ControlPlaneOptions.SectionName);
                if (section.Exists())
                    section.Bind(options);
                else
                {
                    options.Authentication = "ConnectionString";
                    options.ConnectionString = context.Configuration["Source:ControlPlaneConnectionString"] ?? string.Empty;
                }
            })
            .ValidateOnStart();
        services.AddSingleton<PostgresConnectionFactory>(provider =>
            new(provider.GetRequiredService<IOptions<ControlPlaneOptions>>().Value));
        services.AddSingleton<IOperationRepository>(provider =>
            new PostgresOperationRepository(provider.GetRequiredService<PostgresConnectionFactory>().CreateConnection));
        services.AddSingleton<IExtractionCursorRepository>(provider =>
            new PostgresExtractionCursorRepository(provider.GetRequiredService<PostgresConnectionFactory>()));
        services.AddSingleton<RefreshStatusService>();
        services.AddOptions<JobTriggerOptions>()
            .Bind(context.Configuration.GetSection(JobTriggerOptions.SectionName))
            .Validate(options => !string.IsNullOrWhiteSpace(options.Namespace) && options.Sources.Count > 0,
                "JobTrigger Namespace and Sources are required.").ValidateOnStart();
        services.AddSingleton<IKubernetes>(_ =>
            KubernetesClientFactory.Create(KubernetesClientConfiguration.BuildDefaultConfig()));
        services.AddSingleton<IIngestionJobTrigger, KubernetesIngestionJobTrigger>();
    }
}
