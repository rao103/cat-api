using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Repositories;
using ProductionIngestor.Validators;

namespace ProductionIngestor.Worker.Configuration;

public static class ProductionControlPlaneConfiguration
{
    public static IServiceCollection AddProductionControlPlane(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IValidateOptions<ControlPlaneOptions>, ControlPlaneOptionsValidator>();
        services.AddOptions<ControlPlaneOptions>()
            .Configure(options =>
            {
                var section = configuration.GetSection(ControlPlaneOptions.SectionName);
                if (section.Exists())
                    section.Bind(options);
                else
                {
                    options.Authentication = "ConnectionString";
                    options.ConnectionString = configuration["Source:ControlPlaneConnectionString"] ?? string.Empty;
                }
            })
            .ValidateOnStart();
        services.AddSingleton<PostgresConnectionFactory>(provider =>
            new(provider.GetRequiredService<IOptions<ControlPlaneOptions>>().Value));
        services.AddSingleton<IExtractionCursorRepository>(provider =>
            new PostgresExtractionCursorRepository(provider.GetRequiredService<PostgresConnectionFactory>()));
        services.AddSingleton<IExtractionSchedulingRepository, PostgresExtractionSchedulingRepository>();
        services.AddSingleton<IProductionScopeRepository>(provider =>
            new PostgresProductionScopeRepository(provider.GetRequiredService<PostgresConnectionFactory>()));
        return services;
    }
}