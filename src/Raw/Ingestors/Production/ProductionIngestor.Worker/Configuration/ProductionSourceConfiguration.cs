using Microsoft.Extensions.Options;
using ProductionIngestor.Adapters.Source;
using ProductionIngestor.Configuration;
using ProductionIngestor.Factories;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Validators;

namespace ProductionIngestor.Worker.Configuration;

public static class ProductionSourceConfiguration
{
    public static IServiceCollection AddProductionSource(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddProductionControlPlane(configuration);
        services.AddSingleton<IValidateOptions<SqlSourceOptions>, SqlSourceOptionsValidator>();
        services.AddOptions<SqlSourceOptions>()
            .Bind(configuration)
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<SourceOptions>, SourceOptionsValidator>();
        services.AddOptions<SourceOptions>()
            .Bind(configuration.GetSection(SourceOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton<SqlProductionConnectionFactory>(provider => new(
            provider.GetRequiredService<IOptions<SqlSourceOptions>>().Value,
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.ConnectionString));
        services.AddSingleton<IProductionSourceAdapter>(provider => new SqlProductionSourceAdapter(
            provider.GetRequiredService<SqlProductionConnectionFactory>(),
            provider.GetRequiredService<IOptions<IngestionOptions>>().Value));
        return services;
    }
}