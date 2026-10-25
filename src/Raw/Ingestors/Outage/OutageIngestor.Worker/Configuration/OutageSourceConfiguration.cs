using Microsoft.Extensions.Options;
using OutageIngestor.Adapters.Source;
using OutageIngestor.Configuration;
using OutageIngestor.Factories;
using OutageIngestor.Interfaces;
using OutageIngestor.Validators;

namespace OutageIngestor.Worker.Configuration;

public static class OutageSourceConfiguration
{
    public static IServiceCollection AddOutageSource(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOutageControlPlane(configuration);
        services.AddSingleton<IValidateOptions<SqlSourceOptions>, SqlSourceOptionsValidator>();
        services.AddOptions<SqlSourceOptions>()
            .Bind(configuration)
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<SourceOptions>, SourceOptionsValidator>();
        services.AddOptions<SourceOptions>()
            .Bind(configuration.GetSection(SourceOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton<SqlOutageConnectionFactory>(provider => new(
            provider.GetRequiredService<IOptions<SqlSourceOptions>>().Value,
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.ConnectionString));
        services.AddSingleton<IOutageSourceAdapter>(provider => new SqlOutageSourceAdapter(
            provider.GetRequiredService<SqlOutageConnectionFactory>(),
            provider.GetRequiredService<IOptions<IngestionOptions>>().Value.SourceQueryBatchSize));

        return services;
    }
}
