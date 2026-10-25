using Microsoft.Extensions.Options;
using CurtailmentIngestor.Adapters.Storage;
using CurtailmentIngestor.Configuration;
using CurtailmentIngestor.Interfaces;
using CurtailmentIngestor.Repositories;
using CurtailmentIngestor.Validators;

namespace CurtailmentIngestor.Worker.Configuration;

public static class CurtailmentSourceConfiguration
{
    public static IServiceCollection AddCurtailmentSource(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IValidateOptions<SourceOptions>, SourceOptionsValidator>();
        services.AddOptions<SourceOptions>()
            .Bind(configuration.GetSection(SourceOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton<IExtractionCursorRepository>(provider => new PostgresExtractionCursorRepository(
            provider.GetRequiredService<IOptions<SourceOptions>>().Value.ControlPlaneConnectionString));

        services.AddSingleton<IRawObjectStore>(provider =>
        {
            var source = provider.GetRequiredService<IOptions<SourceOptions>>().Value;
            return new AzureBlobRawObjectStore(source.BlobConnectionString, source.BlobContainerName);
        });

        // ICurtailmentSourceAdapter is deliberately unregistered: the real (DB table) adapter is
        // deferred until the concrete curtailment source table schema/indexing is confirmed with
        // the source-system owner.
        return services;
    }
}
