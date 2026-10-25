using Microsoft.Extensions.Options;
using AlarmIngestor.Adapters.Http;
using AlarmIngestor.Adapters.Storage;
using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Repositories;
using AlarmIngestor.Validators;

namespace AlarmIngestor.Worker.Configuration;

public static class AlarmSourceConfiguration
{
    public static IServiceCollection AddAlarmSource(this IServiceCollection services, IConfiguration configuration)
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

        services.AddHttpClient<IAlarmSourceAdapter, AlarmHttpSourceAdapter>((provider, client) =>
        {
            var source = provider.GetRequiredService<IOptions<SourceOptions>>().Value;
            var ingestion = provider.GetRequiredService<IOptions<IngestionOptions>>().Value;
            client.BaseAddress = new Uri(source.ApiBaseUri.TrimEnd('/') + "/");
            client.Timeout = ingestion.RequestTimeout;
        });

        return services;
    }
}
