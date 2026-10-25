using Microsoft.Extensions.Options;
using OutageIngestor.Configuration;
using OutageIngestor.Factories;
using OutageIngestor.Interfaces;
using OutageIngestor.Repositories;
using OutageIngestor.Validators;

namespace OutageIngestor.Worker.Configuration;

public static class OutageControlPlaneConfiguration
{
    public static IServiceCollection AddOutageControlPlane(this IServiceCollection services, IConfiguration configuration)
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
        services.AddSingleton<IExtractionSchedulingRepository>(provider =>
            new PostgresExtractionSchedulingRepository(provider.GetRequiredService<PostgresConnectionFactory>()));
        services.AddSingleton<IOutageScopeRepository>(provider =>
            new PostgresOutageScopeRepository(provider.GetRequiredService<PostgresConnectionFactory>()));
        return services;
    }
}
