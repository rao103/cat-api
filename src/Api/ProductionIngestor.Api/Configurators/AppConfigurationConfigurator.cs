namespace ProductionIngestor.Api.Configurators;

internal static class AppConfigurationConfigurator
{
    public static IWebHostBuilder ConfigureAppConfiguration(this IWebHostBuilder builder) =>
        builder.ConfigureAppConfiguration((_, configurationBuilder) =>
        {
            // Mounted-secret overrides (matches O.WP.CAT.API convention).
            var externalSettingsPath = "/app/appsettings/appsettings.json";
            if (File.Exists(externalSettingsPath))
            {
                configurationBuilder.AddJsonFile(externalSettingsPath, optional: true, reloadOnChange: true);
            }
        });
}
