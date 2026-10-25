using Serilog;

namespace ProductionIngestor.Api.Configurators;

internal static class LoggingConfigurator
{
    public static IWebHostBuilder ConfigureLogging(this IWebHostBuilder builder) =>
        builder.ConfigureLogging((_, loggingBuilder) =>
        {
            loggingBuilder.ClearProviders();

            var logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();

            loggingBuilder.AddSerilog(logger, dispose: true);
        });
}
