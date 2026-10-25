using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace OutageIngestor.Api.Configurators;

internal static class TelemetryConfigurator
{
    public static WebApplicationBuilder ConfigureTelemetry(this WebApplicationBuilder builder)
    {
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeScopes = true;
            logging.IncludeFormattedMessage = true;
            logging.ParseStateValues = true;
        });

        var telemetry = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("cat-outage-ingestor-api",
                serviceInstanceId: Environment.MachineName))
            .WithTracing(tracing => tracing.AddAspNetCoreInstrumentation())
            .WithMetrics(metrics => metrics.AddAspNetCoreInstrumentation());

        if (builder.Configuration.GetValue<bool>("Telemetry:ExportEnabled"))
            telemetry.UseOtlpExporter();

        return builder;
    }
}