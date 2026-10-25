using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using ProductionIngestor.Operations.Execution;

namespace ProductionIngestor.Worker.Configuration;

public static class ProductionTelemetryConfiguration
{
    public static void AddProductionTelemetry(this HostApplicationBuilder builder)
    {
        builder.Logging.AddSimpleConsole(console =>
        {
            console.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ ";
            console.UseUtcTimestamp = true;
        });
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeScopes = true;
            logging.IncludeFormattedMessage = true;
            logging.ParseStateValues = true;
        });

        var telemetry = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("cat-production-ingestor",
                serviceInstanceId: Environment.MachineName))
            .WithTracing(tracing => tracing.AddSource(ScheduledIngestionWorker.TelemetryName))
            .WithMetrics(metrics => metrics.AddMeter(ScheduledIngestionWorker.TelemetryName));

        if (builder.Configuration.GetValue<bool>("Telemetry:ExportEnabled"))
            telemetry.UseOtlpExporter();
    }
}