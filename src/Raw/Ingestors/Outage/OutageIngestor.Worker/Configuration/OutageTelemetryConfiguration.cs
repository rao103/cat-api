using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OutageIngestor.Operations.Execution;

namespace OutageIngestor.Worker.Configuration;

public static class OutageTelemetryConfiguration
{
    public static void AddOutageTelemetry(this HostApplicationBuilder builder)
    {
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeScopes = true;
            logging.IncludeFormattedMessage = true;
            logging.ParseStateValues = true;
        });

        var telemetry = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("cat-outage-ingestor",
                serviceInstanceId: Environment.MachineName))
            .WithTracing(tracing => tracing.AddSource(ScheduledIngestionWorker.TelemetryName))
            .WithMetrics(metrics => metrics.AddMeter(ScheduledIngestionWorker.TelemetryName));

        if (builder.Configuration.GetValue<bool>("Telemetry:ExportEnabled"))
            telemetry.UseOtlpExporter();
    }
}
