using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Observability;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Harmonizer.Worker.Observability;

public static class TelemetryConfiguration
{
    public static void AddHarmonizerTelemetry(this HostApplicationBuilder builder)
    {
        var settings = builder.Configuration.GetSection(TelemetryOptions.SectionName).Get<TelemetryOptions>()
            ?? new TelemetryOptions();
        settings.Validate(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT"));
        builder.Services.AddSingleton(settings);
        builder.Services.AddSingleton(typeof(ITelemetry<>), typeof(HarmonizerTelemetry<>));

        builder.Logging.ClearProviders();
        builder.Logging.Configure(options => options.ActivityTrackingOptions =
            ActivityTrackingOptions.TraceId | ActivityTrackingOptions.SpanId | ActivityTrackingOptions.ParentId);
        builder.Logging.AddJsonConsole(console =>
        {
            console.IncludeScopes = true;
            console.UseUtcTimestamp = true;
            console.TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";
        });
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeScopes = true;
            logging.IncludeFormattedMessage = true;
            logging.ParseStateValues = true;
        });

        var telemetry = builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService(
                settings.ServiceName,
                serviceVersion: settings.ServiceVersion,
                serviceInstanceId: builder.Configuration["HOSTNAME"] ?? Environment.MachineName))
            .WithTracing(tracing => tracing
                .AddSource(HarmonizerDiagnostics.Name)
                .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(settings.TraceSampleRatio))))
            .WithMetrics(metrics => metrics.AddMeter(HarmonizerDiagnostics.Name));

        // One cross-signal registration. The host owns and disposes all providers.
        // OTEL_EXPORTER_OTLP_* configuration supplies protocol and secret headers.
        if (settings.ExportEnabled)
        {
            telemetry.UseOtlpExporter();
        }
    }
}
