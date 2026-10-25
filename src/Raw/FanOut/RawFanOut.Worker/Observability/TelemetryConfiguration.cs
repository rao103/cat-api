using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using CAT.RawFanOut.Observability;
using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace CAT.RawFanOut.Worker.Observability;

public sealed class TelemetryOptions
{
    public string ServiceName { get; set; } = "cat-raw-fanout";
    public string ServiceVersion { get; set; } = "1.0.0";
    public bool ExportEnabled { get; set; }
    public double TraceSampleRatio { get; set; } = 1.0;

    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ServiceName) || string.IsNullOrWhiteSpace(ServiceVersion) ||
            !double.IsFinite(TraceSampleRatio) || TraceSampleRatio is < 0 or > 1)
            throw new ArgumentException("Invalid telemetry service metadata or sampling ratio.");

        if (ExportEnabled)
        {
            var endpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
            if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https") ||
                !string.IsNullOrEmpty(uri.UserInfo) || !string.IsNullOrEmpty(uri.Query))
                throw new ArgumentException("Export requires a valid OTEL_EXPORTER_OTLP_ENDPOINT without credentials or query parameters.");
        }
    }
}

public static class TelemetryConfiguration
{
    public static void AddFanOutTelemetry(this HostApplicationBuilder builder)
    {
        var settings = builder.Configuration.GetSection("Telemetry").Get<TelemetryOptions>() ?? new TelemetryOptions();
        settings.Validate();
        builder.Services.AddSingleton(settings);

        builder.Logging.ClearProviders();
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
                settings.ServiceName, serviceVersion: settings.ServiceVersion,
                serviceInstanceId: Environment.GetEnvironmentVariable("HOSTNAME") ?? Environment.MachineName))
            .WithTracing(tracing => tracing
                .AddSource(FanOutDiagnostics.Name)
                .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(settings.TraceSampleRatio))))
            .WithMetrics(metrics => metrics.AddMeter(FanOutDiagnostics.Name));

        // The host owns and disposes all providers. Use OTEL_EXPORTER_OTLP_* environment
        // variables for endpoint, protocol and secret headers; no backend credentials in code.
        if (settings.ExportEnabled)
            telemetry.UseOtlpExporter();
    }
}
