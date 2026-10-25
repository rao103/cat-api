namespace Harmonizer.Worker.Observability;

public sealed class TelemetryOptions
{
    public const string SectionName = "Telemetry";

    public string ServiceName { get; init; } = "cat-harmonizer";
    public string ServiceVersion { get; init; } = "1.0.0";
    public bool ExportEnabled { get; init; }
    public double TraceSampleRatio { get; init; } = 1.0;

    public void Validate(string? otlpEndpoint)
    {
        if (string.IsNullOrWhiteSpace(ServiceName) || string.IsNullOrWhiteSpace(ServiceVersion))
        {
            throw new ArgumentException("Telemetry service name and version must not be blank.");
        }

        if (!double.IsFinite(TraceSampleRatio) || TraceSampleRatio is < 0 or > 1)
        {
            throw new ArgumentException("Telemetry trace sample ratio must be finite and between zero and one.");
        }

        if (ExportEnabled &&
            (!Uri.TryCreate(otlpEndpoint, UriKind.Absolute, out var endpoint) ||
             endpoint.Scheme is not ("http" or "https") ||
             string.IsNullOrWhiteSpace(endpoint.Host) ||
             !string.IsNullOrEmpty(endpoint.UserInfo) ||
             !string.IsNullOrEmpty(endpoint.Query) ||
             !string.IsNullOrEmpty(endpoint.Fragment)))
        {
            throw new ArgumentException(
                "Telemetry export requires OTEL_EXPORTER_OTLP_ENDPOINT with an HTTP(S) URI without credentials, query parameters or a fragment.");
        }
    }
}
