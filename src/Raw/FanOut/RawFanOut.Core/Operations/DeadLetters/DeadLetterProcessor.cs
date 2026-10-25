using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Operations.DeadLetters;

public sealed class DeadLetterProcessor(
    IDeadLetterStore store, FanOutOptions options, ITelemetry<DeadLetterProcessor>? telemetry = null)
{
    private const int MaxErrorTextLength = 500;

    public async Task ProcessAsync(string queueUrl, QueueDelivery delivery, CancellationToken ct)
    {
        RawEnvelope? artifact = null;
        string? errorType = null, lastError = null;
        try { artifact = RawEnvelope.Parse(delivery.Body, options); }
        catch (Exception ex) when (ex is JsonException or InvalidDataException or InvalidOperationException or FormatException)
        {
            // The envelope failing validation is the only cause evidenced here; the processing failure that led to the DLQ is unknown.
            errorType = ex.GetType().FullName;
            lastError = ex.Message.Length > MaxErrorTextLength ? ex.Message[..MaxErrorTextLength] : ex.Message;
        }
        // Malformed payloads are persisted verbatim, not assigned invented business identifiers.
        var result = await store.RecordAsync(new DeadLetterArchiveRequest(queueUrl, delivery, artifact, errorType, lastError), ct);
        using var scope = telemetry?.BeginScope(new Dictionary<string, object?> { ["IncidentId"] = result.IncidentId });
        if (result.Created)
            telemetry?.Warning("Dead-letter incident archived.");
        else
            telemetry?.Information("Dead-letter message re-observed; existing incident retained.");
    }
}

public sealed class ReplayIncident(
    IDeadLetterStore store, IMessageProcessor processor, ITelemetry<ReplayIncident>? telemetry = null)
{
    public async Task ExecuteAsync(Guid id, CancellationToken ct)
    {
        var incident = await store.LoadAsync(id, ct);
        if (incident.Resolved) return;
        using var scope = telemetry?.BeginScope(new Dictionary<string, object?>
        {
            ["IncidentId"] = id,
            ["ArtifactId"] = incident.ArtifactId,
            ["SourceType"] = incident.SourceType,
            ["WindfarmId"] = incident.WindfarmId,
            ["ExtractionToken"] = incident.ExtractionToken,
            ["RunId"] = incident.RunId,
            ["CorrelationId"] = incident.TraceId
        });
        await processor.ProcessAsync(incident.Body, ct);
        ct.ThrowIfCancellationRequested();
        // Republishing is not recovery: resolution needs verified recovery or an explicit operator closure.
        await store.MarkReplayRequestedAsync(id, ct);
        telemetry?.Information("Incident replay requested; incident remains unresolved until closed.");
    }
}

public sealed class ResolveIncident(IDeadLetterStore store, ITelemetry<ResolveIncident>? telemetry = null)
{
    public async Task ExecuteAsync(ResolveIncidentRequest request, CancellationToken ct)
    {
        using var scope = telemetry?.BeginScope(new Dictionary<string, object?> { ["IncidentId"] = request.IncidentId });
        await store.ResolveAsync(request, ct);
        telemetry?.Information("Incident explicitly closed by an operator.");
    }
}
