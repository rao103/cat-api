using System;

namespace CAT.RawFanOut.Models;

public sealed record DeadLetterIncident(Guid IncidentId, string Body, bool Resolved)
{
    public string? ArtifactId { get; init; }
    public string? SourceType { get; init; }
    public int? WindfarmId { get; init; }
    public long? ExtractionToken { get; init; }
    public string? RunId { get; init; }
    public string? TraceId { get; init; }
}