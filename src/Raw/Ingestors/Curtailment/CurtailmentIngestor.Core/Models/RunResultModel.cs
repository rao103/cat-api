using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Operations.Execution;

namespace CurtailmentIngestor.Models;

/// <summary>Outcome of one bounded <see cref="CurtailmentIngestionRunner"/> pass.</summary>
public sealed record RunResultModel(
    RunOutcome Outcome,
    int EventsWritten,
    string? Error = null);
