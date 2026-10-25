using ProductionIngestor.Models;

namespace ProductionIngestor.Operations.Execution;

/// <summary>The immutable identity and initial checkpoint of an acquired run.</summary>
internal sealed record IngestionRunContext(
    IngestionRequestModel Request,
    ExtractionLeaseModel Lease,
    ExtractionRunModel Run)
{
    public CursorPosition InitialPosition => new(Lease.LastUpdatedUtc, Lease.LastCommittedId);
}

/// <summary>A checkpoint's timestamp and tie-break identity must always travel together.</summary>
internal sealed record CursorPosition(DateTime UpdatedUtc, string CommittedId);

/// <summary>
/// Shared with cleanup so cancellation cannot discard an already committed checkpoint.
/// Only the processing path mutates this state; lease renewal does not access it.
/// </summary>
internal sealed class IngestionRunProgress(CursorPosition initialPosition)
{
    private readonly List<DateOnly> _publishedDates = [];

    public CursorPosition CommittedPosition { get; private set; } = initialPosition;
    public bool IsFinalized { get; private set; }
    public IReadOnlyList<DateOnly> PublishedDates => _publishedDates;

    public void RecordPublication(DateOnly date) => _publishedDates.Add(date);

    public void RecordCommit(CursorPosition position) => CommittedPosition = position;

    public void MarkFinalized() => IsFinalized = true;
}