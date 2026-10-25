using System.Globalization;

namespace ProductionIngestor.Operations.Execution.Changes;

/// <summary>
/// The composite keyset identity of one changed source row:
/// <c>(LastUpdatedUtc, ItemId, ProductionTimestampUtc)</c>, matching the adapter's ORDER BY.
/// </summary>
public sealed record SourceRowKey(DateTime LastUpdatedUtc, string ItemId, DateTime ProductionTimestampUtc)
{
    /// <summary>
    /// The committed tie-break identity as TEXT: <c>"{ItemId}|{ProductionTimestampUtc}"</c> in
    /// round-trip ISO-8601 UTC. Only meaningful together with the paired committed
    /// <see cref="LastUpdatedUtc"/>; never compared numerically/lexically across windows.
    /// </summary>
    public string ToCommittedIdText()
        => $"{ItemId}|{ProductionTimestampUtc.ToString("O", CultureInfo.InvariantCulture)}";
}

/// <summary>One bounded update-time (`LastUpdatedUtc`) calendar-month window to process, plus the execution identity/scope needed to query, write, and publish its parts.</summary>
public sealed record ExtractionWindowExecutionRequest(
    int WindfarmId,
    string WindfarmName,
    string SourceType,
    string SourceSystem,
    string RunId,
    long ExtractionToken,
    IReadOnlyList<string> ActiveTurbineNames,
    DateTime MinRetrieveDateUtc,
    int WindowNumber,
    DateTime WindowMinInclusiveUtc,
    DateTime WindowMaxInclusiveUtc,
    string? OperationId);

public enum WindowOutcome
{
    /// <summary>The window produced and published at least one changed row.</summary>
    Succeeded,

    /// <summary>The window's query range produced zero changed rows.</summary>
    NoData,

    /// <summary>The window failed to read, write, or publish; the caller must not advance past it.</summary>
    Failed,
}

/// <summary>
/// Result of processing one bounded window. Window tasks never touch <c>extraction_cursor</c>
/// themselves; the coordinator decides the whole-execution checkpoint from these results.
/// </summary>
public sealed record WindowProcessingResult(
    ExtractionWindowExecutionRequest Window,
    WindowOutcome Outcome,
    int RowCount,
    int PartCount,
    IReadOnlyCollection<DateOnly> PublishedDates,
    SourceRowKey? LastObservedRowKey,
    string? FailureMessage = null,
    Exception? FailureException = null);
