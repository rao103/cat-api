namespace AlarmIngestor.Models;

/// <summary>
/// Only returned on full-batch success: AlarmBatchWriter throws InvalidDataException instead of
/// returning a result when any row is quarantined, so QuarantinedEvents is always empty here and
/// PublishedEvents always equals the full input batch. Callers still derive cursor advances and
/// open-alarm enrollment/removal from PublishedEvents only.
/// </summary>
public sealed record RawWriteResultModel(
    string WindfarmId,
    long ExtractionToken,
    string? BlobPath,
    IReadOnlyList<string>? ShardPaths,
    int RowCount,
    IReadOnlyList<QuarantinedEventModel> QuarantinedEvents,
    IReadOnlyList<AlarmEventModel> PublishedEvents);
