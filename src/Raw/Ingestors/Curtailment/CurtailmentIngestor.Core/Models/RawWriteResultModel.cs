namespace CurtailmentIngestor.Models;

/// <summary>An event rejected at write time, with an auditable reason (never silently dropped).</summary>
public sealed record QuarantinedEventModel(string EventId, string Reason);

/// <summary>Result of writing one extraction run's Raw batch.</summary>
public sealed record RawWriteResultModel(
    string WindfarmId,
    long ExtractionToken,
    string? BlobPath,
    IReadOnlyList<string>? ShardPaths,
    int RowCount,
    IReadOnlyList<QuarantinedEventModel> QuarantinedEvents);
