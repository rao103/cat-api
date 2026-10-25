namespace ProductionIngestor.Models;

/// <summary>A row rejected at write time, with an auditable reason (never silently dropped).</summary>
public sealed record QuarantinedRowModel(
    string ItemId,
    DateTime ProductionTimestampUtc,
    string Reason);

/// <summary>Result of writing one Windfarm-Day's Raw artifact.</summary>
public sealed record RawWriteResultModel(
    string WindfarmId,
    DateOnly ProductionDate,
    long ExtractionToken,
    string? BlobPath,
    IReadOnlyList<string>? ShardPaths,
    int RowCount,
    IReadOnlyList<QuarantinedRowModel> QuarantinedRows);
