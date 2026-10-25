namespace CAT.Messaging.Contracts;

public sealed record RawSnapshotIntervalV1(DateTime StartInclusiveUtc, DateTime EndExclusiveUtc);
public sealed record RawSnapshotPartV1(string BlobPath, int QueryWindowNumber, int PartNumber, long RowCount);
public sealed record RawSnapshotWindowV1(int QueryWindowNumber, DateTime QueryStartInclusiveUtc,
    DateTime QueryEndExclusiveUtc, long RowCount, int ArtifactCount, bool Completed);

/// <summary>A complete refresh declaration. Publish its pointer only after all reads and durable writes succeed.</summary>
public sealed class RawSnapshotManifestV1
{
    public const int CurrentSchemaVersion = 1;
    public int SchemaVersion { get; init; } = CurrentSchemaVersion;
    public string ManifestKind { get; init; } = "RawSnapshot";
    public required RawArtifactAvailableV2 Envelope { get; init; }
    /// <summary>Observation time, NOT a source revision or an ordering key for replacing downstream data.</summary>
    public required DateTime SnapshotAsOfUtc { get; init; }
    public required IReadOnlyList<RawSnapshotIntervalV1> RequestedIntervals { get; init; }
    /// <summary>Authoritative turbine names (RDSPP identifiers), not internal integer IDs.</summary>
    public required IReadOnlyList<string> ScopeWtgNames { get; init; }
    public required DateTime MinRetrieveDateUtc { get; init; }
    public required IReadOnlyList<RawSnapshotWindowV1> CompletedWindows { get; init; }
    public required IReadOnlyList<RawSnapshotPartV1> Parts { get; init; }
    public long TotalRowCount => Parts.Sum(part => part.RowCount);
    public bool IsEmptyResult => TotalRowCount == 0;

    public void Validate(bool allowLegacyPayloadSchema = false)
    {
        if (Envelope is null) throw new InvalidDataException("Missing manifest envelope.");
        Envelope.Validate(allowLegacyPayloadSchema);
        if (SchemaVersion != CurrentSchemaVersion || ManifestKind != "RawSnapshot" || Envelope.ExtractionMode != "Refresh" ||
            SnapshotAsOfUtc.Kind != DateTimeKind.Utc || MinRetrieveDateUtc.Kind != DateTimeKind.Utc ||
            RequestedIntervals is null || RequestedIntervals.Count == 0 || ScopeWtgNames is null ||
            ScopeWtgNames.Any(string.IsNullOrWhiteSpace) || ScopeWtgNames.Distinct(StringComparer.Ordinal).Count() != ScopeWtgNames.Count ||
            CompletedWindows is null || Parts is null || CompletedWindows.Count != RequestedIntervals.Count ||
            RequestedIntervals.Any(interval => interval is null) || CompletedWindows.Any(window => window is null) || Parts.Any(part => part is null))
            throw new InvalidDataException("Invalid snapshot manifest scope.");
        foreach (var part in Parts) RawArtifactConventions.ValidatePath(part.BlobPath);
        if (ScopeWtgNames.Count == 0 && Parts.Count != 0) throw new InvalidDataException("An empty turbine scope cannot contain parts.");
        var ordered = RequestedIntervals.OrderBy(interval => interval.StartInclusiveUtc).ToArray();
        for (var i = 0; i < ordered.Length; i++)
        {
            var interval = ordered[i];
            if (interval.StartInclusiveUtc.Kind != DateTimeKind.Utc || interval.EndExclusiveUtc.Kind != DateTimeKind.Utc ||
                interval.EndExclusiveUtc <= interval.StartInclusiveUtc ||
                (i > 0 && ordered[i - 1].EndExclusiveUtc > interval.StartInclusiveUtc))
                throw new InvalidDataException("Snapshot intervals must be UTC, nonempty and nonoverlapping.");
        }
        if (ordered[0].StartInclusiveUtc != Envelope.QueryStartInclusiveUtc || ordered[^1].EndExclusiveUtc != Envelope.QueryEndExclusiveUtc ||
            (Envelope.SourceType == "Outage" && ordered.Length != 1) ||
            Parts.Any(part => string.IsNullOrWhiteSpace(part.BlobPath) || part.RowCount <= 0 || part.PartNumber <= 0 || part.QueryWindowNumber <= 0) ||
            Parts.Select(part => part.BlobPath).Distinct(StringComparer.Ordinal).Count() != Parts.Count ||
            Parts.Select(part => (part.QueryWindowNumber, part.PartNumber)).Distinct().Count() != Parts.Count ||
            CompletedWindows.Select(window => window.QueryWindowNumber).Distinct().Count() != CompletedWindows.Count ||
            TotalRowCount != Envelope.RowCount)
            throw new InvalidDataException("Snapshot parts, totals or bounds are inconsistent.");
        var windows = CompletedWindows.OrderBy(window => window.QueryStartInclusiveUtc).ToArray();
        var partsByWindow = Parts.ToLookup(part => part.QueryWindowNumber);
        for (var i = 0; i < windows.Length; i++)
        {
            var window = windows[i];
            var parts = partsByWindow[window.QueryWindowNumber].OrderBy(part => part.PartNumber).ToArray();
            if (!window.Completed || window.QueryWindowNumber != i + 1 || window.QueryStartInclusiveUtc.Kind != DateTimeKind.Utc ||
                window.QueryEndExclusiveUtc.Kind != DateTimeKind.Utc || window.QueryStartInclusiveUtc != ordered[i].StartInclusiveUtc ||
                window.QueryEndExclusiveUtc != ordered[i].EndExclusiveUtc || window.RowCount != parts.Sum(part => part.RowCount) ||
                window.ArtifactCount != parts.Length || !parts.Select(part => part.PartNumber).SequenceEqual(Enumerable.Range(1, parts.Length)))
                throw new InvalidDataException("Snapshot contains an incomplete or inconsistent window.");
        }
        if (Parts.Any(part => part.QueryWindowNumber > windows.Length))
            throw new InvalidDataException("Snapshot part refers to an unknown window.");
    }
}
