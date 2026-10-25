namespace CAT.Messaging.Contracts;

public sealed record TurbineIdentity(int WtgId, string WtgName);

public sealed record SnapshotScope
{
    public required string SnapshotId { get; init; }
    public required string StartDate { get; init; }
    public required string EndDateExclusive { get; init; }
    public required int[] ScopeItemIds { get; init; }
}

/// <summary>PascalCase wire contract for non-Production canonical event work. IDs are numeric; display names are separate.</summary>
public sealed record HarmonizationWorkV2
{
    public int SchemaVersion { get; init; } = 2;
    public required string WorkKind { get; init; }
    public required string ArtifactId { get; init; }
    public required string BatchId { get; init; }
    public required string SourceType { get; init; }
    public required string PayloadKind { get; init; }
    public required int WindfarmId { get; init; }
    public required string WindfarmName { get; init; }
    public string? DataDate { get; init; }
    public required long ExtractionToken { get; init; }
    public required string[] RawPaths { get; init; }
    public required string RunId { get; init; }
    public string? OperationId { get; init; }
    public string? TraceId { get; init; }
    public int[] ItemIds { get; init; } = [];
    public TurbineIdentity[] Turbines { get; init; } = [];
    public string[] EventIds { get; init; } = [];
    public SnapshotScope? Snapshot { get; init; }
    public required string OriginalMessageBody { get; init; }
}
