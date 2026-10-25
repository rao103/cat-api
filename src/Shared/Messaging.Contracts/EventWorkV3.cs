namespace CAT.Messaging.Contracts;

public static class EventReconciliationModes
{
    public const string ReceiptOnly = "ReceiptOnly";
    public const string Authoritative = "Authoritative";
}

/// <summary>Canonical event identity excludes WTG so a changed event can move between turbines.</summary>
public sealed record EventWorkMemberV3(
    string EventKey,
    string MemberKey,
    string EventId,
    int? WtgId,
    string? WtgName,
    string[] RawPaths);

public sealed record EventSnapshotScopeV3
{
    public required string Kind { get; init; }
    public required DateTimeOffset StartInclusiveUtc { get; init; }
    public required DateTimeOffset EndExclusiveUtc { get; init; }
    public int[] WtgIds { get; init; } = [];
}

/// <summary>Outage event input selection, not a calculated lifecycle footprint.</summary>
public sealed record EventWorkV3
{
    public int SchemaVersion { get; init; } = 3;
    public required string WorkKind { get; init; }
    public required string ArtifactId { get; init; }
    public required string BatchId { get; init; }
    public required string SourceType { get; init; }
    public int? PayloadSchemaVersion { get; init; }
    public required string PayloadKind { get; init; }
    public required int WindfarmId { get; init; }
    public required string WindfarmName { get; init; }
    public required long ExtractionToken { get; init; }
    public required string RunId { get; init; }
    public string? AttemptId { get; init; }
    public string? OperationId { get; init; }
    public string? TraceId { get; init; }
    public string? ExtractionMode { get; init; }
    public string? ManifestPath { get; init; }
    public bool IsFullSnapshot { get; init; }
    public string? ReconciliationMode { get; init; }
    public DateTimeOffset? SnapshotObservedAtUtc { get; init; }
    public DateTimeOffset? SourceVersionCutoffUtc { get; init; }
    public EventSnapshotScopeV3? SnapshotScope { get; init; }
    public EventWorkMemberV3[] Members { get; init; } = [];
    public string[] EventIds { get; init; } = [];
    public required string[] RawPaths { get; init; }
    public string[] ExpectedEventBatchIds { get; init; } = [];
    public required string OriginalMessageBody { get; init; }
}