namespace CAT.Messaging.Contracts;

// NEW contract: merge this file into the real shared project; do not replace it.
// PascalCase is intentional. BatchId is stable execution identity within its artifact.
public sealed record HarmonizationWorkV1(
    int SchemaVersion,
    string WorkKind,
    string ArtifactId,
    string BatchId,
    string SourceType,
    string WindfarmId,
    string? DataDate,
    long ExtractionToken,
    string[] RawPaths,
    string RunId,
    string? TraceId,
    string[] ItemIds,
    string[] EventIds,
    string OriginalMessageBody);

public static class HarmonizationWorkKinds
{
    public const string ProductionWtgDays = "ProductionWtgDays";
    public const string ProductionSnapshotReconciliation = "ProductionSnapshotReconciliation";
    public const string CanonicalEvents = "CanonicalEvents";
    public const string EventSnapshotReconciliation = "EventSnapshotReconciliation";
}
