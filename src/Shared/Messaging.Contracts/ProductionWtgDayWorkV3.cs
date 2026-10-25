namespace CAT.Messaging.Contracts;

public sealed record ProductionTurbineIdentityV1(int WtgId, string WtgName);

public sealed record ProductionWtgDayWorkV3(
    int SchemaVersion,
    string WorkKind,
    string ArtifactId,
    string BatchId,
    string SourceType,
    int WindfarmId,
    string WindfarmName,
    string DataDate,
    string ExtractionMode,
    string PayloadKind,
    int PayloadSchemaVersion,
    string RequestId,
    string AttemptId,
    long ExtractionToken,
    string[] RawPaths,
    ProductionTurbineIdentityV1[] ItemTurbines,
    ProductionTurbineIdentityV1[] ScopeTurbines,
    bool IsExtractionReady,
    bool HasNoSourceRows,
    bool AllowRemovalReconciliation,
    string? SourceVersionEvidence,
    string OriginalMessageBody,
    string? TraceId)
{
    public string? OperationId { get; init; }
}
