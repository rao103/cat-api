using Harmonizer.Core.Models;
namespace Harmonizer.Core.SourceTypes.Production.Models;

public sealed record ProductionWork(
    BatchIdentity Batch,
    WindfarmIdentity Windfarm,
    DateOnly DataDate,
    IReadOnlyList<TurbineIdentity> Turbines,
    IReadOnlyList<string> RawPaths,
    long ExtractionToken,
    string RequestId,
    string AttemptId,
    bool IsSnapshot,
    bool HasNoSourceRows,
    bool AllowRemovalReconciliation,
    string? SourceVersionEvidence,
    string OriginalMessageBody,
    string WireBody,
    string? TraceId)
{
    public ProductionIngestor.Contracts.ProductionExtractionReadyV1? ResolvedRefresh { get; init; }
    public OperationIdentity Operation { get; init; } = OperationIdentity.Untracked;
}
