using CAT.Messaging.Contracts;
using TurbineIdentity = Harmonizer.Core.Models.TurbineIdentity;
using Harmonizer.Core.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Models;

public sealed record OutageWork(EventWorkV3 Message, WindfarmIdentity Windfarm,
    IReadOnlyList<TurbineIdentity> Turbines, OperationIdentity Operation, string WireBody)
{
    public BatchIdentity Batch => new(Message.ArtifactId, Message.BatchId);
    public bool IsObservation => Message.WorkKind == "ArtifactObserved";
    public bool IsReconciliation => Message.WorkKind == "EventSnapshotReconciliation";
}

public sealed record OutageOrigin(string RawPath, long ExtractionToken, string RunId, string? AttemptId,
    string? OperationId, string? ExecutionId, DateTime? SourceWatermarkUtc = null);

public sealed record CanonicalOutage(OutageState State, long EventRevision, IReadOnlyList<OutageOrigin> Origins);
public sealed record EventMonthReference(string PartitionKey, long Revision, long Attempt,
    string ParquetPath, string SnapshotPath, string LineagePath);
public sealed record EventMonthSnapshot(int SchemaVersion, IReadOnlyList<CanonicalOutage> Events);

// A scope exclusion only applies to this exact event revision. It is not a global tombstone.
public sealed record OutageScopeExclusion(string EventId, long EventRevision, int WtgId,
    DateTime StartInclusiveUtc, DateTime EndExclusiveUtc, long CaptureToken = 0);
public sealed record OutageSnapshotCapture(int WtgId, DateTime StartInclusiveUtc, DateTime EndExclusiveUtc,
    long ExtractionToken, string ArtifactId, string BatchId)
{
    public DateTime? SourceVersionCutoffUtc { get; init; }
}
public sealed record OutageCatalog(int SchemaVersion, IReadOnlyList<EventMonthReference> Months,
    IReadOnlyList<OutageScopeExclusion> Exclusions, IReadOnlyList<OutageSnapshotCapture>? Captures = null)
{
    public static OutageCatalog Empty { get; } = new(1, [], []);
}

public sealed record OutageCatalogLease(WindfarmIdentity Windfarm, long Token, Guid Owner,
    long Revision, string? CatalogPath);
public sealed record OutageRange(int WtgId, string WtgName, DateOnly StartInclusive, DateOnly EndInclusive);
public sealed record OutageLifecyclePlan(string WorkId, string? EventId, long Generation,
    IReadOnlyList<OutageRange> Pending, IReadOnlyList<OutageRange> Footprint,
    OperationIdentity Operation, string TriggerKind, BatchIdentity? OriginBatch,
    IReadOnlyList<string>? RawPaths = null, string? OriginalMessageBody = null);
public sealed record OutageLifecycleState(string WorkId, string? EventId, string? PartitionKey,
    long EventRevision, string PlanPath, bool NeedsContinuation, DateOnly? ContinuedThrough, DateTime DueUtc);
public sealed record OutageLifecycleUpdate(OutageLifecycleState State, OutageLifecyclePlan Plan);
public sealed record OutagePublication(OutageCatalog Catalog, string CatalogPath,
    IReadOnlyList<EventMonthReference> ChangedMonths, IReadOnlyList<OutageLifecycleUpdate> Lifecycle,
    IReadOnlyList<string> AppliedMemberKeys);
public sealed record OutageDailyCandidate(WtgDayKey Key, long PreviousRevision, long Revision,
    string CanonicalHash, string ParquetPath, string SnapshotPath, string LineagePath,
    int RecordCount, OperationIdentity Operation);
public sealed record OutageDayReference(OutageDaySegment Segment, long EventRevision, EventMonthReference Canonical,
    IReadOnlyList<OutageOrigin>? Origins = null);
public sealed record OutageDayEvidence(CanonicalOutage Event, EventMonthReference Canonical);
