namespace Harmonizer.Core.Models;

public sealed record ComponentLease(
    WtgDayKey Key, BatchIdentity Batch, long FenceToken, long Revision,
    string? CanonicalHash, string? SnapshotPath, string? ParquetPath,
    string? ManifestPath, string Status, Guid OwnerId);
public sealed record ComponentCommit(
    ComponentLease Lease, long Revision, string CanonicalHash, ComponentCandidate Candidate,
    int RecordCount, bool Complete, bool Changed)
{
    public OperationIdentity Operation { get; init; } = OperationIdentity.Untracked;
}
public sealed record QueueDelivery(string MessageId, string ReceiptHandle, string Body, int ReceiveCount);
public sealed record Notification(long Id, string Body, string? OperationId = null, string? ExecutionId = null);
public sealed record NotificationIntent(string DeduplicationKey, string Body, OperationIdentity Operation);
