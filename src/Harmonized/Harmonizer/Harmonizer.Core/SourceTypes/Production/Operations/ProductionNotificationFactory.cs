using System.Security.Cryptography;
using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;

namespace Harmonizer.Core.SourceTypes.Production.Operations;

/// <summary>Keeps operation correlation separate from component revisions and source content.</summary>
public sealed class ProductionNotificationFactory
{
    public IReadOnlyList<NotificationIntent> Create(ComponentCommit commit)
    {
        var invalidate = commit.Changed || commit.Lease.Status == "Failed";
        if (!invalidate && !commit.Operation.IsTracked)
        {
            return [];
        }

        string[] kinds = invalidate
            ? ["PackageCoordinationRequested", "ProductionSummaryInvalidated"]
            : ["PackageCoordinationRequested"];
        var suffix = commit.Operation.IsTracked ? ":operation:" + OperationHash(commit.Operation) : "";
        return kinds.Select(kind =>
        {
            var identity = $"{kind}:{commit.Lease.Key.WindfarmId}:{commit.Lease.Key.WtgId}:{commit.Lease.Key.DataDate:yyyy-MM-dd}:{commit.Revision}{suffix}";
            var body = JsonSerializer.Serialize(new
            {
                SchemaVersion = 1,
                MessageKind = kind,
                NotificationId = identity,
                commit.Operation.OperationId,
                commit.Operation.ExecutionId,
                SourceType = "Production",
                PipelineStage = "harmonizer",
                commit.Lease.Key.WindfarmId,
                WtgId = commit.Lease.Key.WtgId,
                commit.Lease.Key.DataDate,
                InputType = "Production",
                ComponentRevision = commit.Revision,
                Status = commit.Complete ? "Complete" : "Partial",
                commit.Candidate.ManifestPath
            }, HarmonizerJson.Options);
            return new NotificationIntent(identity, body, commit.Operation);
        }).ToArray();
    }

    private static string OperationHash(OperationIdentity operation) =>
        Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(
            new { operation.OperationId, operation.ExecutionId }, HarmonizerJson.Options)));
}
