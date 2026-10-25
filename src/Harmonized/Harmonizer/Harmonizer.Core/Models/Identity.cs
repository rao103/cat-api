namespace Harmonizer.Core.Models;

public sealed record WindfarmIdentity(int WindfarmId, string WindfarmName);
public sealed record TurbineIdentity(int WtgId, string WtgName);
public sealed record WtgDayKey(int WindfarmId, int WtgId, DateOnly DataDate, string InputType = "Production");
public sealed record ArtifactReference(string BlobPath, long ExtractionToken, string RequestId, string AttemptId,
    string? OperationId = null, string? ExecutionId = null);
public sealed record BatchIdentity(string ArtifactId, string BatchId);
