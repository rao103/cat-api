namespace Centralize.Core.Models;

public sealed record OperationModel(
    Guid OperationId, DateTimeOffset CreatedDateUtc, string CreatedBy,
    IReadOnlyList<ExtractionStatusModel> Runs, IReadOnlyList<StageStatusModel> Stages)
{
    public IReadOnlyList<SourceStatusModel> Sources => Runs
        .GroupBy(run => (run.WindfarmId, run.SourceType))
        .Select(group => group.MaxBy(run => run.RunId)!)
        .Select(run => new SourceStatusModel(run.WindfarmId, run.SourceType, run.Status, run.RunId)).ToArray();
}

public sealed record SourceStatusModel(int WindfarmId, string SourceType, string Status, long RunId);

public sealed record ExtractionStatusModel(
    long RunId, int WindfarmId, string SourceType, string ExecutionMode, string Status,
    DateOnly[] RequestedDates, DateTimeOffset? StartedUtc, DateTimeOffset? CompletedUtc,
    string? Error);

public sealed record StageStatusModel(
    string Stage, string Status, long Total, long Completed, long Failed);

public sealed record RefreshAttempt(
    long AttemptId, string RequestId, string WindfarmId, string Mode, string Status,
    DateTimeOffset? StartedAt, DateTimeOffset? CompletedAt, string? JobName,
    string? Error, DateOnly[] RequestedDates, DateTimeOffset? LeaseExpiresAt);
