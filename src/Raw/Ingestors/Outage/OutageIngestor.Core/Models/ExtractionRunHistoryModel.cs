namespace OutageIngestor.Models;

public sealed record ExtractionHistoryQueryModel(string RequestId);

public sealed record ExtractionRunHistoryModel(long AttemptId, string RequestId, int WindfarmId,
    string Mode, string Status, DateTimeOffset StartedAt, DateTimeOffset? CompletedAt,
    string? JobName, string? Error, DateTimeOffset? LeaseExpiresAt);
