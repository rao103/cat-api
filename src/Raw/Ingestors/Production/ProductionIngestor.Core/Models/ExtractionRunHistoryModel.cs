namespace ProductionIngestor.Models;

public sealed record ExtractionHistoryQueryModel(string RequestId);

public sealed record ExtractionRunHistoryModel(long AttemptId, string RequestId, string WindfarmId,
    string Mode, string Status, DateTimeOffset StartedAt, DateTimeOffset? CompletedAt,
    string? JobName, string? Error, DateOnly[] RequestedDates, DateTimeOffset? LeaseExpiresAt);