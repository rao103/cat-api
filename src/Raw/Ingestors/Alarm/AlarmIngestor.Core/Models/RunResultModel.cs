using AlarmIngestor.Enums;

namespace AlarmIngestor.Models;

public sealed record RunResultModel(RunOutcome Outcome, int EventsWritten, string? Error = null);
