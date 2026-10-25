using OutageIngestor.Enums;

namespace OutageIngestor.Models;

public sealed record ScheduledIngestionResult(
    IReadOnlyList<ExtractionAttemptResultModel> Attempts, ExtractionSelectionStatusModel Selection, bool BudgetExhausted)
{
    public bool Succeeded => Attempts.All(attempt => attempt.Result.Outcome == RunOutcome.Success);
}
