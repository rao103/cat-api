namespace ProductionIngestor.Worker.Configuration;

public sealed record IngestionRequestInput(
    string? WindfarmId,
    string? RefreshWindfarmId,
    string? RefreshWindfarmName,
    string? RefreshDates,
    string? RefreshRunId,
    string? OperationId = null)
{
    // Presence of any refresh setting selects refresh mode.
    public bool IsRefresh =>
        RefreshWindfarmId is not null ||
        RefreshWindfarmName is not null ||
        RefreshDates is not null ||
        RefreshRunId is not null;

    public string? SelectedWindfarmId =>
        IsRefresh ? RefreshWindfarmId : WindfarmId;
}