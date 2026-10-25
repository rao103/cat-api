namespace OutageIngestor.Worker.Configuration;

/// <summary>Raw environment/config inputs before parsing into a validated <see cref="Models.IngestionRequestModel"/>.
/// Refresh always requires an explicit event-time scope (<see cref="RefreshScopeStartUtc"/>/
/// <see cref="RefreshScopeEndUtc"/>) — there is no unscoped/full-table refresh mode. Scheduled
/// incremental requests obtain both their integer ID and name from the claimed database lease.</summary>
public sealed record IngestionRequestInput(
    string? WindfarmId,
    string? RefreshWindfarmId,
    string? RefreshWindfarmName,
    string? RefreshRunId,
    string? RefreshScopeStartUtc = null,
    string? RefreshScopeEndUtc = null,
    string? OperationId = null)
{

    public bool IsRefresh => 
		RefreshWindfarmId is not null ||
		RefreshWindfarmName is not null ||
		RefreshRunId is not null ||
		RefreshScopeStartUtc is not null ||
		RefreshScopeEndUtc is not null;

    public string? SelectedWindfarmId => 
		IsRefresh ? RefreshWindfarmId : WindfarmId;
}