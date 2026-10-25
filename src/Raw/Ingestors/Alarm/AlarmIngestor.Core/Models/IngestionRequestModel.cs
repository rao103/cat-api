using AlarmIngestor.Enums;

namespace AlarmIngestor.Models;

/// <summary>
/// One bounded unit of work for <see cref="AlarmIngestor.Operations.Execution.AlarmIngestionRunner"/>.
/// Historical carries an explicit start/end UTC range and never advances the discovery cursor.
/// </summary>
public sealed record IngestionRequestModel(
    string WindfarmId,
    string SourceType,
    IngestionMode Mode,
    DateTime? HistoricalStartUtc = null,
    DateTime? HistoricalEndUtc = null,
    string? RunId = null)
{
    public string? OperationId { get; init; }
}
