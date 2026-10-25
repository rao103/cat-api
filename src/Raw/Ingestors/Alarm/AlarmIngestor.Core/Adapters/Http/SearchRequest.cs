namespace AlarmIngestor.Adapters.Http;

/// <summary>
/// Wire shapes for the deployed Alarm API's POST /sdh/alarm/api/search endpoint. Field names
/// match the legacy AlarmsRepository filters exactly (WtgRdsppId, AlarmCode, AlarmId, TimeOnUtc,
/// AlarmType, AlarmState) — LastUpdatedUtc is deliberately never used; a live probe confirmed
/// the deployed API returns HTTP 400 for it in both filters and orderFields (see README "Alarm
/// Source Contract"). The response schema is NOT published in the deployed Swagger and has not
/// been verified live beyond a bounded top=1 request that timed out before returning a body;
/// field names below (id, wtgRdsppId, alarmCode, alarmText, timeOnUtc, timeOffUtc, alarmState,
/// alarmType) are carried over from the legacy dynamic-typed caller and must be confirmed
/// against a real response before this adapter is treated as production-verified.
/// </summary>
public sealed class SearchFilterDetail
{
    public required string Field { get; init; }
    public required string Operator { get; init; }
    public required string Value { get; init; }
}

public sealed class SearchOrderField
{
    public required string Field { get; init; }
    public required string Direction { get; init; }
}

public sealed class SearchRequest
{
    public required List<SearchFilterDetail> Filters { get; init; }
    public required string LogicOperator { get; init; }
    public List<SearchOrderField>? OrderFields { get; init; }
    public required int Top { get; init; }
    public required int Offset { get; init; }
}
