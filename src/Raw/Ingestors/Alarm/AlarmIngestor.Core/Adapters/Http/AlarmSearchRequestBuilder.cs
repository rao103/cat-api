using System.Text;
using System.Text.Json;
using AlarmIngestor.Models;

namespace AlarmIngestor.Adapters.Http;

/// <summary>
/// Builds the legacy-equivalent search request bodies and parses the response. Requests Alarm
/// and Warning records in Good and Deleted states (Section "Discovery requests Alarm and
/// Warning records"). Never adds a LastUpdatedUtc filter or order field. Response parsing is
/// defensive (top-level array or a wrapping "items"/"result"/"results" array) because the
/// deployed Swagger publishes no response schema and a successful live response has not been
/// observed; a missing/unrecognized shape or missing required identity fields throws rather
/// than silently returning an empty page (Section "Empty HTTP bodies... are failures").
/// </summary>
public static class AlarmSearchRequestBuilder
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    public static SearchRequest ByAlarmIdRange(
        string wtgRdsppId, long afterAlarmId, long upToAlarmId, DateTime minRetrievalDateUtc,
        IReadOnlyList<string> alarmCodes, int pageSize, int offset)
    {
        var filters = new List<SearchFilterDetail>
        {
            new() { Field = "WtgRdsppId", Operator = "Eq", Value = wtgRdsppId },
            new() { Field = "AlarmCode", Operator = "In", Value = string.Join(",", alarmCodes) },
            new() { Field = "AlarmId", Operator = "Gt", Value = afterAlarmId.ToString() },
            new() { Field = "AlarmId", Operator = "Lte", Value = upToAlarmId.ToString() },
            new() { Field = "TimeOnUtc", Operator = "Gt", Value = minRetrievalDateUtc.ToString("yyyy-MM-ddTHH:mm:ssZ") },
            new() { Field = "AlarmType", Operator = "In", Value = "Alarm,Warning" },
            new() { Field = "AlarmState", Operator = "In", Value = "Good,Deleted" },
        };

        return new SearchRequest
        {
            Filters = filters,
            LogicOperator = "And",
            OrderFields = [new SearchOrderField { Field = "AlarmId", Direction = "Asc" }],
            Top = pageSize,
            Offset = offset,
        };
    }

    public static SearchRequest MaxAlarmId(string wtgRdsppId, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes)
    {
        var filters = new List<SearchFilterDetail>
        {
            new() { Field = "WtgRdsppId", Operator = "Eq", Value = wtgRdsppId },
            new() { Field = "AlarmCode", Operator = "In", Value = string.Join(",", alarmCodes) },
            new() { Field = "TimeOnUtc", Operator = "Gt", Value = minRetrievalDateUtc.ToString("yyyy-MM-ddTHH:mm:ssZ") },
            new() { Field = "AlarmType", Operator = "In", Value = "Alarm,Warning" },
            new() { Field = "AlarmState", Operator = "In", Value = "Good,Deleted" },
        };

        return new SearchRequest
        {
            Filters = filters,
            LogicOperator = "And",
            OrderFields = [new SearchOrderField { Field = "AlarmId", Direction = "Desc" }],
            Top = 1,
            Offset = 0,
        };
    }

    public static SearchRequest ByAlarmIds(string wtgRdsppId, IReadOnlyList<long> alarmIds, int pageSize, int offset)
    {
        var filters = new List<SearchFilterDetail>
        {
            new() { Field = "WtgRdsppId", Operator = "Eq", Value = wtgRdsppId },
            new() { Field = "AlarmState", Operator = "In", Value = "Good,Deleted" },
            new() { Field = "AlarmId", Operator = "In", Value = string.Join(",", alarmIds) },
        };

        return new SearchRequest { Filters = filters, LogicOperator = "And", Top = pageSize, Offset = offset };
    }

    public static SearchRequest BetweenDates(
        string wtgRdsppId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string> alarmCodes, int pageSize, int offset)
    {
        var filters = new List<SearchFilterDetail>
        {
            new() { Field = "WtgRdsppId", Operator = "Eq", Value = wtgRdsppId },
            new() { Field = "AlarmCode", Operator = "In", Value = string.Join(",", alarmCodes) },
            new() { Field = "TimeOnUtc", Operator = "Gte", Value = startUtc.ToString("yyyy-MM-ddTHH:mm:ssZ") },
            new() { Field = "TimeOnUtc", Operator = "Lte", Value = endUtc.ToString("yyyy-MM-ddTHH:mm:ssZ") },
            new() { Field = "AlarmType", Operator = "In", Value = "Alarm,Warning" },
            new() { Field = "AlarmState", Operator = "In", Value = "Good,Deleted" },
        };

        return new SearchRequest
        {
            Filters = filters,
            LogicOperator = "And",
            OrderFields = [new SearchOrderField { Field = "AlarmId", Direction = "Asc" }],
            Top = pageSize,
            Offset = offset,
        };
    }

    public static StringContent Serialize(SearchRequest request)
        => new(JsonSerializer.Serialize(request, SerializerOptions), Encoding.UTF8, "application/json");

    public static IReadOnlyList<AlarmEventModel> ParseAlarms(
        string json, string windfarmId, string turbineRdspp, string itemId, DateTime observedAtUtc, ref int sequence)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new InvalidOperationException("Alarm search response body was empty.");
        }

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        var items = root.ValueKind switch
        {
            JsonValueKind.Array => root,
            JsonValueKind.Object when TryGetArrayProperty(root, out var array) => array,
            _ => throw new InvalidOperationException($"Unrecognized alarm search response shape: {root.ValueKind}."),
        };

        var results = new List<AlarmEventModel>();
        foreach (var item in items.EnumerateArray())
        {
            results.Add(ParseAlarm(item, windfarmId, turbineRdspp, itemId, observedAtUtc, sequence++));
        }

        return results;
    }

    private static bool TryGetArrayProperty(JsonElement root, out JsonElement array)
    {
        foreach (var propertyName in new[] { "items", "result", "results", "data" })
        {
            if (root.TryGetProperty(propertyName, out var candidate) && candidate.ValueKind == JsonValueKind.Array)
            {
                array = candidate;
                return true;
            }
        }

        array = default;
        return false;
    }

    private static AlarmEventModel ParseAlarm(
        JsonElement item, string windfarmId, string turbineRdspp, string itemId, DateTime observedAtUtc, int sequence)
    {
        if (!TryGetProperty(item, out var idElement, "id", "alarmId", "AlarmId")
            || !idElement.TryGetInt64(out var alarmId))
        {
            throw new InvalidOperationException("Alarm search response item is missing a numeric alarm id.");
        }

        if (!TryGetProperty(item, out var wtgElement, "wtgRdsppId", "WtgRdsppId"))
        {
            throw new InvalidOperationException($"Alarm {alarmId} response item is missing wtgRdsppId.");
        }

        var responseTurbine = wtgElement.ValueKind == JsonValueKind.Number
            ? wtgElement.GetRawText()
            : wtgElement.GetString() ?? string.Empty;
        if (!string.Equals(responseTurbine, turbineRdspp, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Wrong windturbine with rdspp: {turbineRdspp}, alarm turbine id: {responseTurbine}.");
        }

        if (!TryGetProperty(item, out var startElement, "timeOnUtc", "TimeOnUtc")
            || !startElement.TryGetDateTime(out var start))
        {
            throw new InvalidOperationException($"Alarm {alarmId} response item is missing/malformed timeOnUtc.");
        }

        DateTime? end = null;
        if (TryGetProperty(item, out var endElement, "timeOffUtc", "TimeOffUtc")
            && endElement.ValueKind != JsonValueKind.Null && endElement.TryGetDateTime(out var parsedEnd))
        {
            end = DateTime.SpecifyKind(parsedEnd, DateTimeKind.Utc);
        }

        var alarmCode = TryGetProperty(item, out var codeElement, "alarmCode", "AlarmCode")
            ? codeElement.ToString() : string.Empty;
        var alarmText = TryGetProperty(item, out var textElement, "alarmText", "AlarmText")
            ? textElement.GetString() : null;
        var alarmType = TryGetProperty(item, out var typeElement, "alarmType", "AlarmType")
            ? typeElement.GetString() ?? "Alarm" : "Alarm";
        var alarmState = TryGetProperty(item, out var stateElement, "alarmState", "AlarmState")
            ? stateElement.GetString() ?? "Good" : "Good";

        return new AlarmEventModel(
            windfarmId, turbineRdspp, itemId, alarmId, alarmCode, alarmText,
            DateTime.SpecifyKind(start, DateTimeKind.Utc), end, alarmType, alarmState,
            IsTombstone: string.Equals(alarmState, "Deleted", StringComparison.OrdinalIgnoreCase),
            observedAtUtc, sequence);
    }

    private static bool TryGetProperty(JsonElement element, out JsonElement value, params string[] names)
    {
        foreach (var name in names)
        {
            if (element.TryGetProperty(name, out value))
            {
                return true;
            }
        }

        value = default;
        return false;
    }
}
