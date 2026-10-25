using AlarmIngestor.Adapters.Http;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="AlarmSearchRequestBuilder"/>: never emits a LastUpdatedUtc filter or order field
/// (the deployed API rejects both), preserves nullable end times, treats an empty body or
/// unrecognized shape as a failure rather than an empty page, and rejects a response whose
/// WtgRdsppId does not match the requested turbine.
/// </summary>
public sealed class AlarmSearchRequestBuilderTests
{
    private static readonly DateTime MinRetrievalDate = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ByAlarmIdRangeNeverIncludesLastUpdatedUtc()
    {
        var request = AlarmSearchRequestBuilder.ByAlarmIdRange("RDSPP-1", 0, 100, MinRetrievalDate, ["100"], 500, 0);

        Assert.DoesNotContain(request.Filters, f => f.Field == "LastUpdatedUtc");
        Assert.DoesNotContain(request.OrderFields ?? [], f => f.Field == "LastUpdatedUtc");
    }

    [Fact]
    public void MaxAlarmIdNeverIncludesLastUpdatedUtc()
    {
        var request = AlarmSearchRequestBuilder.MaxAlarmId("RDSPP-1", MinRetrievalDate, ["100"]);

        Assert.DoesNotContain(request.Filters, f => f.Field == "LastUpdatedUtc");
        Assert.DoesNotContain(request.OrderFields ?? [], f => f.Field == "LastUpdatedUtc");
    }

    [Fact]
    public void ParseAlarmsPreservesNullEndTime()
    {
        const string json = """
            [{"id":1,"wtgRdsppId":"RDSPP-1","timeOnUtc":"2026-01-01T01:00:00Z","timeOffUtc":null,"alarmCode":"100","alarmState":"Good"}]
            """;
        var sequence = 0;

        var results = AlarmSearchRequestBuilder.ParseAlarms(json, "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence);

        Assert.Single(results);
        Assert.Null(results[0].EndTimeUtc);
    }

    [Fact]
    public void ParseAlarmsThrowsOnEmptyBody()
    {
        var sequence = 0;
        Assert.Throws<InvalidOperationException>(
            () => AlarmSearchRequestBuilder.ParseAlarms("", "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence));
    }

    [Fact]
    public void ParseAlarmsThrowsOnUnrecognizedShape()
    {
        var sequence = 0;
        Assert.Throws<InvalidOperationException>(
            () => AlarmSearchRequestBuilder.ParseAlarms("""{"unexpected":true}""", "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence));
    }

    [Fact]
    public void ParseAlarmsThrowsWhenResponseTurbineDoesNotMatchRequestedTurbine()
    {
        const string json = """
            [{"id":1,"wtgRdsppId":"RDSPP-OTHER","timeOnUtc":"2026-01-01T01:00:00Z","alarmCode":"100","alarmState":"Good"}]
            """;
        var sequence = 0;

        Assert.Throws<InvalidOperationException>(
            () => AlarmSearchRequestBuilder.ParseAlarms(json, "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence));
    }

    [Fact]
    public void ParseAlarmsMarksDeletedStateAsTombstone()
    {
        const string json = """
            [{"id":1,"wtgRdsppId":"RDSPP-1","timeOnUtc":"2026-01-01T01:00:00Z","alarmCode":"100","alarmState":"Deleted"}]
            """;
        var sequence = 0;

        var results = AlarmSearchRequestBuilder.ParseAlarms(json, "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence);

        Assert.True(results[0].IsTombstone);
    }

    [Fact]
    public void ParseAlarmsSupportsWrappedItemsArray()
    {
        const string json = """
            {"items":[{"id":1,"wtgRdsppId":"RDSPP-1","timeOnUtc":"2026-01-01T01:00:00Z","alarmCode":"100","alarmState":"Good"}]}
            """;
        var sequence = 0;

        var results = AlarmSearchRequestBuilder.ParseAlarms(json, "farm", "RDSPP-1", "ITEM-1", DateTime.UtcNow, ref sequence);

        Assert.Single(results);
    }
}
