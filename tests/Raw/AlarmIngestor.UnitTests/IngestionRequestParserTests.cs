using AlarmIngestor.Enums;
using AlarmIngestor.Worker.Configuration;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="IngestionRequestParser"/>: environment overrides configuration for WindfarmId,
/// Historical mode requires both bounds, and an invalid/missing windfarm id fails fast.
/// </summary>
public sealed class IngestionRequestParserTests
{
    [Fact]
    public void EnvironmentWindfarmIdOverridesConfiguredValue()
    {
        var request = IngestionRequestParser.Parse("1", "2", null, null, null, null);

        Assert.Equal("2", request.WindfarmId);
        Assert.Equal(IngestionMode.Incremental, request.Mode);
    }

    [Fact]
    public void MissingWindfarmIdThrows()
    {
        Assert.Throws<ArgumentNullException>(() => IngestionRequestParser.Parse(null, null, null, null, null, null));
    }

    [Fact]
    public void NonIntegerWindfarmIdThrows()
    {
        Assert.Throws<ArgumentException>(() => IngestionRequestParser.Parse("not-a-number", null, null, null, null, null));
    }

    [Fact]
    public void NonPositiveWindfarmIdThrows()
    {
        Assert.Throws<ArgumentException>(() => IngestionRequestParser.Parse("0", null, null, null, null, null));
    }

    [Fact]
    public void HistoricalModeRequiresBothBounds()
    {
        Assert.Throws<ArgumentException>(
            () => IngestionRequestParser.Parse("1", null, null, "Historical", "2026-01-01T00:00:00Z", null));
    }

    [Fact]
    public void HistoricalModeParsesBothBoundsAsUtc()
    {
        var request = IngestionRequestParser.Parse(
            "1", null, "run-1", "Historical", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z");

        Assert.Equal(IngestionMode.Historical, request.Mode);
        Assert.Equal(DateTimeKind.Utc, request.HistoricalStartUtc!.Value.Kind);
        Assert.Equal(DateTimeKind.Utc, request.HistoricalEndUtc!.Value.Kind);
    }
}
