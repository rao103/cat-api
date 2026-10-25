using ProductionIngestor.Enums;
using ProductionIngestor.Raw;

namespace ProductionIngestor.UnitTests;

public sealed class RawPathBuilderTests
{
    [Fact]
    public void BuildsDistinctModePathsWithHalfOpenUtcBounds()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = start.AddDays(1);
        var incremental = RawPathBuilder.BuildPartPath("WOW02", IngestionMode.Incremental, 25, start, end, 1);
        var refresh = RawPathBuilder.BuildPartPath("WOW02", IngestionMode.Refresh, 26, start, end, 1);

        Assert.Equal("raw/production/windfarm=WOW02/mode=incremental/extraction=25/window-20260901T000000Z-to-20260902T000000Z-part-0001.parquet", incremental);
        Assert.Equal("raw/production/windfarm=WOW02/mode=refresh/extraction=26/window-20260901T000000Z-to-20260902T000000Z-part-0001.parquet", refresh);
    }

    [Fact]
    public void RetainsFractionalPrecisionAndDoesNotCapPartNumber()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc).AddTicks(1);
        var path = RawPathBuilder.BuildPartPath("WOW02", IngestionMode.Refresh, 26, start, start.AddTicks(2), 10_000);

        Assert.Contains("20260901T000000.0000001Z-to-20260901T000000.0000003Z-part-10000.parquet", path);
    }

    [Fact]
    public void RequiresUtcAndPositivePartNumber()
    {
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        Assert.Throws<ArgumentException>(() => RawPathBuilder.BuildPartPath(
            "WOW02", IngestionMode.Refresh, 26, start, DateTime.SpecifyKind(start.AddDays(1), DateTimeKind.Utc), 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => RawPathBuilder.BuildPartPath(
            "WOW02", IngestionMode.Refresh, 26, DateTime.SpecifyKind(start, DateTimeKind.Utc),
            DateTime.SpecifyKind(start.AddDays(1), DateTimeKind.Utc), 0));
    }
}