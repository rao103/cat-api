using ProductionIngestor.Configuration;
using ProductionIngestor.Enums;
using ProductionIngestor.Validators;
using ProductionIngestor.Worker.Configuration;

namespace ProductionIngestor.UnitTests;

public sealed class IngestionRequestTests
{
    [Fact]
    public void RefreshNameWithoutFarmIdFailsClosed()
    {
        Assert.Throws<ArgumentNullException>(() => IngestionRequestParser.Parse("42", null, "farm", null, null));
    }

    [Fact]
    public void RefreshUsesEnvironmentWindfarmAndSortsDistinctDates()
    {
        var request = IngestionRequestParser.Parse("7", "99", "farm",
            "2026-09-02,2026-09-01,2026-09-02", "requested-run");
        Assert.Equal(IngestionMode.Refresh, request.Mode);
        Assert.Equal(99, request.WindfarmId);
        Assert.Equal("requested-run", request.RunId);
        Assert.Equal(new[] { new DateOnly(2026, 9, 1), new DateOnly(2026, 9, 2) }, request.ExplicitDates);
    }

    [Theory]
    [InlineData(null, null, null)]
    [InlineData("farm", null, "2026-09-01")]
    [InlineData(null, "farm", "")]
    [InlineData(null, "farm", "09/01/2026")]
    [InlineData(null, "farm", "2026-09-01,")]
    [InlineData("../farm", null, null)]
    public void InvalidRequestsFailBeforeExecution(string? windfarmId, string? refreshWindfarmId, string? refreshDates)
        => Assert.ThrowsAny<ArgumentException>(() => IngestionRequestParser.Parse(windfarmId, refreshWindfarmId, "farm", refreshDates, null));

    [Fact]
    public void InvalidProcessingLimitsAreRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => IngestionOptionsValidator.ValidateAndThrow(new IngestionOptions { MaxRowsPerShard = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => IngestionOptionsValidator.ValidateAndThrow(new IngestionOptions { ParquetRowGroupSize = 0 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => IngestionOptionsValidator.ValidateAndThrow(new IngestionOptions { SafetyLagInterval = TimeSpan.FromSeconds(-1) }));
    }
}