using ProductionIngestor.Models;

namespace ProductionIngestor.UnitTests;

public sealed class ProductionModelTests
{
    [Fact]
    public void CursorTimestampNamesPreserveTheStatusJsonContract()
    {
        var lease = new ExtractionLeaseModel
        {
            WindfarmId = 1, WindfarmName = "farm", SourceType = "Production", LastUpdatedUtc = DateTime.UtcNow,
            LastCommittedId = "0", LeaseToken = 1, LeaseExpiresAtUtc = null
        };
        using var json = System.Text.Json.JsonDocument.Parse(System.Text.Json.JsonSerializer.Serialize(lease,
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)));
        Assert.True(json.RootElement.TryGetProperty("lastUpdatedDate", out _));
        Assert.Equal(System.Text.Json.JsonValueKind.Null, json.RootElement.GetProperty("leaseExpiresAt").ValueKind);
        Assert.False(json.RootElement.TryGetProperty("lastUpdatedUtc", out _));
    }

    [Fact]
    public void MeasurementsPreserveDecimalPrecisionAndMissingValues()
    {
        const decimal measurement = 123456789.1234567890123456789m;
        var timestamp = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);
        var row = new ProductionRowModel("farm", "turbine", timestamp, measurement, null,
            measurement, null, timestamp, null, timestamp.AddMinutes(10), "CurrentPowerCurvekWh");

        Assert.Equal(measurement, row.ActualProduction);
        Assert.Null(row.PossibleProduction);
        Assert.Equal(measurement, row.LostProductionCapacitykWh);
        Assert.Null(row.SiteAverageWindSpeedMps);
        Assert.Equal(timestamp.AddMinutes(10), row.EndDateAndTimeUtc);
        Assert.Equal("CurrentPowerCurvekWh", row.ContractualPoPEColumn);
    }
}