using OutageIngestor.Enums;
using OutageIngestor.Worker.Configuration;

namespace OutageIngestor.UnitTests;

public sealed class WorkerIngestionRequestParserTests
{
    [Fact]
    public void RefreshParsesInternalIntegerIdentityAndKeepsTheSeparateSourceName()
    {
        var result = IngestionRequestParser.TryParse(new("99", "00042", "CHW01", "run-42", "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z"));

        Assert.True(result.Succeeded);
        Assert.Equal(42, result.Request!.WindfarmId);
        Assert.Equal("CHW01", result.Request.WindfarmName);
        Assert.Equal(IngestionMode.Refresh, result.Request.Mode);
        Assert.Equal("Outage", result.Request.SourceType);
        Assert.Equal("run-42", result.Request.RunId);
        Assert.Equal(new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), result.Request.RefreshScopeStartUtc);
        Assert.Equal(new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc), result.Request.RefreshScopeEndUtc);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("0")]
    [InlineData("-42")]
    [InlineData("+42")]
    [InlineData("42.0")]
    [InlineData(" 42 ")]
    [InlineData("2147483648")]
    [InlineData("CHW01")]
    public void InvalidOrMissingIdIsRejectedWithoutUsingConfiguredId(string? id)
    {
        var result = IngestionRequestParser.TryParse(new("99", id, "CHW01", "run-42"));
        Assert.False(result.Succeeded);
        Assert.Null(result.Request);
        Assert.NotNull(result.Error);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void MissingNameNeverFallsBackToNumericId(string? name)
    {
        var result = IngestionRequestParser.TryParse(new(null, "42", name, "run-42"));
        Assert.False(result.Succeeded);
        Assert.Null(result.Request);
        Assert.Contains("REFRESH_WINDFARM_NAME", result.Error!);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void MissingRunIdGeneratesServerRunIdentity(string? runId)
    {
        var result = IngestionRequestParser.TryParse(new(null, "42", "CHW01", runId, "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z"));
        Assert.True(result.Succeeded);
        Assert.True(Guid.TryParseExact(result.Request!.RunId, "N", out _));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(null, "2026-09-02T00:00:00Z")]
    [InlineData("2026-09-01T00:00:00Z", null)]
    [InlineData("", "2026-09-02T00:00:00Z")]
    [InlineData("2026-09-01T00:00:00Z", " ")]
    [InlineData("not-a-date", "2026-09-02T00:00:00Z")]
    [InlineData("2026-09-01T00:00:00Z", "not-a-date")]
    [InlineData("2026-09-31T00:00:00Z", "2026-10-01T00:00:00Z")]
    [InlineData("2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z")]
    [InlineData("2026-09-02T00:00:00Z", "2026-09-01T00:00:00Z")]
    public void MissingOrInvalidRefreshScopeIsRejected(string? startUtc, string? endUtc)
    {
        var result = IngestionRequestParser.TryParse(new("99", "42", "CHW01", "run-42", startUtc, endUtc));
        Assert.False(result.Succeeded);
        Assert.Null(result.Request);
        Assert.Contains("REFRESH_SCOPE_", result.Error!);
    }

    [Fact]
    public void RefreshScopePreservesSubDayPrecisionAndNormalizesOffsetsToUtc()
    {
        var result = IngestionRequestParser.TryParse(new(null, "42", "CHW01", "run-42",
            "2026-09-01T03:04:05.1234567+02:00", "2026-09-01T03:04:05.1234568+02:00"));

        Assert.True(result.Succeeded, result.Error);
        var start = Assert.IsType<DateTime>(result.Request!.RefreshScopeStartUtc);
        var end = Assert.IsType<DateTime>(result.Request.RefreshScopeEndUtc);
        Assert.Equal(new DateTime(2026, 9, 1, 1, 4, 5, DateTimeKind.Utc).AddTicks(1234567), start);
        Assert.Equal(start.AddTicks(1), end);
        Assert.Equal(DateTimeKind.Utc, start.Kind);
        Assert.Equal(DateTimeKind.Utc, end.Kind);
    }

    [Theory]
    [InlineData("2026-09-01T00:00:00Z", null)]
    [InlineData(null, "2026-09-02T00:00:00Z")]
    [InlineData("", null)]
    [InlineData(null, " ")]
    [InlineData("not-a-date", null)]
    public void ScopeOnlyInputSelectsRefreshAndNeverUsesConfiguredIdentity(string? startUtc, string? endUtc)
    {
        var input = new IngestionRequestInput("99", null, null, null, startUtc, endUtc);

        Assert.True(input.IsRefresh);
        Assert.Null(input.SelectedWindfarmId);
        var result = IngestionRequestParser.TryParse(input);
        Assert.False(result.Succeeded);
        Assert.Null(result.Request);
        Assert.Contains("REFRESH_WINDFARM_ID", result.Error!);
    }

    [Fact]
    public void AnyRefreshSettingSelectsRefreshEvenWhenEmpty()
    {
        Assert.True(new IngestionRequestInput("42", "", null, null).IsRefresh);
        Assert.True(new IngestionRequestInput("42", null, "", null).IsRefresh);
        Assert.True(new IngestionRequestInput("42", null, null, "").IsRefresh);
        Assert.False(new IngestionRequestInput("42", null, null, null).IsRefresh);
    }

    [Fact]
    public void IncrementalInputWithoutNameCannotProduceAnInvalidRequest()
    {
        var result = IngestionRequestParser.TryParse(new("42", null, null, null));
        Assert.False(result.Succeeded);
        Assert.Null(result.Request);
    }
}