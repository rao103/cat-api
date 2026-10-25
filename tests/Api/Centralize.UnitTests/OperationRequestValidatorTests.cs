using Centralize.Core.Models;
using Centralize.Core.Validators;

namespace Centralize.UnitTests;

public sealed class OperationRequestValidatorTests
{
    [Fact]
    public void Normalize_PreservesInclusiveUtcDatesAndCanonicalizesSource()
    {
        var request = new OperationRequestModel(12, new(2026, 9, 30), new(2026, 10, 1), " caller ", " outage ");
        var actual = OperationRequestValidator.Normalize(request);
        Assert.Equal("caller", actual.RequestedBy);
        Assert.Equal("Outage", actual.SourceType);
        Assert.Equal(new[] { new DateOnly(2026, 9, 30), new DateOnly(2026, 10, 1) },
            OperationRequestValidator.GetDates(actual));
    }

    [Fact]
    public void OneDayRange_IsNotEmpty()
    {
        var request = Request();
        Assert.Equal(request.StartDate, Assert.Single(OperationRequestValidator.GetDates(request)));
    }

    [Theory]
    [InlineData("farm")]
    [InlineData("caller")]
    [InlineData("range")]
    [InlineData("lastDate")]
    [InlineData("source")]
    [InlineData("missingDate")]
    public void Normalize_RejectsInvalidRequests(string invalid)
    {
        var request = Request();
        request = invalid switch
        {
            "farm" => request with { WindfarmId = 0 },
            "caller" => request with { RequestedBy = " " },
            "range" => request with { EndDate = request.StartDate.AddDays(-1) },
            "lastDate" => request with { EndDate = DateOnly.MaxValue },
            "source" => request with { SourceType = "Adjustment" },
            "missingDate" => request with { StartDate = default },
            _ => throw new InvalidOperationException()
        };
        Assert.ThrowsAny<ArgumentException>(() => OperationRequestValidator.Normalize(request));
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Outage")]
    [InlineData("Alarm")]
    [InlineData("Curtailment")]
    public void Normalize_AllowsEveryIngestorExceptAdjustment(string source)
    {
        var normalized = OperationRequestValidator.Normalize(Request() with { SourceType = source.ToLowerInvariant() });
        Assert.Equal(source, normalized.SourceType);
    }

    private static OperationRequestModel Request() => new(12, new(2026, 9, 30), new(2026, 9, 30), "caller");
}
