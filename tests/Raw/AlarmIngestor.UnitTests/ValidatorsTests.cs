using AlarmIngestor.Configuration;
using AlarmIngestor.Validators;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="IngestionOptionsValidator"/> and <see cref="SourceOptionsValidator"/>: an empty
/// windfarm code/turbine list fails fast rather than defaulting to "ingest everything", and
/// malformed API/queue URIs are rejected before any provider I/O.
/// </summary>
public sealed class ValidatorsTests
{
    private static IngestionOptions ValidOptions() => new()
    {
        Windfarms =
        [
            new WindfarmAlarmOptions
            {
                WindfarmId = "farm",
                MinRetrievalDateUtc = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                AlarmCodes = ["100"],
                Turbines = [new TurbineIdentity("RDSPP-1", "ITEM-1")],
            },
        ],
    };

    [Fact]
    public void ValidOptionsPassValidation()
    {
        IngestionOptionsValidator.ValidateAndThrow(ValidOptions());
    }

    [Fact]
    public void EmptyAlarmCodesFailsClosedRatherThanDefaultingToAllCodes()
    {
        var options = ValidOptions();
        options.Windfarms[0].AlarmCodes.Clear();

        Assert.Throws<ArgumentException>(() => IngestionOptionsValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void EmptyTurbineListFailsClosed()
    {
        var options = ValidOptions();
        options.Windfarms[0].Turbines.Clear();

        Assert.Throws<ArgumentException>(() => IngestionOptionsValidator.ValidateAndThrow(options));
    }

    [Fact]
    public void NoWindfarmsConfiguredFailsClosed()
    {
        Assert.Throws<ArgumentException>(() => IngestionOptionsValidator.ValidateAndThrow(new IngestionOptions()));
    }

    private static SourceOptions ValidSource() => new()
    {
        ApiBaseUri = "https://alarm-api.example/sdh/alarm/api",
        SourceSystemName = "CAT",
        SqsQueueUrl = "https://sqs.example/queue",
        BlobConnectionString = "UseDevelopmentStorage=true",
        BlobContainerName = "raw",
        ControlPlaneConnectionString = "Host=localhost",
    };

    [Fact]
    public void ValidSourceOptionsPassValidation()
    {
        SourceOptionsValidator.ValidateAndThrow(ValidSource());
    }

    [Fact]
    public void NonAbsoluteApiBaseUriFailsClosed()
    {
        var options = ValidSource();
        options.ApiBaseUri = "not-a-uri";

        Assert.Throws<ArgumentException>(() => SourceOptionsValidator.ValidateAndThrow(options));
    }
}
