using System.Globalization;
using Microsoft.Extensions.Configuration;
using OutageIngestor.Adapters.Source;
using OutageIngestor.Configuration;
using OutageIngestor.Factories;
using OutageIngestor.Worker.Configuration;
using Xunit.Abstractions;

namespace OutageIngestor.IntegrationTests;

/// <summary>Opt-in, read-only source smoke test. No cursor, Blob or SQS operations.</summary>
public sealed class OutageSourceRefreshTests(ITestOutputHelper output)
{
    [OutageSourceFact]
    public async Task ScopedRefreshStreamsLiveEventsFromConfiguredSource()
    {
        var settingsPath = Environment.GetEnvironmentVariable("OUTAGE_TEST_SOURCE_SETTINGS")!;
        var farm = Environment.GetEnvironmentVariable("OUTAGE_TEST_SOURCE_FARM")!;
        var turbines = Environment.GetEnvironmentVariable("OUTAGE_TEST_SOURCE_TURBINES")!
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        Assert.NotEmpty(turbines);
        var start = DateTime.Parse(Environment.GetEnvironmentVariable("OUTAGE_TEST_SOURCE_START")!, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        var end = DateTime.Parse(Environment.GetEnvironmentVariable("OUTAGE_TEST_SOURCE_END")!, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
        Assert.True(end > start);

        // Match the local worker's development layers, including user secrets and environment
        // overrides. A base settings file alone deliberately contains no source credentials.
        // No host is started and no cursor, object store or queue service is resolved.
        var builder = new ConfigurationBuilder()
            .AddJsonFile(settingsPath, optional: false)
            .AddJsonFile(Path.Combine(Path.GetDirectoryName(settingsPath)!, "appsettings.Development.json"), optional: true)
            .AddUserSecrets(typeof(IngestionRequestInput).Assembly, optional: true)
            .AddEnvironmentVariables();
        using var settings = (ConfigurationRoot)builder.Build();
        var options = settings.Get<SqlSourceOptions>()!;
        var legacy = settings["Source:ConnectionString"];
        using var factory = new SqlOutageConnectionFactory(options, legacy);
        var adapter = new SqlOutageSourceAdapter(factory, batchSize: 1);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMinutes(3));
        var count = 0;
        await foreach (var row in adapter.GetAllCurrentEventsAsync(farm, turbines, start, start, end, cancellation.Token))
        {
            Assert.Equal(farm, row.WindfarmId);
            Assert.Contains(row.ItemId, turbines);
            Assert.False(row.IsTombstone);
            Assert.True(row.OriginalStartUtc < end);
            Assert.True(row.OriginalStartUtc > start
                || (row.OriginalStartUtc < start && row.OriginalEndUtc > start)
                || (row.OriginalStartUtc < start && row.OriginalEndUtc is null));
            Assert.True(row.OriginalEndUtc is null || row.OriginalEndUtc >= start);
            count++;
        }
        output.WriteLine("Completed one scoped source read: {0} live events; no Raw writes or SQS publications.", count);
    }
}

public sealed class OutageSourceFactAttribute : FactAttribute
{
    public OutageSourceFactAttribute()
    {
        if (new[] { "OUTAGE_TEST_SOURCE_SETTINGS", "OUTAGE_TEST_SOURCE_FARM", "OUTAGE_TEST_SOURCE_TURBINES", "OUTAGE_TEST_SOURCE_START", "OUTAGE_TEST_SOURCE_END" }
            .Any(name => string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name))))
            Skip = "Set OUTAGE_TEST_SOURCE_SETTINGS/FARM/TURBINES/START/END to explicitly opt in to a read-only source test.";
    }
}