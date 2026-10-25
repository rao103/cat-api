using System.Text.Json;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Adapters;
using Harmonizer.Worker.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace Harmonizer.UnitTests;

public sealed class StorageSeparationTests
{
    [Fact]
    public void Registration_resolves_distinct_raw_and_harmonized_instances()
    {
        using var provider = Provider("https://silver.example.invalid/outputs");
        Assert.NotSame(provider.GetRequiredService<IRawObjectStore>(), provider.GetRequiredService<IHarmonizedArtifactStore>());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("relative/container")]
    [InlineData("ftp://silver.example.invalid/outputs")]
    public void Missing_or_invalid_output_destination_is_rejected_before_processing(string? outputUrl)
    {
        using var provider = Provider(outputUrl);
        Assert.Throws<OptionsValidationException>(() => provider.GetRequiredService<IOptions<HarmonizerOptions>>().Value);
    }

    [Fact]
    public async Task Snapshot_reads_use_harmonized_storage_even_when_raw_contains_the_same_path()
    {
        var raw = new MemoryObjects();
        var output = new MemoryObjects();
        const string previousPath = "harmonized/production/windfarm_id=2/wtgid=11/date=2026-09-01/revision=1/attempts/token=1/snapshot.json";
        raw.Data[previousPath] = "unrelated raw data"u8.ToArray();
        output.Data[previousPath] = JsonSerializer.SerializeToUtf8Bytes(new Harmonizer.Core.SourceTypes.Production.Models.ProductionSnapshot([Fixtures.Row()]), HarmonizerJson.Options);
        var adapter = new ParquetProductionArtifactStore(raw, output, new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        var snapshot = await adapter.ReadSnapshotAsync(previousPath, default);
        Assert.Equal(Fixtures.Row(), Assert.Single(snapshot.Rows));
    }

    [Fact]
    public void Reserved_partition_characters_do_not_change_the_wtg_day_hierarchy()
    {
        var lease = new ComponentLease(Fixtures.Key, Fixtures.Work().Batch, 12, 4, null, null, null, null, "Partial", Guid.NewGuid());
        var prefix = new HarmonizedArtifactPathBuilder().CreateComponentPrefix(lease, new(2, "WF/A"), new(11, "T/01"));
        Assert.Equal("harmonized/wtg/windfarm=WF%2FA/wtg=T%2F01/date=2026-09-01/production/revision=5/attempts/token=12", prefix);
        Assert.True(ObjectPath.IsSafe(prefix));
    }

    [Fact]
    public void Path_builder_rejects_a_turbine_outside_the_leased_component()
    {
        var lease = new ComponentLease(Fixtures.Key, Fixtures.Work().Batch, 12, 4, null, null, null, null, "Partial", Guid.NewGuid());
        Assert.Throws<WorkValidationException>(() => new HarmonizedArtifactPathBuilder().CreateComponentPrefix(lease, new(2, "WOW02"), new(12, "WTG12")));
    }

    private static ServiceProvider Provider(string? outputUrl)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Harmonizer:QueueUrl"] = "https://sqs.example.invalid/harmonizer",
            ["Harmonizer:BlobContainerUrl"] = "https://raw.example.invalid/inputs",
            ["Harmonizer:OutputBlobContainerUrl"] = outputUrl,
            ["Harmonizer:PostgresConnectionString"] = "Host=localhost;Username=test;Database=test"
        }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(typeof(ITelemetry<>), typeof(HarmonizerTelemetry<>));
        services.AddHarmonizer(configuration);
        return services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true, ValidateScopes = true });
    }
}
