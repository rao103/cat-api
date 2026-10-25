using Harmonizer.Core.SourceTypes.Production.Serialization;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using System.Text.Json;
using CAT.Messaging.Contracts;
using Xunit;
namespace Harmonizer.UnitTests;

public sealed class RealMessageTests
{
    [Theory]
    [InlineData(1, 50, "2026-02-01")]
    [InlineData(2, 1, "2026-02-01")]
    [InlineData(3, 50, "2026-02-04")]
    [InlineData(4, 50, "2026-01-31")]
    [InlineData(5, 1, "2026-01-31")]
    [InlineData(6, 50, "2026-02-03")]
    public async Task Supplied_v3_refresh_uses_item_identities_and_business_date(int number, int expectedMembers, string expectedDate)
    {
        var body = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", $"production-v3-real-{number}.json"));
        var work = await Fixtures.Reader().ReadAsync(body, default);
        Assert.Equal(expectedMembers, work.Turbines.Count);
        Assert.Equal(ProductionWorkMessageReader.ParseDate(expectedDate), work.DataDate);
        Assert.Equal(2, work.Windfarm.WindfarmId);
        Assert.Equal("WOW02", work.Windfarm.WindfarmName);
        Assert.True(work.IsSnapshot);
        Assert.False(work.AllowRemovalReconciliation);
        Assert.Equal(expectedMembers == 1 ? 102 : 52, work.Turbines[0].WtgId);
        var manifest = RefreshManifestValidator.Validate(work);
        Assert.Equal(2, manifest.Artifacts.Count);
        Assert.Single(RefreshManifestValidator.RequiredArtifacts(manifest, work.DataDate));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task Supplied_v2_examples_resolve_names_and_preserve_the_original_body(int number)
    {
        var body = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Fixtures", $"production-v2-real-{number}.json"));
        var wire = JsonSerializer.Deserialize<ProductionWtgDayWorkV2>(body, HarmonizerJson.Options)!;
        var identities = wire.WtgNames.Select((name, index) => new Harmonizer.Core.Models.TurbineIdentity(index + 11, name)).ToArray();
        var catalog = new RecordingIdentityCatalog(identities);
        var work = await Fixtures.Reader(catalog).ReadAsync(body, default);
        Assert.Equal(identities, work.Turbines);
        Assert.Equal(2, work.Windfarm.WindfarmId);
        Assert.Equal("WOW02", work.Windfarm.WindfarmName);
        Assert.Equal(body, work.WireBody);
        Assert.Equal(wire.ExtractionMode == "Refresh", work.IsSnapshot);
        Assert.Equal(1, catalog.NameLookups);
    }
}
