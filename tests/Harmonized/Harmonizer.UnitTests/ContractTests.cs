using Harmonizer.Core.SourceTypes.Production.Serialization;
using System.Text.Json;
using Harmonizer.Core.Adapters;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Xunit;
namespace Harmonizer.UnitTests;

public sealed class ContractTests
{
    private readonly ProductionWorkMessageReader reader = Fixtures.Reader();
    [Fact]
    public async Task V3_round_trips_numeric_identity_without_name_to_id_coercion()
    {
        var wire = Fixtures.Wire();
        var work = await reader.ReadAsync(Fixtures.Json(wire), default);
        Assert.Equal(2, work.Windfarm.WindfarmId);
        Assert.Equal("WOW02", work.Windfarm.WindfarmName);
        Assert.Equal(11, work.Turbines[0].WtgId);
        Assert.Equal("WTG11", work.Turbines[0].WtgName);
    }
    [Fact]
    public async Task String_id_in_numeric_v3_is_rejected() =>
        await Assert.ThrowsAsync<JsonException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire()).Replace("\"WindfarmId\":2", "\"WindfarmId\":\"2\""), default));
    [Fact]
    public async Task Duplicate_identity_is_rejected() =>
        await Assert.ThrowsAsync<WorkValidationException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire() with { ItemTurbines = [new(11, "A"), new(11, "B")] }), default));
    [Fact]
    public async Task Multiple_incremental_paths_are_rejected() =>
        await Assert.ThrowsAsync<WorkValidationException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire() with { RawPaths = ["raw/a.parquet", "raw/b.parquet"] }), default));
    [Fact]
    public async Task Invalid_work_kind_is_rejected() =>
        await Assert.ThrowsAsync<WorkValidationException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire() with { WorkKind = "CanonicalEvents" }), default));
    [Fact]
    public async Task Unsupported_event_work_cannot_be_silently_acknowledged() =>
        await Assert.ThrowsAsync<UnsupportedWorkException>(() => reader.ReadAsync("{\"SchemaVersion\":2,\"SourceType\":\"Outage\"}", default));
    [Theory]
    [InlineData("../raw/file.parquet")]
    [InlineData("/raw/file.parquet")]
    [InlineData("https://example.test/file.parquet")]
    [InlineData("raw//file.parquet")]
    public async Task Unsafe_paths_are_rejected(string path) =>
        await Assert.ThrowsAsync<WorkValidationException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire() with { RawPaths = [path] }), default));
    [Fact]
    public async Task Unknown_version_is_rejected() =>
        await Assert.ThrowsAsync<UnsupportedWorkException>(() => reader.ReadAsync(Fixtures.Json(Fixtures.Wire() with { SchemaVersion = 99 }), default));
}
