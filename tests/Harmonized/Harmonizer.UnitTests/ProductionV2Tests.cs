using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using ProductionIngestor.Contracts;
using Xunit;
using Turbine = Harmonizer.Core.Models.TurbineIdentity;

namespace Harmonizer.UnitTests;

public sealed class ProductionV2Tests
{
    private static ProductionWtgDayWorkV2 Wire() => new(2, "ProductionWtgDays", new('a', 64), new('b', 64),
        "Production", "2", "WOW02", "2026-09-01", "Incremental", "ProductionChanges", 2, "run", "attempt", 1,
        ["raw/part-1.parquet"], ["WTG11"], [], false, false, false, null, "{}", null);

    private static string Body(ProductionWtgDayWorkV2 wire) => JsonSerializer.Serialize(wire, HarmonizerJson.Options);

    [Fact]
    public async Task Both_versions_produce_the_same_business_work_and_keep_the_original_wire_body()
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11")]);
        var reader = Fixtures.Reader(catalog);
        var body = Body(Wire());
        var v2 = await reader.ReadAsync(body, default);
        var v3 = await reader.ReadAsync(Fixtures.Json(Fixtures.Wire()), default);
        Assert.Equal(v3.Batch, v2.Batch);
        Assert.Equal(v3.Windfarm, v2.Windfarm);
        Assert.Equal(v3.DataDate, v2.DataDate);
        Assert.Equal(v3.Turbines, v2.Turbines);
        Assert.Equal(v3.RawPaths, v2.RawPaths);
        Assert.Equal(body, v2.WireBody);
        Assert.Equal(1, catalog.NameLookups);
        Assert.Equal(0, catalog.IdLookups);
    }

    [Theory]
    [InlineData("WOW02")]
    [InlineData("0")]
    [InlineData("-2")]
    [InlineData("2.1")]
    [InlineData(" 2")]
    public async Task Invalid_legacy_farm_id_is_rejected_before_catalog_lookup(string id)
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11")]);
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader(catalog).ReadAsync(Body(Wire() with { WindfarmId = id }), default));
        Assert.Equal(0, catalog.NameLookups);
    }

    [Fact]
    public async Task Unknown_turbine_name_is_rejected()
    {
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader(new RecordingIdentityCatalog([])).ReadAsync(Body(Wire()), default));
    }

    [Fact]
    public async Task Duplicate_names_are_rejected_before_lookup()
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11")]);
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader(catalog).ReadAsync(Body(Wire() with { WtgNames = ["WTG11", "WTG11"] }), default));
        Assert.Equal(0, catalog.NameLookups);
    }

    [Fact]
    public async Task Scope_ids_are_resolved_independently_of_name_array_order()
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11"), new(12, "WTG12")]);
        var wire = Wire() with { WtgNames = ["WTG11", "WTG12"], ScopeWtgIds = [12, 11] };
        var work = await Fixtures.Reader(catalog).ReadAsync(Body(wire), default);
        Assert.Equal(new Turbine[] { new(11, "WTG11"), new(12, "WTG12") }, work.Turbines);
        Assert.Equal(1, catalog.IdLookups);
    }

    [Fact]
    public async Task Actual_members_outside_scope_are_rejected()
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11"), new(12, "WTG12")]);
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader(catalog).ReadAsync(Body(Wire() with { ScopeWtgIds = [12] }), default));
    }

    [Fact]
    public async Task Empty_refresh_reconciliation_resolves_scope_ids_without_actual_names()
    {
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11"), new(12, "WTG12")]);
        var range = new ProductionDateRangeV1("2026-09-01", "2026-09-01");
        var manifest = new ProductionExtractionReadyV1(2, "ExtractionReady", "Production", 2, "WOW02", "Refresh",
            "ProductionSnapshot", 2, "ProductionTimestamp", "run", "attempt", 1, [range],
            [new(1, Fixtures.Start, Fixtures.Start.AddDays(1), [range], 0, 0, true)], [], 0, true, ["WTG11", "WTG12"], null);
        var wire = Wire() with
        {
            WorkKind = HarmonizationWorkKinds.ProductionSnapshotReconciliation,
            ExtractionMode = "Refresh", PayloadKind = "ProductionSnapshot", WtgNames = [], ScopeWtgIds = [12, 11],
            RawPaths = [], HasNoSourceRows = true, IsExtractionReady = true,
            OriginalMessageBody = JsonSerializer.Serialize(manifest, HarmonizerJson.Options)
        };
        var work = await Fixtures.Reader(catalog).ReadAsync(Body(wire), default);
        Assert.True(work.IsSnapshot);
        Assert.Equal(new Turbine[] { new(11, "WTG11"), new(12, "WTG12") }, work.Turbines);
        Assert.Equal(0, catalog.NameLookups);
        Assert.Equal(1, catalog.IdLookups);
    }

    [Fact]
    public async Task Cancellation_is_propagated_before_lookup()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var catalog = new RecordingIdentityCatalog([new(11, "WTG11")]);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Fixtures.Reader(catalog).ReadAsync(Body(Wire()), cancellation.Token));
        Assert.Equal(0, catalog.NameLookups);
    }
}
