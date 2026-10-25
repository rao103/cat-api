using System.Runtime.CompilerServices;
using ProductionIngestor.Operations.Discovery;
using ProductionIngestor.Operations.Extraction;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.UnitTests;

public sealed class SourceContractTests
{
    private static readonly DateTime Timestamp = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public async Task EqualUpdateTimestampsRetainEveryAffectedDate()
    {
        var source = new Source([
            Row("first", Timestamp.AddDays(-2)), Row("first", Timestamp.AddDays(-1)),
            Row("second", Timestamp.AddDays(-1))]);
        var result = await new ChangeDiscoveryReader(source).DiscoverAsync("farm", Timestamp, Timestamp.AddDays(1), CancellationToken.None);
        Assert.Equal(3, result.ChangedRowCount);
        Assert.Equal(2, result.AffectedDates.Count);
    }

    [Fact]
    public async Task DiscoveryRejectsOverflowInsteadOfSilentlyTruncatingDates()
    {
        var source = new Source([Row("first", Timestamp.AddDays(-2)), Row("first", Timestamp.AddDays(-1))]);
        await Assert.ThrowsAsync<InvalidDataException>(() => new ChangeDiscoveryReader(source, 1)
            .DiscoverAsync("farm", Timestamp, Timestamp.AddDays(1), CancellationToken.None));
    }

    [Fact]
    public async Task DeduplicationUsesLatestUpdateRatherThanFabricatedRowId()
    {
        var older = Row("first", Timestamp) with { ActualProduction = 2m, LastUpdatedUtc = Timestamp.AddHours(-1) };
        var newer = older with { ActualProduction = 3m, LastUpdatedUtc = Timestamp };
        var rows = await CollectAsync(new WindfarmDayExtractor(new Source([newer, older]))
            .GetDeduplicatedSnapshotAsync("farm", DateOnly.FromDateTime(Timestamp)));
        Assert.Equal(3m, Assert.Single(rows).ActualProduction);
    }

    [Fact]
    public async Task ContradictoryEqualVersionsFailInsteadOfChoosingAnArbitraryRow()
    {
        var row = Row("first", Timestamp);
        var extractor = new WindfarmDayExtractor(new Source([row, row with { ActualProduction = 9m }]));
        await Assert.ThrowsAsync<InvalidDataException>(() => CollectAsync(extractor.GetDeduplicatedSnapshotAsync("farm", DateOnly.FromDateTime(Timestamp))));
    }

    [Fact]
    public async Task SnapshotRowLimitFailsExplicitly()
    {
        var extractor = new WindfarmDayExtractor(new Source([Row("first", Timestamp), Row("second", Timestamp)]), 1);
        await Assert.ThrowsAsync<InvalidDataException>(() => CollectAsync(extractor.GetDeduplicatedSnapshotAsync("farm", DateOnly.FromDateTime(Timestamp))));
    }

    private static ProductionRowModel Row(string itemId, DateTime timestamp)
        => new("farm", itemId, timestamp, 1m, null, null, null, Timestamp, null,
            timestamp.AddMinutes(10), null);

    private static async Task<List<ProductionRowModel>> CollectAsync(IAsyncEnumerable<ProductionRowModel> rows)
    {
        var result = new List<ProductionRowModel>();
        await foreach (var row in rows) result.Add(row);
        return result;
    }

    private sealed class Source(IReadOnlyList<ProductionRowModel> rows) : IProductionSourceAdapter
    {
        public Task<LatestSourceUpdatedDateModel> GetLatestSourceUpdatedDateByWindfarmIdAsync(
            WindfarmSourceQueryModel query, CancellationToken cancellationToken)
            => Task.FromResult(new LatestSourceUpdatedDateModel(rows.Count == 0 ? null : rows.Max(row => row.LastUpdatedUtc)));

        public async IAsyncEnumerable<ChangedProductionRowModel> GetChangedRowsAsync(string windfarmId, DateTime low,
            DateTime end, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return new(row.LastUpdatedUtc, row.WtgName, row.ProductionTimestampUtc, DateOnly.FromDateTime(row.ProductionTimestampUtc));
            }
        }

        public async IAsyncEnumerable<ProductionRowModel> GetWindfarmDaySnapshotAsync(string windfarmId, DateTime dayStartUtc,
            DateTime dayEndUtc, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return row;
            }
        }

        public Task<DateTime?> GetLatestLastUpdatedUtcAsync(string windfarmId, IReadOnlyList<string> activeTurbineNames,
            DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public IAsyncEnumerable<ProductionChangeRowModel> GetProductionChangeRowsAsync(string windfarmId,
            IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, DateTime windowMinInclusiveUtc,
            DateTime windowMaxInclusiveUtc, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}