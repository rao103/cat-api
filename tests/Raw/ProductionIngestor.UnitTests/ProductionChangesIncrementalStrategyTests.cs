using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ProductionIngestor.Configuration;
using ProductionIngestor.Contracts;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Monitoring;
using ProductionIngestor.Operations.Execution.Changes;
using ProductionIngestor.Raw;
using ProductionIngestor.Serialization;
using Microsoft.Extensions.Logging.Abstractions;

namespace ProductionIngestor.UnitTests;

public sealed class ProductionChangesIncrementalStrategyTests
{
    private static readonly DateTime Checkpoint = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly WindfarmScopeModel Scope = new(42, "windfarm-name", DateTime.MinValue, new[] { "turbine" });

    [Fact]
    public async Task TwoPartsOfOneWindowShareTheWindowMinMaxPrefixAndUseTheWindfarmName()
    {
        var frozenMax = Checkpoint.AddDays(2);
        var window = Assert.Single(ProductionChangeWindowPlanner.PlanCalendarMonthWindows(Checkpoint, frozenMax));
        var adapter = new FakeAdapter
        {
            FrozenMaxLastUpdatedUtc = frozenMax,
            RowsByWindow = { [(window.MinInclusiveUtc, window.MaxInclusiveUtc)] =
                [Row(Checkpoint.AddHours(1), Checkpoint.AddHours(1)), Row(Checkpoint.AddHours(2), Checkpoint.AddHours(2))] },
        };
        var strategy = BuildStrategy(adapter, out var store, out _, maxRowsPerPart: 1);

        var result = await strategy.ExecuteAsync(
            new(Scope.WindfarmId, Scope.WindfarmName, "Production", "run-1", 7, Checkpoint, "0"), CancellationToken.None);

        Assert.Equal(ChangesOutcome.Succeeded, result.Outcome);
        var parts = store.Objects.Keys.Where(path => path.Contains("part-")).OrderBy(path => path).ToArray();
        Assert.Equal(2, parts.Length);
        var sharedPrefix = RawPathBuilder.BuildWindowExtractionPrefix("windfarm-name", 7)
            + $"window-{RawPathBuilder.FormatWindowBoundary(window.MinInclusiveUtc)}-to-{RawPathBuilder.FormatWindowBoundary(window.MaxInclusiveUtc.AddTicks(1))}-part-";
        Assert.All(parts, path => Assert.StartsWith(sharedPrefix, path));
        Assert.Contains("windfarm=windfarm-name", parts[0]);
    }

    [Fact]
    public async Task CheckpointAdvancesToTheMaximumObservedLastUpdatedUtcAcrossWindows()
    {
        var frozenMax = Checkpoint.AddMonths(1).AddDays(1);
        var windows = ProductionChangeWindowPlanner.PlanCalendarMonthWindows(Checkpoint, frozenMax);
        Assert.Equal(2, windows.Count);
        var earlyWindow = windows[0];
        var lateWindow = windows[1];
        var adapter = new FakeAdapter
        {
            FrozenMaxLastUpdatedUtc = frozenMax,
            RowsByWindow =
            {
                [(earlyWindow.MinInclusiveUtc, earlyWindow.MaxInclusiveUtc)] = [Row(earlyWindow.MinInclusiveUtc, Checkpoint.AddHours(5))],
                [(lateWindow.MinInclusiveUtc, lateWindow.MaxInclusiveUtc)] = [Row(frozenMax, lateWindow.MinInclusiveUtc)],
            },
        };
        var strategy = BuildStrategy(adapter, out _, out _);

        var result = await strategy.ExecuteAsync(
            new(Scope.WindfarmId, Scope.WindfarmName, "Production", "run-1", 7, Checkpoint, "0"), CancellationToken.None);

        Assert.True(result.Outcome == ChangesOutcome.Succeeded, result.FailureMessage);
        Assert.Equal(ChangesOutcome.Succeeded, result.Outcome);
        Assert.Equal(frozenMax, result.NewCommittedUpdatedUtc);
    }

    [Fact]
    public async Task NoFrozenUpperBoundPastTheCheckpointLeavesTheCheckpointUnchanged()
    {
        var adapter = new FakeAdapter { FrozenMaxLastUpdatedUtc = null };
        var strategy = BuildStrategy(adapter, out _, out var publisher);

        var result = await strategy.ExecuteAsync(
            new(Scope.WindfarmId, Scope.WindfarmName, "Production", "run-1", 7, Checkpoint, "0"), CancellationToken.None);

        Assert.Equal(ChangesOutcome.Succeeded, result.Outcome);
        Assert.Equal(Checkpoint, result.NewCommittedUpdatedUtc);
        Assert.Equal("0", result.NewCommittedId);
        Assert.Empty(publisher.Published);
        Assert.Null(result.QueryStartInclusiveUtc);
        Assert.Null(result.QueryEndExclusiveUtc);
    }

    [Fact]
    public async Task FailedQueryReturnsValidAttemptBoundsAndPreservesTheQueryFailure()
    {
        var frozenMax = Checkpoint.AddTicks(1);
        var window = Assert.Single(ProductionChangeWindowPlanner.PlanCalendarMonthWindows(Checkpoint, frozenMax));
        var adapter = new FakeAdapter
        {
            FrozenMaxLastUpdatedUtc = frozenMax,
            QueryFailure = new InvalidOperationException("source query failed"),
        };
        var strategy = BuildStrategy(adapter, out _, out _);

        var result = await strategy.ExecuteAsync(
            new(Scope.WindfarmId, Scope.WindfarmName, "Production", "run-1", 7, Checkpoint, "0"), CancellationToken.None);

        Assert.Equal(ChangesOutcome.Failed, result.Outcome);
        Assert.Equal("source query failed", result.FailureMessage);
        Assert.Equal(window.MinInclusiveUtc, result.QueryStartInclusiveUtc);
        Assert.Equal(window.MaxInclusiveUtc.AddTicks(1), result.QueryEndExclusiveUtc);
        Assert.True(result.QueryEndExclusiveUtc > result.QueryStartInclusiveUtc);
    }

    [Fact]
    public async Task EmptyExecutedWindowReturnsValidAttemptBounds()
    {
        var frozenMax = Checkpoint.AddTicks(1);
        var window = Assert.Single(ProductionChangeWindowPlanner.PlanCalendarMonthWindows(Checkpoint, frozenMax));
        var adapter = new FakeAdapter { FrozenMaxLastUpdatedUtc = frozenMax };
        var strategy = BuildStrategy(adapter, out _, out _);

        var result = await strategy.ExecuteAsync(
            new(Scope.WindfarmId, Scope.WindfarmName, "Production", "run-1", 7, Checkpoint, "0"), CancellationToken.None);

        Assert.Equal(ChangesOutcome.Succeeded, result.Outcome);
        Assert.Equal(window.MinInclusiveUtc, result.QueryStartInclusiveUtc);
        Assert.Equal(window.MaxInclusiveUtc.AddTicks(1), result.QueryEndExclusiveUtc);
        Assert.True(result.QueryEndExclusiveUtc > result.QueryStartInclusiveUtc);
    }

    private static ProductionChangesIncrementalStrategy BuildStrategy(
        FakeAdapter adapter, out MemoryObjectStore store, out FakePublisher publisher, int maxRowsPerPart = 50_000)
    {
        store = new MemoryObjectStore();
        publisher = new FakePublisher();
        var windowProcessor = new ProductionChangeWindowProcessor(
            adapter, new ProductionChangesPartWriter(store), publisher,
            new IngestionTelemetry<ProductionChangeWindowProcessor>(NullLogger<ProductionChangeWindowProcessor>.Instance), maxRowsPerPart);
        return new ProductionChangesIncrementalStrategy(
            adapter, new FakeScopeRepository(), windowProcessor,
            new IngestionTelemetry<ProductionChangesIncrementalStrategy>(NullLogger<ProductionChangesIncrementalStrategy>.Instance),
            new IngestionOptions(), "test-source");
    }

    private static ProductionChangeRowModel Row(DateTime lastUpdatedUtc, DateTime productionTimestampUtc)
        => new("turbine", productionTimestampUtc, productionTimestampUtc, lastUpdatedUtc, 1m, null, "CurrentPowerCurvekWh", null, null);

    private sealed class FakeScopeRepository : IProductionScopeRepository
    {
        public Task<WindfarmScopeModel?> GetActiveScopeAsync(int windfarmId, DateTime asOfUtc, CancellationToken cancellationToken)
            => Task.FromResult<WindfarmScopeModel?>(Scope);
    }

    private sealed class FakeAdapter : IProductionSourceAdapter
    {
        public DateTime? FrozenMaxLastUpdatedUtc { get; init; }
        public Exception? QueryFailure { get; init; }
        public Dictionary<(DateTime Min, DateTime Max), IReadOnlyList<ProductionChangeRowModel>> RowsByWindow { get; } = new();

        public Task<LatestSourceUpdatedDateModel> GetLatestSourceUpdatedDateByWindfarmIdAsync(
            WindfarmSourceQueryModel query, CancellationToken cancellationToken) => throw new NotSupportedException();

        public IAsyncEnumerable<ChangedProductionRowModel> GetChangedRowsAsync(
            string windfarmName, DateTime low, DateTime end, CancellationToken cancellationToken) => throw new NotSupportedException();

        public IAsyncEnumerable<ProductionRowModel> GetWindfarmDaySnapshotAsync(
            string windfarmName, DateTime dayStartUtc, DateTime dayEndUtc, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task<DateTime?> GetLatestLastUpdatedUtcAsync(
            string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc, CancellationToken cancellationToken)
            => Task.FromResult(FrozenMaxLastUpdatedUtc);

        public async IAsyncEnumerable<ProductionChangeRowModel> GetProductionChangeRowsAsync(
            string windfarmName, IReadOnlyList<string> activeTurbineNames, DateTime minRetrieveDateUtc,
            DateTime windowMinInclusiveUtc, DateTime windowMaxInclusiveUtc,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            if (QueryFailure is not null) throw QueryFailure;
            if (RowsByWindow.TryGetValue((windowMinInclusiveUtc, windowMaxInclusiveUtc), out var rows))
                foreach (var row in rows) yield return row;
        }
    }

    private sealed class MemoryObjectStore : IRawObjectStore
    {
        public ConcurrentDictionary<string, byte[]> Objects { get; } = new(StringComparer.Ordinal);

        public Task UploadAsync(string path, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            Objects[path] = content.ToArray();
            return Task.CompletedTask;
        }

        public Task<bool> ExistsAsync(string path, CancellationToken cancellationToken) => Task.FromResult(Objects.ContainsKey(path));
        public Task<byte[]> DownloadAsync(string path, CancellationToken cancellationToken) => Task.FromResult(Objects[path]);
    }

    private sealed class FakePublisher : IRawArtifactPublisher
    {
        public List<ProductionChangesV2> Published { get; } = new();

        public Task PublishAsync(RawArtifactAvailableV1 message, CancellationToken cancellationToken) => throw new NotSupportedException();

        public Task PublishExtractionReadyAsync(ProductionExtractionReadyV1 message, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<bool> PublishProductionChangesAsync(IReadOnlyList<ProductionChangesV2> parts, CancellationToken cancellationToken)
        {
            Published.AddRange(parts);
            return Task.FromResult(true);
        }
    }
}
