using Harmonizer.Core.SourceTypes.Production.Adapters;
using Harmonizer.Core.SourceTypes.Production.Operations;
using Harmonizer.Core.Adapters;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Operations;
using Parquet.Serialization;
using Xunit;
namespace Harmonizer.UnitTests;

public sealed class ParquetTests
{
    [Fact]
    public async Task Actual_parquet_filters_exact_member_date_and_preserves_null_values()
    {
        var store = new MemoryObjects();
        using var stream = new MemoryStream();
        var rows = new[] { Fixtures.Row(actual: null), Fixtures.Row(1), Fixtures.Row(0, wtgId: 12), Fixtures.Row(144) };
        await ParquetSerializer.SerializeAsync(rows.Select(row => ProductionParquetRow.From(new(2, "WOW02"), row)), stream);
        stream.Position = 0;
        await store.WriteNewAsync("raw/part-1.parquet", stream, "application/vnd.apache.parquet", default);
        var adapter = new ParquetProductionArtifactStore(store, new MemoryObjects(), new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        var selected = await adapter.ReadRawAsync(Fixtures.Work(), default);
        Assert.Equal(2, selected.Count);
        Assert.Null(selected[0].Values.ActualProductionKWh);
        Assert.All(selected, row => Assert.Equal(DateTimeKind.Utc, row.StartTimeInterval.Kind));
    }
    [Fact]
    public async Task Candidate_round_trip_preserves_row_versions_and_prior_lineage()
    {
        var rawStore = new MemoryObjects();
        var outputStore = new MemoryObjects();
        var adapter = new ParquetProductionArtifactStore(rawStore, outputStore, new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        var source = Fixtures.Row() with { LastUpdatedDate = Fixtures.Start.AddTicks(123456789) };
        var result = new ProductionMergeService().Merge(Fixtures.Key, new([]), [source, Fixtures.Row(1, extraction: 2)]);
        var lease = new ComponentLease(Fixtures.Key, Fixtures.Work().Batch, 12, 4, null, null, null, null, "Partial", Guid.NewGuid());
        var candidate = await adapter.WriteCandidateAsync(lease, Fixtures.Work(), result, default);
        Assert.Equal("harmonized/wtg/windfarm=WOW02/wtg=WTG11/date=2026-09-01/production/revision=5/attempts/token=12/snapshot.json", candidate.SnapshotPath);
        Assert.EndsWith("/production-10minute.parquet", candidate.ParquetPath);
        var snapshot = await adapter.ReadSnapshotAsync(candidate.SnapshotPath, default);
        Assert.Equal(source.LastUpdatedDate, snapshot.Rows[0].LastUpdatedDate);
        Assert.Equal(2, snapshot.Rows.Select(row => row.Origin.BlobPath).Distinct().Count());
        Assert.Empty(rawStore.Data);
        Assert.Equal(3, outputStore.Data.Count);
    }
    [Fact]
    public async Task Failed_candidate_upload_never_produces_a_commit_candidate()
    {
        var rawStore = new MemoryObjects();
        var outputStore = new MemoryObjects { FailWriteNumber = 2 };
        var adapter = new ParquetProductionArtifactStore(rawStore, outputStore, new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        var lease = new ComponentLease(Fixtures.Key, Fixtures.Work().Batch, 1, 0, null, null, null, null, "Partial", Guid.NewGuid());
        var result = new ProductionMergeService().Merge(Fixtures.Key, new([]), [Fixtures.Row()]);
        await Assert.ThrowsAsync<IOException>(() => adapter.WriteCandidateAsync(lease, Fixtures.Work(), result, default));
        Assert.Single(outputStore.Data); // Unreferenced Parquet is safe; no state pointer was returned/committed.
    }
}

internal sealed class MemoryObjects : IRawObjectStore, IHarmonizedArtifactStore
{
    public Dictionary<string, byte[]> Data { get; } = new();
    public int FailWriteNumber { get; init; }
    private int writes;
    public Task<Stream> OpenReadAsync(string path, CancellationToken cancellationToken) => Task.FromResult<Stream>(new MemoryStream(Data[path]));
    public async Task WriteNewAsync(string path, Stream content, string contentType, CancellationToken cancellationToken)
    {
        if (++writes == FailWriteNumber) throw new IOException("Injected upload failure");
        using var copy = new MemoryStream();
        await content.CopyToAsync(copy, cancellationToken);
        Data.Add(path, copy.ToArray());
    }
}
