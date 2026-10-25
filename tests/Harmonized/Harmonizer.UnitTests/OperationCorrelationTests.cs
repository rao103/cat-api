using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Nodes;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Adapters;
using Harmonizer.Core.SourceTypes.Production.Interfaces;
using Harmonizer.Core.SourceTypes.Production.Models;
using Harmonizer.Core.SourceTypes.Production.Operations;
using Parquet.Serialization;
using Xunit;

namespace Harmonizer.UnitTests;

[Collection("Telemetry")]
public sealed class OperationCorrelationTests
{
    private static string Body(string? operationId = "OP-001", string? executionId = "EXEC-001", string original = "{}")
    {
        var envelope = JsonNode.Parse(Fixtures.Json(Fixtures.Wire()))!.AsObject();
        envelope["OperationId"] = operationId;
        envelope["ExecutionId"] = executionId;
        envelope["OriginalMessageBody"] = original;
        return envelope.ToJsonString();
    }

    [Fact]
    public async Task V3_preserves_opaque_ids_and_original_wire_body()
    {
        var body = Body();
        var work = await Fixtures.Reader().ReadAsync(body, default);
        Assert.Equal(new OperationIdentity("OP-001", "EXEC-001"), work.Operation);
        Assert.Equal(body, work.WireBody);
    }

    [Fact]
    public async Task V2_reads_additive_ids_without_changing_shared_contracts()
    {
        var envelope = JsonNode.Parse(await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory,
            "Fixtures", "production-v2-real-1.json")))!.AsObject();
        envelope["OperationId"] = "OP-V2";
        envelope["ExecutionId"] = "EXEC-V2";
        var catalog = new RecordingIdentityCatalog([new(11, "WOW02F15")]);
        var work = await Fixtures.Reader(catalog).ReadAsync(envelope.ToJsonString(), default);
        Assert.Equal(new OperationIdentity("OP-V2", "EXEC-V2"), work.Operation);
    }

    [Fact]
    public async Task Embedded_original_envelope_supplies_missing_ids()
    {
        var work = await Fixtures.Reader().ReadAsync(Body(null, null,
            "{\"OperationId\":\"OP-001\",\"ExecutionId\":\"EXEC-001\"}"), default);
        Assert.Equal(new OperationIdentity("OP-001", "EXEC-001"), work.Operation);
    }

    [Fact]
    public async Task Conflicting_original_identity_is_rejected()
    {
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader().ReadAsync(
            Body(original: "{\"OperationId\":\"another-operation\"}"), default));
    }

    [Theory]
    [InlineData("\"\"")]
    [InlineData("\"   \"")]
    [InlineData("123")]
    [InlineData("\"OP\\n001\"")]
    public async Task Invalid_identifier_is_rejected(string jsonValue)
    {
        var envelope = JsonNode.Parse(Body())!.AsObject();
        envelope["OperationId"] = JsonNode.Parse(jsonValue);
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader().ReadAsync(envelope.ToJsonString(), default));
    }

    [Fact]
    public async Task Oversized_identifier_is_rejected()
    {
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader().ReadAsync(Body(new string('x', 257)), default));
    }

    [Fact]
    public async Task Duplicate_embedded_identity_is_rejected()
    {
        await Assert.ThrowsAsync<WorkValidationException>(() => Fixtures.Reader().ReadAsync(Body(original:
            "{\"OperationId\":\"OP-001\",\"OperationId\":\"OP-001\"}"), default));
    }

    [Fact]
    public async Task Legacy_messages_do_not_alias_request_or_trace_ids_to_operation_ids()
    {
        var wire = Fixtures.Wire() with { TraceId = "source-trace" };
        var work = await Fixtures.Reader().ReadAsync(Fixtures.Json(wire), default);
        Assert.Equal(OperationIdentity.Untracked, work.Operation);
    }

    [Fact]
    public void Legacy_row_origin_deserializes_without_new_optional_metadata()
    {
        var origin = JsonSerializer.Deserialize<ArtifactReference>(
            "{\"BlobPath\":\"raw/part.parquet\",\"ExtractionToken\":1,\"RequestId\":\"run\",\"AttemptId\":\"attempt\"}", HarmonizerJson.Options)!;
        Assert.Null(origin.OperationId);
        Assert.Null(origin.ExecutionId);
    }

    [Fact]
    public async Task Raw_rows_and_lineage_preserve_operation_origin_across_merges()
    {
        var objects = new MemoryObjects();
        var output = new MemoryObjects();
        var adapter = new ParquetProductionArtifactStore(objects, output, new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        using var parquet = new MemoryStream();
        await ParquetSerializer.SerializeAsync(new[] { ProductionParquetRow.From(new(2, "WOW02"), Fixtures.Row(1)) }, parquet);
        parquet.Position = 0;
        await objects.WriteNewAsync("raw/part-1.parquet", parquet, "application/vnd.apache.parquet", default);
        var work = Fixtures.Work() with { Operation = new("OP-NEW", "EXEC-NEW") };
        var raw = await adapter.ReadRawAsync(work, default);
        Assert.Equal("OP-NEW", Assert.Single(raw).Origin.OperationId);
        var previous = Fixtures.Row() with { Origin = new("old.parquet", 0, "old-run", "old-attempt", "OP-OLD", "EXEC-OLD") };
        var merger = new ProductionMergeService();
        var merged = merger.Merge(Fixtures.Key, new([previous]), raw);
        var lease = new ComponentLease(Fixtures.Key, work.Batch, 1, 1, null, null, null, null, "Partial", Guid.NewGuid());
        var candidate = await adapter.WriteCandidateAsync(lease, work, merged, default);
        using var manifest = JsonDocument.Parse(output.Data[candidate.ManifestPath]);
        Assert.Equal("OP-NEW", manifest.RootElement.GetProperty("OperationId").GetString());
        var snapshot = await adapter.ReadSnapshotAsync(candidate.SnapshotPath, default);
        Assert.Equal("OP-OLD", snapshot.Rows.Single(row => row.StartTimeInterval == Fixtures.Start).Origin.OperationId);
        Assert.Equal("EXEC-NEW", snapshot.Rows.Single(row => row.StartTimeInterval != Fixtures.Start).Origin.ExecutionId);
        var retriggered = raw.Select(row => row with { Origin = row.Origin with { OperationId = "OP-OTHER" } }).ToArray();
        var replay = merger.Merge(Fixtures.Key, merged.Snapshot, retriggered);
        Assert.False(replay.Changed);
        Assert.Equal(merged.CanonicalHash, replay.CanonicalHash);
    }

    private static ComponentCommit Commit(OperationIdentity identity, bool changed = true, string status = "Complete") =>
        new(new(Fixtures.Key, Fixtures.Work().Batch, 1, 5, null, null, null, null, status, Guid.NewGuid()),
            5, new('a', 64), new("data.parquet", "snapshot.json", "lineage.json"), 144, true, changed)
        { Operation = identity };

    [Fact]
    public void Notifications_preserve_identity_and_deduplicate_retries_per_operation_execution()
    {
        var factory = new ProductionNotificationFactory();
        var first = factory.Create(Commit(new("OP-001", "EXEC-001")));
        var retry = factory.Create(Commit(new("OP-001", "EXEC-001")));
        var otherOperation = factory.Create(Commit(new("OP-002", "EXEC-001")));
        var otherExecution = factory.Create(Commit(new("OP-001", "EXEC-002")));
        Assert.Equal(2, first.Count);
        Assert.Equal(first.Select(item => item.DeduplicationKey), retry.Select(item => item.DeduplicationKey));
        Assert.NotEqual(first[0].DeduplicationKey, otherOperation[0].DeduplicationKey);
        Assert.NotEqual(first[0].DeduplicationKey, otherExecution[0].DeduplicationKey);
        using var body = JsonDocument.Parse(first[0].Body);
        Assert.Equal("OP-001", body.RootElement.GetProperty("OperationId").GetString());
        Assert.Equal("EXEC-001", body.RootElement.GetProperty("ExecutionId").GetString());
        Assert.Equal("Complete", body.RootElement.GetProperty("Status").GetString());
    }

    [Fact]
    public void Tracked_no_op_requests_coordination_without_invalidating_unchanged_summary()
    {
        var factory = new ProductionNotificationFactory();
        var intent = Assert.Single(factory.Create(Commit(new("OP-001", "EXEC-001"), changed: false)));
        using var body = JsonDocument.Parse(intent.Body);
        Assert.Equal("PackageCoordinationRequested", body.RootElement.GetProperty("MessageKind").GetString());
        Assert.Equal(5, body.RootElement.GetProperty("ComponentRevision").GetInt64());
        Assert.Empty(factory.Create(Commit(OperationIdentity.Untracked, changed: false)));
        Assert.Equal(2, factory.Create(Commit(new("OP-001"), changed: false, status: "Failed")).Count);
    }

    [Fact]
    public async Task Processor_span_carries_business_correlation_for_replayed_batches()
    {
        Activity? captured = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == HarmonizerDiagnostics.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity => { if (activity.OperationName == "harmonizer.production.batch") captured = activity; }
        };
        ActivitySource.AddActivityListener(listener);
        var adapter = new ParquetProductionArtifactStore(new MemoryObjects(), new MemoryObjects(), new(), Fixtures.Options, Fixtures.Telemetry<ParquetProductionArtifactStore>());
        var processor = new ProductionBatchProcessor(new CompletedBatch(), adapter, new(), Fixtures.Options,
            Fixtures.Telemetry<ProductionBatchProcessor>(), Fixtures.Telemetry<Harmonizer.Core.Operations.AsyncHeartbeat>());
        await processor.ProcessAsync(Fixtures.Work() with { Operation = new("OP-001", "EXEC-001") }, default);
        Assert.NotNull(captured);
        Assert.Equal("OP-001", captured.GetTagItem("cat.operation.id"));
        Assert.Equal("EXEC-001", captured.GetTagItem("cat.execution.id"));
        Assert.Equal("harmonizer", captured.GetTagItem("cat.pipeline.stage"));
        Assert.Equal(2, captured.GetTagItem("cat.windfarm.id"));
        Assert.Equal("Production", captured.GetTagItem("cat.source.type"));
    }

    private sealed class CompletedBatch : IProductionControlStateRepository
    {
        public Task RegisterBatchAsync(ProductionWork work, CancellationToken token) => Task.CompletedTask;
        public Task<bool> IsItemAppliedAsync(BatchIdentity batch, int wtgId, CancellationToken token) => Task.FromResult(true);
        public Task<ComponentLease> ClaimAsync(WtgDayKey key, BatchIdentity batch, Guid ownerId, CancellationToken token) => throw new NotSupportedException();
        public Task RenewAsync(ComponentLease lease, CancellationToken token) => throw new NotSupportedException();
        public Task CommitAsync(ComponentCommit commit, CancellationToken token) => throw new NotSupportedException();
        public Task FailAsync(ComponentLease lease, ProductionWork work, string errorCode, CancellationToken token) => throw new NotSupportedException();
    }
}
