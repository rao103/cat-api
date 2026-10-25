using System.Text.Json;
using CAT.Messaging.Contracts;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Adapters;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Parquet;
using Parquet.Serialization;
using Xunit;

namespace Harmonizer.UnitTests;

public sealed class OutageContractTests
{
    [Theory]
    [InlineData("outage-events.parquet", 41, false)]
    [InlineData("outage-deleted-events.parquet", 1, true)]
    public async Task Actual_supplied_parquet_preserves_identity_string_codes_and_source_precision(string file, int expected, bool deleted)
    {
        await using var stream = File.OpenRead(Path.Combine(AppContext.BaseDirectory, "Fixtures", file));
        int groups;
        await using (var reader = await ParquetReader.CreateAsync(stream, leaveStreamOpen: true)) groups = reader.RowGroupCount;
        var count = 0;
        var mapper = new NormalizedOutageRowMapper();
        for (var group = 0; group < groups; group++)
        {
            stream.Position = 0;
            var data = await ParquetSerializer.DeserializeUntypedAsync(stream, rowGroupIndex: group);
            foreach (var raw in data.Data)
            {
                var farmName = Assert.IsType<string>(raw["WindfarmId"]);
                var turbineName = Assert.IsType<string>(raw["WtgId"]);
                var change = mapper.Map(raw, new(2, farmName), [new(11, turbineName)]);
                Assert.Equal(Assert.IsType<string>(raw["EventId"]), change.Key.OutageClassificationId);
                Assert.Equal(DateTime.SpecifyKind(Assert.IsType<DateTime>(raw["LastUpdatedDate"]), DateTimeKind.Utc).Ticks, change.LastUpdatedUtc.Ticks);
                Assert.Equal(deleted, change is OutageDeletion);
                var output = OutageParquetRow.From(new(2, farmName), new(new(change), 1, []));
                Assert.Equal(raw.GetValueOrDefault("AlarmCode") as string, output.AlarmCode);
                Assert.Equal(raw.GetValueOrDefault("Comment") as string, output.Comment);
                Assert.Equal(raw.GetValueOrDefault("OutageId") as string, output.OutageId);
                count++;
            }
        }
        Assert.Equal(expected, count);
    }

    [Fact]
    public async Task Invalid_member_hash_and_wtg_name_are_rejected()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([OutagePipelineFixture.Row()]);
        var member = work.Message.Members[0] with { MemberKey = new('0', 64) };
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(
            JsonSerializer.Serialize(work.Message with { Members = [member] }, HarmonizerJson.Options), default));
        member = work.Message.Members[0] with { WtgName = "wrong" };
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(
            JsonSerializer.Serialize(work.Message with { Members = [member] }, HarmonizerJson.Options), default));
    }

    [Fact]
    public async Task Empty_snapshot_requires_frozen_scope_roster_and_half_open_utc_interval()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.EmptySnapshotAsync();
        var scope = work.Message.SnapshotScope!;
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(JsonSerializer.Serialize(
            work.Message with { SnapshotScope = scope with { WtgIds = [] } }, HarmonizerJson.Options), default));
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(JsonSerializer.Serialize(
            work.Message with { SnapshotScope = scope with { EndExclusiveUtc = scope.StartInclusiveUtc } }, HarmonizerJson.Options), default));
    }

    [Fact]
    public async Task Authoritative_snapshot_requires_source_cutoff_and_receipt_only_rejects_one()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.EmptySnapshotAsync();
        var authoritative = work.Message with
        {
            ReconciliationMode = EventReconciliationModes.Authoritative,
            SourceVersionCutoffUtc = null
        };
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(
            JsonSerializer.Serialize(authoritative, HarmonizerJson.Options), default));

        var receiptOnlyWithCutoff = work.Message with { SourceVersionCutoffUtc = DateTimeOffset.UtcNow };
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(
            JsonSerializer.Serialize(receiptOnlyWithCutoff, HarmonizerJson.Options), default));
    }

    [Fact]
    public async Task Payload_and_tombstone_mismatch_cannot_be_acknowledged()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([OutagePipelineFixture.Row(deleted: true)]);
        var body = JsonSerializer.Serialize(work.Message with { PayloadKind = "OutageEvents" }, HarmonizerJson.Options);
        work = await fixture.Reader.ReadAsync(body, default);
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Processor.ProcessAsync(work, default));
        Assert.DoesNotContain(work.Batch, fixture.Control.Canonical);
    }

    [Fact]
    public async Task Operation_id_conflict_and_duplicate_json_fields_are_rejected()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([OutagePipelineFixture.Row()]);
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(JsonSerializer.Serialize(
            work.Message with { OriginalMessageBody = "{\"OperationId\":\"different\"}" }, HarmonizerJson.Options), default));
        var body = work.WireBody.Replace("\"SourceType\":\"Outage\"", "\"SourceType\":\"Outage\",\"SourceType\":\"Outage\"", StringComparison.Ordinal);
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Reader.ReadAsync(body, default));
    }
}
