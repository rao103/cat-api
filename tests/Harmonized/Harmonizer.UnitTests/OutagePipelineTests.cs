using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Xunit;
using static Harmonizer.UnitTests.OutagePipelineFixture;

namespace Harmonizer.UnitTests;

public sealed class OutagePipelineTests
{
    [Fact]
    public async Task Open_event_writes_typed_columns_and_exact_canonical_links()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([Row()]);
        await fixture.Processor.ProcessAsync(work, default);
        var day = Assert.Single(await fixture.DayAsync());
        Assert.Equal("classification-1", day.OutageClassificationId);
        Assert.Equal("0152310", day.AlarmCode);
        Assert.Null(day.Comment);
        Assert.Null(day.OriginalEndUtc);
        Assert.Equal(2026, day.CanonicalStartYear);
        Assert.Equal(7, day.CanonicalStartMonth);
        Assert.True(fixture.Objects.Output.ContainsKey(day.CanonicalEventPath!));
        Assert.Contains("startyear=2026/startmonth=07/revisions/revision=1/attempts/token=1", day.CanonicalEventPath);
        Assert.Equal(2, day.WindfarmId);
        Assert.Equal("WF01", day.WindfarmName);
        Assert.Equal(11, day.WtgId);
        Assert.Equal("T001", day.WtgName);
        Assert.Contains(work.Batch, fixture.Control.Completed);
        using var lineage = JsonDocument.Parse(fixture.Objects.Output[fixture.Control.Days[new(2, 11, new(2026, 7, 18), "Outage")].LineagePath]);
        Assert.Equal("operation-1", lineage.RootElement.GetProperty("OperationId").GetString());
    }

    [Fact]
    public async Task Moving_turbine_rebuilds_old_days_and_preserves_unrelated_contributors()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(), Row("other")]), default);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 2, wtg: "T002")]), default);
        Assert.Equal("other", Assert.Single(await fixture.DayAsync()).EventId);
        Assert.Equal("classification-1", Assert.Single(await fixture.DayAsync(12)).EventId);
    }

    [Fact]
    public async Task Shortening_and_deletion_rebuild_old_footprint_with_explicit_zero_row_parquet()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 2, end: Start.AddMinutes(20))]), default);
        Assert.Single(await fixture.DayAsync());
        Assert.Empty(await fixture.DayAsync(day: "2026-07-19"));
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 3, deleted: true, start: DateTime.MinValue)]), default);
        Assert.Empty(await fixture.DayAsync());
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        var canonical = Assert.Single((await fixture.Artifacts.ReadMonthAsync(Assert.Single(catalog.Months), default)).Events);
        Assert.True(canonical.State.IsDeleted);
        Assert.Equal(3, canonical.EventRevision);
    }

    [Fact]
    public async Task Delete_first_has_no_placeholder_start_month_and_prevents_stale_resurrection()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 2, deleted: true, start: DateTime.MinValue)]), default);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        Assert.Empty(fixture.Control.Days);
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        Assert.Equal("_tombstones", Assert.Single(catalog.Months).PartitionKey);
        Assert.True(Assert.Single((await fixture.Artifacts.ReadMonthAsync(catalog.Months[0], default)).Events).State.IsDeleted);
    }

    [Fact]
    public async Task Start_month_change_publishes_both_months_and_old_links_remain_readable()
    {
        var fixture = new OutagePipelineFixture();
        fixture.Clock.Now = new(2026, 8, 3, 12, 0, 0, TimeSpan.Zero);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(end: Start.AddHours(1))]), default);
        var historicalPath = Assert.Single(await fixture.DayAsync()).CanonicalEventPath!;
        var movedStart = new DateTime(2026, 8, 1, 1, 0, 0, DateTimeKind.Utc);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 2, start: movedStart, end: movedStart.AddHours(1))]), default);
        Assert.Empty(await fixture.DayAsync());
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        Assert.Equal(2, catalog.Months.Count);
        Assert.Empty((await fixture.Artifacts.ReadMonthAsync(catalog.Months.Single(month => month.PartitionKey == "2026-07"), default)).Events);
        Assert.Single((await fixture.Artifacts.ReadMonthAsync(catalog.Months.Single(month => month.PartitionKey == "2026-08"), default)).Events);
        Assert.True(fixture.Objects.Output.ContainsKey(historicalPath));
    }

    [Fact]
    public async Task Long_rebuild_is_durably_handed_off_and_resumes_in_bounded_pages()
    {
        var fixture = new OutagePipelineFixture(daysPerPass: 1);
        var work = await fixture.WorkAsync([Row()]);
        await fixture.Processor.ProcessAsync(work, default);
        Assert.Contains(work.Batch, fixture.Control.Canonical);
        Assert.DoesNotContain(work.Batch, fixture.Control.Completed);
        Assert.Single(fixture.Control.Days);
        await fixture.Lifecycle.ProcessFarmAsync(Farm, default);
        await fixture.Lifecycle.ProcessFarmAsync(Farm, default);
        Assert.Equal(3, fixture.Control.Days.Count);
        Assert.Contains(work.Batch, fixture.Control.Completed);
    }

    [Fact]
    public async Task Clock_continues_without_source_update_or_new_canonical_revision()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var catalogPath = fixture.Control.CatalogPath;
        fixture.Clock.Now = fixture.Clock.Now.AddDays(1);
        await fixture.Lifecycle.ProcessFarmAsync(Farm, default);
        var row = Assert.Single(await fixture.DayAsync(day: "2026-07-21"));
        Assert.Equal(1, row.EventRevision);
        Assert.Equal(catalogPath, fixture.Control.CatalogPath);
        var update = fixture.Control.Lifecycle.Values.Single();
        Assert.Equal("ClockContinuation", update.Plan.TriggerKind);
        Assert.False(update.Plan.Operation.IsTracked);
    }

    [Fact]
    public async Task Authoritative_snapshot_waits_for_receipts_then_clears_only_its_scope()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var snapshot = await fixture.EmptySnapshotAsync([Hash("expected-event-batch")], authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2)));
        fixture.Control.ExpectedReceiptsAvailable = false;
        var catalogPath = fixture.Control.CatalogPath;
        await Assert.ThrowsAsync<LeaseUnavailableException>(() => fixture.Processor.ProcessAsync(snapshot, default));
        Assert.Equal(catalogPath, fixture.Control.CatalogPath);
        fixture.Control.ExpectedReceiptsAvailable = true;
        await fixture.Processor.ProcessAsync(snapshot, default);
        Assert.Empty(await fixture.DayAsync());
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        Assert.NotEmpty(catalog.Exclusions);
        Assert.False(Assert.Single((await fixture.Artifacts.ReadMonthAsync(catalog.Months[0], default)).Events).State.IsDeleted);
        fixture.Clock.Now = fixture.Clock.Now.AddDays(1);
        await fixture.Lifecycle.ProcessFarmAsync(Farm, default);
        Assert.Single(await fixture.DayAsync(day: "2026-07-21"));
    }

    [Fact]
    public async Task Receipt_only_snapshot_waits_for_receipts_without_applying_absence()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var snapshot = await fixture.EmptySnapshotAsync([Hash("expected-event-batch")]);
        fixture.Control.ExpectedReceiptsAvailable = false;
        await Assert.ThrowsAsync<LeaseUnavailableException>(() => fixture.Processor.ProcessAsync(snapshot, default));
        fixture.Control.ExpectedReceiptsAvailable = true;
        await fixture.Processor.ProcessAsync(snapshot, default);
        Assert.Single(await fixture.DayAsync());
        Assert.Empty((await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default)).Exclusions);
        Assert.Contains(snapshot.Batch, fixture.Control.Completed);
    }

    [Fact]
    public async Task Late_snapshot_does_not_clear_a_later_extraction_event()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()], token: 200), default);
        await fixture.Processor.ProcessAsync(await fixture.EmptySnapshotAsync(token: 100, authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2))), default);
        Assert.Single(await fixture.DayAsync());
    }

    [Fact]
    public async Task Source_update_after_snapshot_restores_applicability()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        await fixture.Processor.ProcessAsync(await fixture.EmptySnapshotAsync(authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2))), default);
        Assert.Empty(await fixture.DayAsync());
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 3)], token: 101), default);
        Assert.Single(await fixture.DayAsync());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task In_scope_source_update_at_or_before_snapshot_cutoff_is_rejected(int version)
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        await fixture.Processor.ProcessAsync(await fixture.EmptySnapshotAsync(authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2))), default);

        var staleWork = await fixture.WorkAsync([Row(version: version)]);
        await Assert.ThrowsAsync<WorkValidationException>(() => fixture.Processor.ProcessAsync(staleWork, default));

        Assert.Empty(await fixture.DayAsync());
    }

    [Fact]
    public async Task Raw_read_failure_and_equal_version_conflict_are_not_empty_success()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([Row()]);
        fixture.Objects.Raw.Clear();
        await Assert.ThrowsAsync<KeyNotFoundException>(() => fixture.Processor.ProcessAsync(work, default));
        Assert.DoesNotContain(work.Batch, fixture.Control.Canonical);
        Assert.Empty(fixture.Control.Days);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var conflict = await fixture.WorkAsync([Row(wtg: "T002")]);
        await Assert.ThrowsAsync<SourceConflictException>(() => fixture.Processor.ProcessAsync(conflict, default));
        Assert.DoesNotContain(conflict.Batch, fixture.Control.Canonical);
        Assert.Single(await fixture.DayAsync());
    }

    [Fact]
    public async Task Completed_batch_replay_does_not_write_more_files()
    {
        var fixture = new OutagePipelineFixture();
        var work = await fixture.WorkAsync([Row()]);
        await fixture.Processor.ProcessAsync(work, default);
        var count = fixture.Objects.Output.Count;
        await fixture.Processor.ProcessAsync(work, default);
        Assert.Equal(count, fixture.Objects.Output.Count);
    }

    [Fact]
    public async Task Nonempty_snapshot_applies_canonical_children_before_reconciliation()
    {
        var fixture = new OutagePipelineFixture();
        var child = await fixture.WorkAsync([Row()], snapshot: true);
        await fixture.Processor.ProcessAsync(child, default);
        var message = child.Message with { WorkKind = "EventSnapshotReconciliation", BatchId = Hash("barrier"),
            EventIds = [], Members = [], ExpectedEventBatchIds = [child.Batch.BatchId],
            ReconciliationMode = CAT.Messaging.Contracts.EventReconciliationModes.ReceiptOnly };
        var scope = await fixture.Reader.ReadAsync(JsonSerializer.Serialize(message, HarmonizerJson.Options), default);
        await fixture.Processor.ProcessAsync(scope, default);
        Assert.Single(await fixture.DayAsync());
        Assert.Contains(scope.Batch, fixture.Control.Completed);
    }
    [Fact]
    public async Task Newer_snapshot_presence_restores_an_unchanged_source_event_and_older_absence_cannot_remove_it()
    {
        var fixture = new OutagePipelineFixture();
        var initial = await fixture.WorkAsync([Row()], token: 1);
        await fixture.Processor.ProcessAsync(initial, default);
        await fixture.Processor.ProcessAsync(await fixture.EmptySnapshotAsync(token: 100, authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2))), default);
        Assert.Empty(await fixture.DayAsync());
        var child = await fixture.WorkAsync([Row(version: 3)], snapshot: true, token: 200);
        await fixture.Processor.ProcessAsync(child, default);
        var message = child.Message with { WorkKind = "EventSnapshotReconciliation", BatchId = Hash("newer-presence"),
            EventIds = [], Members = [], ExpectedEventBatchIds = [child.Batch.BatchId],
            ReconciliationMode = CAT.Messaging.Contracts.EventReconciliationModes.Authoritative,
            SourceVersionCutoffUtc = new DateTimeOffset(Start.AddHours(4)) };
        var barrier = await fixture.Reader.ReadAsync(JsonSerializer.Serialize(message, HarmonizerJson.Options), default);
        await fixture.Processor.ProcessAsync(barrier, default);
        Assert.Single(await fixture.DayAsync());
        await fixture.Processor.ProcessAsync(await fixture.EmptySnapshotAsync(token: 150, authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(3))), default);
        Assert.Single(await fixture.DayAsync());
    }

    [Fact]
    public async Task Empty_incremental_observation_does_not_remove_existing_events()
    {
        var fixture = new OutagePipelineFixture();
        var initial = await fixture.WorkAsync([Row()]);
        await fixture.Processor.ProcessAsync(initial, default);
        var emptyPath = "raw/outage/empty.parquet";
        using (var stream = new MemoryStream())
        {
            await Parquet.Serialization.ParquetSerializer.SerializeAsync(Array.Empty<RawOutageTestRow>(), stream);
            fixture.Objects.Raw[emptyPath] = stream.ToArray();
        }
        var observed = initial.Message with { WorkKind = "ArtifactObserved", ArtifactId = Hash("empty-artifact"), BatchId = Hash("empty-batch"),
            Members = [], EventIds = [], RawPaths = [emptyPath] };
        var work = await fixture.Reader.ReadAsync(JsonSerializer.Serialize(observed, HarmonizerJson.Options), default);
        var catalogPath = fixture.Control.CatalogPath;
        await fixture.Processor.ProcessAsync(work, default);
        Assert.Equal(catalogPath, fixture.Control.CatalogPath);
        Assert.Single(await fixture.DayAsync());
        Assert.Contains(work.Batch, fixture.Control.Completed);
    }

    [Fact]
    public async Task Superseding_a_partially_materialized_generation_retains_outstanding_cleanup()
    {
        var fixture = new OutagePipelineFixture(daysPerPass: 1);
        var initial = await fixture.WorkAsync([Row()]);
        await fixture.Processor.ProcessAsync(initial, default);
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(version: 2, wtg: "T002", end: Start.AddHours(1))]), default);
        for (var i = 0; i < 5; i++) await fixture.Lifecycle.ProcessFarmAsync(Farm, default);
        Assert.Empty(await fixture.DayAsync());
        Assert.Single(await fixture.DayAsync(12));
        Assert.Contains(initial.Batch, fixture.Control.Completed);
        Assert.Empty(fixture.Control.Lifecycle.Values.Single().Plan.Pending);
    }

    [Fact]
    public async Task Empty_day_lineage_retains_the_tombstone_that_caused_cleanup()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var deletion = await fixture.WorkAsync([Row(version: 2, deleted: true)]);
        await fixture.Processor.ProcessAsync(deletion, default);
        var candidate = fixture.Control.Days[new(2, 11, new(2026, 7, 18), "Outage")];
        using var lineage = JsonDocument.Parse(fixture.Objects.Output[candidate.LineagePath]);
        Assert.Equal("classification-1", lineage.RootElement.GetProperty("EventId").GetString());
        Assert.Equal(deletion.Message.RawPaths[0], lineage.RootElement.GetProperty("RawPaths")[0].GetString());
        Assert.True(lineage.RootElement.GetProperty("TriggerEvidence").GetProperty("Event").GetProperty("State").GetProperty("IsDeleted").GetBoolean());
    }

    [Fact]
    public async Task Subsecond_interval_and_seventh_source_fraction_survive_parquet_and_snapshot_round_trips()
    {
        var fixture = new OutagePipelineFixture();
        var start = Start.AddTicks(2507397);
        var updated = Start.AddHours(1).AddTicks(1234567);
        var source = Row(start: start, end: start.AddTicks(7654321));
        source.LastUpdatedDate = updated;
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([source]), default);
        var row = Assert.Single(await fixture.DayAsync());
        Assert.Equal(start.Ticks, row.OriginalStartUtc!.Value.Ticks);
        Assert.Equal(start.AddTicks(7654321).Ticks, row.SegmentEndUtc!.Value.Ticks);
        Assert.Equal(updated.Ticks, row.LastUpdatedDate.Ticks);
        Assert.Equal(updated.Ticks, row.LastUpdatedTicks);
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        Assert.Equal(updated.Ticks, Assert.Single((await fixture.Artifacts.ReadMonthAsync(catalog.Months[0], default)).Events).State.LastUpdatedUtc.Ticks);
    }

    [Fact]
    public async Task Partial_day_snapshot_absence_clips_its_interval_and_preserves_other_turbines()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row(), Row("other", wtg: "T002")]), default);
        var empty = await fixture.EmptySnapshotAsync(authoritative: true,
            sourceVersionCutoffUtc: new DateTimeOffset(Start.AddHours(2)));
        var scope = empty.Message.SnapshotScope! with { WtgIds = [11],
            StartInclusiveUtc = new(2026,7,18,10,0,0,TimeSpan.Zero), EndExclusiveUtc = new(2026,7,18,11,0,0,TimeSpan.Zero) };
        var work = await fixture.Reader.ReadAsync(JsonSerializer.Serialize(empty.Message with { SnapshotScope = scope }, HarmonizerJson.Options), default);
        await fixture.Processor.ProcessAsync(work, default);
        var segments = await fixture.DayAsync();
        Assert.Equal(2, segments.Count);
        Assert.Equal(new DateTime(2026,7,18,10,0,0,DateTimeKind.Utc), segments[0].SegmentEndUtc);
        Assert.Equal(new DateTime(2026,7,18,11,0,0,DateTimeKind.Utc), segments[1].SegmentStartUtc);
        Assert.Single(await fixture.DayAsync(12));
    }

    [Fact]
    public async Task A_new_capture_watermark_does_not_turn_unchanged_source_content_into_a_conflict()
    {
        var fixture = new OutagePipelineFixture();
        await fixture.Processor.ProcessAsync(await fixture.WorkAsync([Row()]), default);
        var repeated = Row();
        repeated.SourceWatermarkUtc = Start.AddDays(1);
        var work = await fixture.WorkAsync([repeated]);
        await fixture.Processor.ProcessAsync(work, default);
        var catalog = await fixture.Artifacts.ReadCatalogAsync(fixture.Control.CatalogPath, default);
        Assert.Equal(1, Assert.Single(catalog.Months).Revision);
        Assert.Contains(work.Batch, fixture.Control.Completed);
    }

}
