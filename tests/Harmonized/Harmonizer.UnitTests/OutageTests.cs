using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Operations;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Adapters;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Operations;
using Xunit;

namespace Harmonizer.UnitTests;

public sealed class OutageTests
{
    private readonly OutageMergeService merger = new();
    private readonly OutageFootprintPlanner planner = new(new EventFootprintPlanner());
    private readonly OutageDayMaterializer materializer = new();
    private static readonly DateTime Start = new(2026, 7, 18, 4, 57, 29, DateTimeKind.Utc);
    private static readonly DateOnly FirstDay = new(2026, 7, 18);
    private static readonly OutageKey Key = new(2, "WTG-ACC-wf01T001-260718045729");

    private static OutageAttributes Attributes(object? value = null) =>
        new(new Dictionary<string, JsonElement> { ["OutageID"] = JsonSerializer.SerializeToElement(value ?? "description-only") });

    private static OutageEvent Event(int version = 1, DateTime? end = null, int wtgId = 11) =>
        new(Key, new(wtgId, $"T{wtgId - 10:000}"), Start.AddDays(version), Attributes(), Start, end);

    private static WtgDayKey Day(int offset = 0, int wtgId = 11) => new(2, wtgId, FirstDay.AddDays(offset), "Outage");

    [Fact]
    public void Replay_is_a_no_op_and_equal_versions_compare_attribute_content()
    {
        var state = merger.Merge(null, Event()).State;
        var replay = merger.Merge(state, Event());
        Assert.False(replay.Changed);
        Assert.Same(state, replay.State);
    }

    [Fact]
    public void Older_source_change_cannot_replace_a_newer_interval()
    {
        var state = merger.Merge(null, Event(2, Start.AddHours(2))).State;
        var older = merger.Merge(state, Event(1, Start.AddHours(1)));
        Assert.False(older.Changed);
        Assert.Same(state, older.State);
    }

    [Fact]
    public void Equal_timestamp_with_different_content_is_a_conflict()
    {
        var state = merger.Merge(null, Event()).State;
        Assert.Throws<SourceConflictException>(() => merger.Merge(state, Event() with { Attributes = Attributes("changed") }));
        Assert.Throws<SourceConflictException>(() => merger.Merge(state, Event() with { EndUtc = Start.AddHours(1) }));
    }

    [Fact]
    public void Deletion_retains_its_version_and_blocks_stale_resurrection_in_either_delivery_order()
    {
        var outage = Event();
        var deletion = new OutageDeletion(Key, outage.Turbine, Start.AddDays(2), Attributes());
        var live = merger.Merge(null, outage).State;
        var deleted = merger.Merge(live, deletion).State;
        Assert.True(deleted.IsDeleted);
        Assert.Null(deleted.CurrentEvent);
        Assert.Equal(deletion.LastUpdatedUtc, deleted.LastUpdatedUtc);
        Assert.False(merger.Merge(deleted, outage).Changed);
        var deletionFirst = merger.Merge(null, deletion).State;
        Assert.False(merger.Merge(deletionFirst, outage).Changed);
    }

    [Fact]
    public void Open_end_stays_null_and_is_included_in_future_calculation_days()
    {
        var state = merger.Merge(null, Event()).State;
        Assert.Null(state.CurrentEvent?.EndUtc);
        Assert.Equal(3, planner.Plan(state, [], FirstDay.AddDays(2)).Count);
        var future = new WtgDayKey(2, 11, new(2030, 1, 1), "Outage");
        var segment = Assert.Single(materializer.Build(future, [state]));
        Assert.Null(segment.OriginalEndUtc);
        Assert.Equal(TimeSpan.FromDays(1), segment.EndUtc - segment.StartUtc);
        Assert.Throws<ArgumentException>(() => planner.Plan(state, [], FirstDay.AddDays(10), maximumDays: 3));
    }

    [Fact]
    public void Closing_an_open_event_rebuilds_old_days_and_clears_days_beyond_the_end()
    {
        var open = merger.Merge(null, Event()).State;
        var oldDays = planner.Plan(open, [], FirstDay.AddDays(4));
        var closed = merger.Merge(open, Event(2, Start.Date.AddDays(2).AddHours(14))).State;
        Assert.Equal(oldDays, planner.Plan(closed, oldDays, FirstDay.AddDays(4)));
        Assert.Single(materializer.Build(Day(2), [closed]));
        Assert.Empty(materializer.Build(Day(3), [closed]));
        Assert.Empty(materializer.Build(Day(4), [closed]));
    }

    [Fact]
    public void Deletion_schedules_old_footprint_without_needing_an_interval_in_the_tombstone()
    {
        var outage = Event();
        var state = merger.Merge(null, new OutageDeletion(Key, outage.Turbine, Start.AddDays(2), Attributes())).State;
        var oldDays = new EventDay[] { new(11, FirstDay), new(11, FirstDay.AddDays(1)) };
        Assert.Equal(oldDays, planner.Plan(state, oldDays, FirstDay.AddDays(10)));
        Assert.Empty(materializer.Build(Day(), [state]));
    }

    [Fact]
    public void Turbine_move_schedules_both_old_and_new_members()
    {
        var previous = new EventDay[] { new(11, FirstDay) };
        var moved = merger.Merge(null, Event(end: Start.AddHours(1), wtgId: 12)).State;
        Assert.Equal(new EventDay[] { new(11, FirstDay), new(12, FirstDay) }, planner.Plan(moved, previous, FirstDay));
        Assert.Empty(materializer.Build(Day(), [moved]));
        Assert.Single(materializer.Build(Day(wtgId: 12), [moved]));
    }

    [Fact]
    public void Start_month_move_keeps_the_classification_key_and_cleans_old_days()
    {
        var old = merger.Merge(null, Event(end: Start.AddHours(1))).State;
        var next = Event(2) with { StartUtc = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc), EndUtc = new(2026, 8, 2, 0, 0, 0, DateTimeKind.Utc) };
        var moved = merger.Merge(old, next).State;
        var affected = planner.Plan(moved, [new(11, FirstDay)], new(2026, 8, 2));
        Assert.Equal(new EventDay[] { new(11, FirstDay), new(11, new(2026, 8, 1)) }, affected);
        Assert.Equal(new DateOnly(2026, 8, 1), moved.CurrentEvent?.StartMonth);
        Assert.Equal(old.Key, moved.Key);
    }

    [Fact]
    public void Midnight_end_is_exclusive_and_daily_segments_keep_original_interval()
    {
        var midnight = FirstDay.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc);
        var state = merger.Merge(null, Event(end: midnight)).State;
        Assert.Single(planner.Plan(state, [], FirstDay.AddDays(1)));
        var segment = Assert.Single(materializer.Build(Day(), [state]));
        Assert.Equal(Start, segment.StartUtc);
        Assert.Equal(midnight, segment.EndUtc);
        Assert.Equal(Start, segment.OriginalStartUtc);
        Assert.Empty(materializer.Build(Day(1), [state]));
    }

    [Fact]
    public void Complete_day_preserves_unrelated_overlapping_classifications()
    {
        var first = merger.Merge(null, Event(end: Start.AddHours(1))).State;
        var second = merger.Merge(null, Event(end: Start.AddHours(2)) with { Key = new(2, "another-classification") }).State;
        var deleted = merger.Merge(first, new OutageDeletion(Key, first.LatestChange.Turbine, Start.AddDays(2), Attributes())).State;
        Assert.Equal(2, materializer.Build(Day(), [first, second]).Count);
        Assert.Equal("another-classification", Assert.Single(materializer.Build(Day(), [deleted, second])).Key.OutageClassificationId);
        Assert.Throws<SourceConflictException>(() => materializer.Build(Day(), [first, first]));
    }

    [Fact]
    public void Invalid_interval_or_local_timestamp_is_rejected()
    {
        Assert.Throws<WorkValidationException>(() => merger.Merge(null, Event(end: Start)));
        Assert.Throws<WorkValidationException>(() => merger.Merge(null, Event() with { LastUpdatedUtc = DateTime.SpecifyKind(Start, DateTimeKind.Unspecified) }));
        Assert.Throws<WorkValidationException>(() => merger.Merge(null, Event() with { EndUtc = DateTime.SpecifyKind(Start.AddHours(1), DateTimeKind.Local) }));
    }

    [Fact]
    public void Descriptive_fields_can_change_without_changing_the_footprint()
    {
        var previous = merger.Merge(null, Event(end: Start.AddHours(1))).State;
        var updated = merger.Merge(previous, Event(2, Start.AddHours(1)) with { Attributes = Attributes(new { NewField = "kept", OutageID = "another-description" }) }).State;
        Assert.Equal(planner.Plan(previous, [], FirstDay), planner.Plan(updated, [], FirstDay));
        Assert.Equal(previous.Key, updated.Key);
    }

    [Fact]
    public void Raw_mapping_preserves_attributes_precision_null_end_and_classification_identity()
    {
        var raw = RawRow();
        using var document = JsonDocument.Parse("{\"extra\":{\"count\":3},\"nullable\":null}");
        raw["FutureField"] = document.RootElement;
        var outage = new OutageRawRowMapper().MapEvent(raw, new(2, "wf01"), new(11, "T001"));
        document.Dispose();
        Assert.Equal(Key, outage.Key);
        Assert.Null(outage.EndUtc);
        Assert.Equal(DateTimeKind.Utc, outage.LastUpdatedUtc.Kind);
        Assert.Equal(2507390, outage.LastUpdatedUtc.Ticks % TimeSpan.TicksPerSecond);
        Assert.Equal("descriptive-parent", outage.Attributes.Values["OutageID"].GetString());
        Assert.Equal("wf01", outage.Attributes.Values["SiteRDSPPID"].GetString());
        Assert.Equal("T001", outage.Attributes.Values["WtgRDSPPID"].GetString());
        Assert.Equal(3, outage.Attributes.Values["FutureField"].GetProperty("extra").GetProperty("count").GetInt32());
        Assert.Equal(JsonValueKind.Null, outage.Attributes.Values["OutageClassificationComment"].ValueKind);
        Assert.DoesNotContain("OutageClassificationId", outage.Attributes.Values.Keys);
    }

    [Fact]
    public void Deleted_raw_schema_uses_classification_id_casing_and_does_not_require_dates()
    {
        var raw = RawRow();
        raw.Remove("OutageClassificationStartUTC");
        raw.Remove("OutageClassificationEndUTC");
        var classificationId = raw["OutageClassificationId"];
        raw.Remove("OutageClassificationId");
        raw["OutageClassificationID"] = classificationId;
        var deleted = new OutageRawRowMapper().MapDeletion(raw, new(2, "wf01"), new(11, "T001"));
        Assert.Equal(Key, deleted.Key);
        Assert.Equal("descriptive-parent", deleted.Attributes.Values["OutageID"].GetString());
    }

    [Fact]
    public void Supplied_closed_raw_interval_maps_to_the_expected_utc_times()
    {
        var raw = RawRow();
        raw["OutageClassificationEndUTC"] = JsonSerializer.SerializeToElement("2026-07-18 05:27:57.0000000");
        var outage = new OutageRawRowMapper().MapEvent(raw, new(2, "wf01"), new(11, "T001"));
        Assert.Equal(Start, outage.StartUtc);
        Assert.Equal(new DateTime(2026, 7, 18, 5, 27, 57, DateTimeKind.Utc), outage.EndUtc);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Internal_state_round_trip_preserves_interval_attributes_and_deletion_kind(bool deleted)
    {
        var outage = Event();
        OutageChange change = deleted
            ? new OutageDeletion(Key, outage.Turbine, Start.AddDays(2), Attributes()) : outage;
        var state = new OutageState(change);
        var json = JsonSerializer.Serialize(state, HarmonizerJson.Options);
        var restored = JsonSerializer.Deserialize<OutageState>(json, HarmonizerJson.Options)
            ?? throw new InvalidOperationException("Missing serialized state.");
        Assert.Equal(state.Key, restored.Key);
        Assert.Equal(state.LastUpdatedUtc, restored.LastUpdatedUtc);
        Assert.Equal(deleted, restored.IsDeleted);
        Assert.True(state.LatestChange.Attributes.ContentEquals(restored.LatestChange.Attributes));
        if (!deleted)
        {
            Assert.Equal(Start, restored.CurrentEvent?.StartUtc);
            Assert.Null(restored.CurrentEvent?.EndUtc);
        }
    }

    [Fact]
    public void Raw_mapper_rejects_missing_core_fields_mismatched_names_and_ambiguous_casing()
    {
        var mapper = new OutageRawRowMapper();
        var raw = RawRow();
        Assert.Throws<WorkValidationException>(() => mapper.MapEvent(raw, new(2, "other-farm"), new(11, "T001")));
        raw.Remove("OutageClassificationEndUTC");
        Assert.Throws<WorkValidationException>(() => mapper.MapEvent(raw, new(2, "wf01"), new(11, "T001")));
        raw = RawRow();
        raw["OutageClassificationID"] = raw["OutageClassificationId"];
        Assert.Throws<WorkValidationException>(() => mapper.MapDeletion(raw, new(2, "wf01"), new(11, "T001")));
    }

    [Fact]
    public void Maximum_calendar_date_does_not_overflow_the_shared_footprint_loop()
    {
        var shared = new EventFootprintPlanner();
        var max = new DateTime(9999, 12, 31, 0, 0, 0, DateTimeKind.Utc);
        Assert.Single(shared.Plan([], new([11], max, null), DateOnly.MaxValue));
        Assert.Equal(DateOnly.MaxValue, shared.AdvanceContiguous(DateOnly.MaxValue, DateOnly.MaxValue, new HashSet<DateOnly> { DateOnly.MaxValue }));
    }

    private static Dictionary<string, JsonElement> RawRow() => new()
    {
        ["SiteRDSPPID"] = JsonSerializer.SerializeToElement("wf01"),
        ["WtgRDSPPID"] = JsonSerializer.SerializeToElement("T001"),
        ["OutageID"] = JsonSerializer.SerializeToElement("descriptive-parent"),
        ["OutageClassificationId"] = JsonSerializer.SerializeToElement(Key.OutageClassificationId),
        ["OutageClassificationStartUTC"] = JsonSerializer.SerializeToElement("2026-07-18 04:57:29.0000000"),
        ["OutageClassificationEndUTC"] = JsonSerializer.SerializeToElement<string?>(null),
        ["OutageClassificationComment"] = JsonSerializer.SerializeToElement<string?>(null),
        ["OutageClassificationAlarmCode"] = JsonSerializer.SerializeToElement(152310),
        ["LastUpdatedUtc"] = JsonSerializer.SerializeToElement("2026-07-18 09:14:21.2507390")
    };
}
