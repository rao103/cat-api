using Harmonizer.Core.SourceTypes.Production.Operations;
using Harmonizer.Core.Models;
using Harmonizer.Core.Operations;
using Xunit;
namespace Harmonizer.UnitTests;

public sealed class ProductionMergeTests
{
    private readonly ProductionMergeService merger = new();
    [Fact]
    public void Independent_parts_survive_in_both_orders()
    {
        var first = merger.Merge(Fixtures.Key, new([]), [Fixtures.Row(0)]);
        var forward = merger.Merge(Fixtures.Key, first.Snapshot, [Fixtures.Row(1, extraction: 2)]);
        var reverseFirst = merger.Merge(Fixtures.Key, new([]), [Fixtures.Row(1, extraction: 2)]);
        var reverse = merger.Merge(Fixtures.Key, reverseFirst.Snapshot, [Fixtures.Row(0)]);
        Assert.Equal(forward.CanonicalHash, reverse.CanonicalHash);
        Assert.Equal(2, forward.Snapshot.Rows.Count);
        Assert.Equal(2, forward.Snapshot.Rows.Select(row => row.Origin.BlobPath).Distinct().Count());
    }
    [Fact]
    public void Duplicate_preserves_original_lineage_and_revision()
    {
        var first = merger.Merge(Fixtures.Key, new([]), [Fixtures.Row()]);
        var replay = merger.Merge(Fixtures.Key, first.Snapshot, [Fixtures.Row(extraction: 99)]);
        Assert.False(replay.Changed);
        Assert.Equal(first.CanonicalHash, replay.CanonicalHash);
        Assert.Equal(1, replay.Snapshot.Rows[0].Origin.ExtractionToken);
    }
    [Fact]
    public void Lower_extraction_token_can_carry_newer_record()
    {
        var result = merger.Merge(Fixtures.Key, new([Fixtures.Row(extraction: 100)]), [Fixtures.Row(version: 2, actual: 11, extraction: 1)]);
        Assert.Equal(11, result.Snapshot.Rows[0].Values.ActualProductionKWh);
    }
    [Fact]
    public void Higher_extraction_token_cannot_overwrite_newer_source_version()
    {
        var result = merger.Merge(Fixtures.Key, new([Fixtures.Row(version: 2, actual: 11)]), [Fixtures.Row(extraction: 100)]);
        Assert.False(result.Changed);
        Assert.Equal(11, result.Snapshot.Rows[0].Values.ActualProductionKWh);
    }
    [Fact]
    public void Equal_version_different_values_is_a_conflict() =>
        Assert.Throws<SourceConflictException>(() => merger.Merge(Fixtures.Key, new([Fixtures.Row()]), [Fixtures.Row(actual: 11)]));
    [Fact]
    public void Newer_version_with_equal_values_updates_ordering_evidence()
    {
        var old = merger.Merge(Fixtures.Key, new([]), [Fixtures.Row()]);
        var updated = merger.Merge(Fixtures.Key, old.Snapshot, [Fixtures.Row(version: 2)]);
        Assert.True(updated.Changed);
        Assert.NotEqual(old.CanonicalHash, updated.CanonicalHash);
    }
    [Fact]
    public void Sparse_new_day_is_partial_without_fabricated_zeroes()
    {
        var result = merger.Merge(Fixtures.Key, new([]), [Fixtures.Row(actual: null)]);
        Assert.False(result.Complete);
        Assert.Single(result.Snapshot.Rows);
        Assert.Null(result.Snapshot.Rows[0].Values.ActualProductionKWh);
    }
    [Fact]
    public void All_144_distinct_utc_slots_complete_a_day() =>
        Assert.True(merger.Merge(Fixtures.Key, new([]), Enumerable.Range(0, 144).Select(slot => Fixtures.Row(slot)).ToArray()).Complete);
    [Fact]
    public void Repeated_slots_do_not_count_towards_completeness() =>
        Assert.False(merger.Merge(Fixtures.Key, new([]), Enumerable.Repeat(Fixtures.Row(), 144).ToArray()).Complete);
    [Fact]
    public void Non_utc_timestamp_is_rejected() =>
        Assert.Throws<WorkValidationException>(() => merger.Merge(Fixtures.Key, new([]), [Fixtures.Row() with { LastUpdatedDate = DateTime.SpecifyKind(Fixtures.Start, DateTimeKind.Unspecified) }]));
    [Fact]
    public void Wrong_day_is_rejected() =>
        Assert.Throws<WorkValidationException>(() => merger.Merge(Fixtures.Key, new([]), [Fixtures.Row(144)]));
    [Fact]
    public void Misaligned_interval_is_rejected() =>
        Assert.Throws<WorkValidationException>(() => merger.Merge(Fixtures.Key, new([]), [Fixtures.Row() with { StartTimeInterval = Fixtures.Start.AddMinutes(1) }]));
    [Fact]
    public void Infinite_measurement_is_rejected() =>
        Assert.Throws<WorkValidationException>(() => merger.Merge(Fixtures.Key, new([]), [Fixtures.Row(actual: double.PositiveInfinity)]));
}
