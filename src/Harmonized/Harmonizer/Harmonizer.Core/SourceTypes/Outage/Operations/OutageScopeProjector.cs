using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

/// <summary>Scopes snapshot absence to its exact UTC interval and canonical revision.</summary>
public sealed class OutageScopeProjector
{
    public IReadOnlyList<OutageSnapshotCapture> EffectiveScope(OutageWork work, OutageCatalog catalog)
    {
        var scope = work.Message.SnapshotScope!;
        var result = new List<OutageSnapshotCapture>();
        foreach (var wtgId in scope.WtgIds)
        {
            var intervals = new List<OutageSnapshotCapture> { new(wtgId, scope.StartInclusiveUtc.UtcDateTime,
                scope.EndExclusiveUtc.UtcDateTime, work.Message.ExtractionToken, work.Batch.ArtifactId, work.Batch.BatchId) };
            foreach (var newer in (catalog.Captures ?? []).Where(capture => capture.WtgId == wtgId && capture.ExtractionToken > work.Message.ExtractionToken))
                intervals = intervals.SelectMany(interval => Subtract(interval, newer.StartInclusiveUtc, newer.EndExclusiveUtc)).ToList();
            result.AddRange(intervals);
        }
        return result;
    }

    public IEnumerable<OutageScopeExclusion> RestorePresence(OutageScopeExclusion exclusion, OutageSnapshotCapture scope)
    {
        if (scope.WtgId != exclusion.WtgId || scope.ExtractionToken < exclusion.CaptureToken)
        { yield return exclusion; yield break; }
        var wrapper = new OutageSnapshotCapture(exclusion.WtgId, exclusion.StartInclusiveUtc, exclusion.EndExclusiveUtc,
            exclusion.CaptureToken, "", "");
        foreach (var retained in Subtract(wrapper, scope.StartInclusiveUtc, scope.EndExclusiveUtc))
            yield return exclusion with { StartInclusiveUtc = retained.StartInclusiveUtc, EndExclusiveUtc = retained.EndExclusiveUtc };
    }

    private static IEnumerable<OutageSnapshotCapture> Subtract(OutageSnapshotCapture interval, DateTime start, DateTime end)
    {
        if (start >= interval.EndExclusiveUtc || end <= interval.StartInclusiveUtc)
        { yield return interval; yield break; }
        if (interval.StartInclusiveUtc < start) yield return interval with { EndExclusiveUtc = start };
        if (interval.EndExclusiveUtc > end) yield return interval with { StartInclusiveUtc = end };
    }

    public IReadOnlyList<OutageDaySegment> Apply(OutageDaySegment segment, long revision,
        IReadOnlyList<OutageScopeExclusion> exclusions)
    {
        var segments = new List<OutageDaySegment> { segment };
        foreach (var exclusion in exclusions.Where(exclusion => exclusion.EventId == segment.Key.OutageClassificationId &&
                     exclusion.EventRevision == revision && exclusion.WtgId == segment.Turbine.WtgId))
        {
            var next = new List<OutageDaySegment>();
            foreach (var current in segments)
            {
                if (exclusion.StartInclusiveUtc >= current.EndUtc || exclusion.EndExclusiveUtc <= current.StartUtc)
                { next.Add(current); continue; }
                if (current.StartUtc < exclusion.StartInclusiveUtc)
                    next.Add(current with { EndUtc = exclusion.StartInclusiveUtc });
                if (current.EndUtc > exclusion.EndExclusiveUtc)
                    next.Add(current with { StartUtc = exclusion.EndExclusiveUtc });
            }
            segments = next;
        }
        return segments;
    }
}
