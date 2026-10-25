using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

/// <summary>Pure per-classification merge. Extraction order never replaces source timestamp ordering.</summary>
public sealed class OutageMergeService
{
    public OutageMergeResult Merge(OutageState? previous, OutageChange change)
    {
        ArgumentNullException.ThrowIfNull(change);
        OutageRecordValidator.Validate(change);
        if (previous is null)
        {
            // A deletion received before its older create still establishes a durable version barrier.
            return new(new(change), true);
        }

        OutageRecordValidator.Validate(previous.LatestChange);
        if (previous.Key != change.Key)
        {
            throw new WorkValidationException("Outage changes must be merged by the same farm and classification key.");
        }

        if (change.LastUpdatedUtc < previous.LastUpdatedUtc)
        {
            return new(previous, false);
        }

        if (change.LastUpdatedUtc == previous.LastUpdatedUtc)
        {
            if (!HasSameContent(previous.LatestChange, change))
            {
                throw new SourceConflictException("Equal Outage source timestamps contain different content.");
            }

            return new(previous, false);
        }

        return new(new(change), true);
    }

    private static bool HasSameContent(OutageChange previous, OutageChange next)
    {
        if (previous.GetType() != next.GetType() || previous.Turbine != next.Turbine ||
            !previous.Attributes.ContentEquals(next.Attributes))
        {
            return false;
        }

        return previous is not OutageEvent oldEvent || next is OutageEvent newEvent &&
            oldEvent.StartUtc == newEvent.StartUtc && oldEvent.EndUtc == newEvent.EndUtc;
    }
}
