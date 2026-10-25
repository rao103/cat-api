using Harmonizer.Core.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Models;

/// <summary>A daily projection retains the canonical interval and clips only its calculation segment.</summary>
public sealed record OutageDaySegment(
    OutageKey Key,
    TurbineIdentity Turbine,
    DateTime StartUtc,
    DateTime EndUtc,
    DateTime OriginalStartUtc,
    DateTime? OriginalEndUtc,
    DateTime LastUpdatedUtc,
    OutageAttributes Attributes);
