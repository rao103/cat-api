using System.Text.Json.Serialization;

namespace Harmonizer.Core.SourceTypes.Outage.Models;

/// <summary>Retains the latest change, including a deletion's version, to prevent stale resurrection.</summary>
public sealed record OutageState(OutageChange LatestChange)
{
    public OutageKey Key => LatestChange.Key;
    public DateTime LastUpdatedUtc => LatestChange.LastUpdatedUtc;
    [JsonIgnore]
    public OutageEvent? CurrentEvent => LatestChange as OutageEvent;
    public bool IsDeleted => LatestChange is OutageDeletion;
}

public sealed record OutageMergeResult(OutageState State, bool Changed);
