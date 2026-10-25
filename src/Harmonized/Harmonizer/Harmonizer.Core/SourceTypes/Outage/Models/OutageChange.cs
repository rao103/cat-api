using Harmonizer.Core.Models;
using System.Text.Json.Serialization;

namespace Harmonizer.Core.SourceTypes.Outage.Models;

public sealed record OutageKey(int WindfarmId, string OutageClassificationId);

[JsonPolymorphic(TypeDiscriminatorPropertyName = "ChangeKind")]
[JsonDerivedType(typeof(OutageEvent), "Upsert")]
[JsonDerivedType(typeof(OutageDeletion), "Delete")]
public abstract record OutageChange(
    OutageKey Key,
    TurbineIdentity Turbine,
    DateTime LastUpdatedUtc,
    OutageAttributes Attributes);

public sealed record OutageEvent(
    OutageKey Key,
    TurbineIdentity Turbine,
    DateTime LastUpdatedUtc,
    OutageAttributes Attributes,
    DateTime StartUtc,
    DateTime? EndUtc) : OutageChange(Key, Turbine, LastUpdatedUtc, Attributes)
{
    public DateOnly StartMonth => new(StartUtc.Year, StartUtc.Month, 1);

    public bool Overlaps(DateTime startInclusiveUtc, DateTime endExclusiveUtc) =>
        StartUtc < endExclusiveUtc && (EndUtc is null || EndUtc > startInclusiveUtc);
}

public sealed record OutageDeletion(
    OutageKey Key,
    TurbineIdentity Turbine,
    DateTime LastUpdatedUtc,
    OutageAttributes Attributes) : OutageChange(Key, Turbine, LastUpdatedUtc, Attributes);
