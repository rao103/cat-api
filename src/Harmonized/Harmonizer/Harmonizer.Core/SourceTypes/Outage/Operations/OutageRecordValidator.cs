using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

internal static class OutageRecordValidator
{
    internal static void Validate(OutageChange change)
    {
        if (change is not (OutageEvent or OutageDeletion) || change.Key is null || change.Key.WindfarmId <= 0 ||
            string.IsNullOrWhiteSpace(change.Key.OutageClassificationId) || change.Key.OutageClassificationId.Length > 1024 ||
            change.Key.OutageClassificationId.Any(char.IsControl) ||
            change.Turbine is null || change.Turbine.WtgId <= 0 || string.IsNullOrWhiteSpace(change.Turbine.WtgName) ||
            change.Attributes is null || change.LastUpdatedUtc.Kind != DateTimeKind.Utc)
        {
            throw new WorkValidationException("Outage requires a classification key, numeric identities and a UTC source version.");
        }

        if (change is OutageEvent outage &&
            (outage.StartUtc.Kind != DateTimeKind.Utc || outage.EndUtc is { Kind: not DateTimeKind.Utc } ||
             outage.EndUtc <= outage.StartUtc))
        {
            throw new WorkValidationException("Outage interval must use UTC with a null or strictly later end.");
        }
    }
}
