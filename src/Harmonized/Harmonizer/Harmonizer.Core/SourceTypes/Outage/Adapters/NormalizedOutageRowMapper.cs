using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;
using Harmonizer.Core.SourceTypes.Outage.Operations;

namespace Harmonizer.Core.SourceTypes.Outage.Adapters;

/// <summary>Maps the verified normalized Ingestor schema; tombstone placeholder intervals are ignored.</summary>
public sealed class NormalizedOutageRowMapper
{
    private static readonly HashSet<string> IdentityColumns = new(StringComparer.Ordinal)
    { "EventId", "WindfarmId", "WtgId", "WindfarmName", "WtgName", "OriginalStartUtc", "OriginalEndUtc", "IsTombstone", "LastUpdatedDate", "SourceWatermarkUtc" };

    public OutageChange Map(IDictionary<string, object> row, WindfarmIdentity farm, IReadOnlyList<TurbineIdentity> turbines)
    {
        var eventId = RequiredString(row, "EventId");
        if (!Matches(row["WindfarmId"], farm.WindfarmId, farm.WindfarmName))
            throw new WorkValidationException("Raw Outage farm identity does not match the numeric ID/text name.");
        var turbine = turbines.SingleOrDefault(identity => Matches(row["WtgId"], identity.WtgId, identity.WtgName))
            ?? throw new WorkValidationException("Raw Outage WTG identity is unknown or belongs to another farm.");
        if (row.ContainsKey("WindfarmName") || row.ContainsKey("WtgName"))
        {
            if (row["WindfarmId"] is not int || row["WtgId"] is not int ||
                RequiredString(row, "WindfarmName") != farm.WindfarmName || RequiredString(row, "WtgName") != turbine.WtgName)
                throw new WorkValidationException("Outage schema 2 ID/name pairs disagree with the catalog.");
        }
        if (row["IsTombstone"] is not bool deleted)
            throw new WorkValidationException("IsTombstone must be a Parquet boolean.");
        var attributes = new OutageAttributes(row.Where(pair => !IdentityColumns.Contains(pair.Key))
            .ToDictionary(pair => pair.Key, pair => JsonSerializer.SerializeToElement(pair.Value), StringComparer.Ordinal));
        var key = new OutageKey(farm.WindfarmId, eventId);
        var updated = Timestamp(row, "LastUpdatedDate");
        OutageChange change = deleted
            ? new OutageDeletion(key, turbine, updated, attributes)
            : new OutageEvent(key, turbine, updated, attributes, Timestamp(row, "OriginalStartUtc"),
                !row.TryGetValue("OriginalEndUtc", out var end) || end is null ? null : Timestamp(row, "OriginalEndUtc"));
        OutageRecordValidator.Validate(change);
        return change;
    }

    private static bool Matches(object value, int id, string name) => value switch
    {
        int number => number == id,
        string text => string.Equals(text, name, StringComparison.Ordinal),
        _ => false
    };

    private static string RequiredString(IDictionary<string, object> row, string column) =>
        row.TryGetValue(column, out var value) && value is string text && !string.IsNullOrWhiteSpace(text) ? text :
        throw new WorkValidationException($"{column} must contain a nonblank string.");

    private static DateTime Timestamp(IDictionary<string, object> row, string column) =>
        row[column] is DateTime time && time.Kind != DateTimeKind.Local ? DateTime.SpecifyKind(time, DateTimeKind.Utc) :
        throw new WorkValidationException($"{column} must contain a UTC timestamp.");
}
