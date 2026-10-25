using System.Globalization;
using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Adapters;

/// <summary>Maps the supplied logical Raw columns after authoritative farm/turbine identity resolution.</summary>
public sealed class OutageRawRowMapper
{
    private static readonly HashSet<string> CoreFields = new(StringComparer.OrdinalIgnoreCase)
    {
        "OutageClassificationId", "OutageClassificationStartUTC",
        "OutageClassificationEndUTC", "LastUpdatedUtc"
    };

    private static readonly string[] TimestampFormats =
    [
        "yyyy-MM-dd HH:mm:ss.FFFFFFF", "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK", "yyyy-MM-dd'T'HH:mm:ssK"
    ];

    public OutageEvent MapEvent(IReadOnlyDictionary<string, JsonElement> raw,
        WindfarmIdentity windfarm, TurbineIdentity turbine)
    {
        var row = NormalizeColumns(raw);
        ValidateIdentity(row, windfarm, turbine);
        var start = ReadTimestamp(row, "OutageClassificationStartUTC");
        if (!row.TryGetValue("OutageClassificationEndUTC", out var endValue))
        {
            throw new WorkValidationException("Outage raw row is missing its nullable end column.");
        }

        var end = endValue.ValueKind == JsonValueKind.Null
            ? (DateTime?)null : ReadTimestamp(row, "OutageClassificationEndUTC");
        if (end <= start)
        {
            throw new WorkValidationException("Outage end must be null or strictly later than its start.");
        }

        return new(new(windfarm.WindfarmId, ReadString(row, "OutageClassificationId")), turbine,
            ReadTimestamp(row, "LastUpdatedUtc"), ReadAttributes(raw), start, end);
    }

    public OutageDeletion MapDeletion(IReadOnlyDictionary<string, JsonElement> raw,
        WindfarmIdentity windfarm, TurbineIdentity turbine)
    {
        var row = NormalizeColumns(raw);
        ValidateIdentity(row, windfarm, turbine);
        return new(new(windfarm.WindfarmId, ReadString(row, "OutageClassificationId")), turbine,
            ReadTimestamp(row, "LastUpdatedUtc"), ReadAttributes(raw));
    }

    private static Dictionary<string, JsonElement> NormalizeColumns(IReadOnlyDictionary<string, JsonElement> raw)
    {
        var columns = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        foreach (var pair in raw)
        {
            if (!columns.TryAdd(pair.Key, pair.Value))
            {
                throw new WorkValidationException("Outage raw row contains ambiguous column casing.");
            }
        }

        return columns;
    }

    private static void ValidateIdentity(IReadOnlyDictionary<string, JsonElement> row,
        WindfarmIdentity windfarm, TurbineIdentity turbine)
    {
        if (windfarm.WindfarmId <= 0 || turbine.WtgId <= 0 ||
            ReadString(row, "SiteRDSPPID") != windfarm.WindfarmName || ReadString(row, "WtgRDSPPID") != turbine.WtgName)
        {
            throw new WorkValidationException("Outage source names do not match the resolved numeric farm/turbine identities.");
        }
    }

    private static string ReadString(IReadOnlyDictionary<string, JsonElement> row, string column)
    {
        if (!row.TryGetValue(column, out var value) || value.ValueKind != JsonValueKind.String)
        {
            throw new WorkValidationException($"Outage requires a nonempty {column} field.");
        }

        var text = value.GetString();
        return !string.IsNullOrWhiteSpace(text)
            ? text : throw new WorkValidationException($"Outage requires a nonempty {column} field.");
    }

    private static DateTime ReadTimestamp(IReadOnlyDictionary<string, JsonElement> row, string column)
    {
        var value = ReadString(row, column);
        if (!DateTimeOffset.TryParseExact(value, TimestampFormats, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var timestamp))
        {
            throw new WorkValidationException($"Outage requires a valid UTC {column} timestamp.");
        }

        return timestamp.UtcDateTime;
    }

    private static OutageAttributes ReadAttributes(IReadOnlyDictionary<string, JsonElement> raw) =>
        new(raw.Where(pair => !CoreFields.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal));
}
