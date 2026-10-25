using System.Security.Cryptography;
using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Production.Models;
namespace Harmonizer.Core.SourceTypes.Production.Operations;

/// <summary>Applies the source key and timestamp guarantees confirmed by the source owner.</summary>
public sealed class ProductionMergeService
{
    public MergeResult Merge(WtgDayKey key, ProductionSnapshot previous, IReadOnlyList<ProductionRow> changes)
    {
        var rows = new Dictionary<DateTime, ProductionRow>();
        foreach (var row in previous.Rows)
        {
            Validate(key, row);
            if (!rows.TryAdd(row.StartTimeInterval, row))
            {
                throw new SourceConflictException("Committed snapshot contains duplicate source keys.");
            }
        }
        var changed = false;
        foreach (var row in changes)
        {
            Validate(key, row);
            if (rows.TryGetValue(row.StartTimeInterval, out var existing))
            {
                if (row.LastUpdatedDate < existing.LastUpdatedDate)
                {
                    continue;
                }

                if (row.LastUpdatedDate == existing.LastUpdatedDate)
                {
                    if (row.Values != existing.Values || row.EndTimeInterval != existing.EndTimeInterval)
                    {
                        throw new SourceConflictException($"Equal source version differs for WTG {key.WtgId} at {row.StartTimeInterval:O}.");
                    }

                    continue;
                }
            }
            rows[row.StartTimeInterval] = row;
            changed = true;
        }
        var ordered = rows.Values.OrderBy(row => row.StartTimeInterval).ToArray();
        // Includes ordering evidence even if the business values did not change; excludes transport lineage.
        var canonical = ordered.Select(row => new
        {
            row.WtgId,
            row.StartTimeInterval,
            row.EndTimeInterval,
            row.LastUpdatedDate,
            row.Values
        });
        var hash = Convert.ToHexStringLower(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(canonical, HarmonizerJson.Options)));
        return new(new(ordered), hash, changed, ordered.Length == 144);
    }

    private static void Validate(WtgDayKey key, ProductionRow row)
    {
        if (row.WtgId != key.WtgId || string.IsNullOrWhiteSpace(row.WtgName))
        {
            throw new WorkValidationException("Production row does not match the turbine identity.");
        }

        if (row.StartTimeInterval.Kind != DateTimeKind.Utc || row.EndTimeInterval.Kind != DateTimeKind.Utc ||
            row.LastUpdatedDate.Kind != DateTimeKind.Utc)
        {
            throw new WorkValidationException("Production timestamps must be explicitly normalized as UTC.");
        }

        if (DateOnly.FromDateTime(row.StartTimeInterval) != key.DataDate ||
            row.StartTimeInterval.Ticks % TimeSpan.FromMinutes(10).Ticks != 0 ||
            row.EndTimeInterval != row.StartTimeInterval.AddMinutes(10))
        {
            throw new WorkValidationException("Production rows must occupy a ten-minute slot within the work UTC date.");
        }

        double?[] values = [row.Values.ActualProductionKWh, row.Values.PossibleProductionKWh,
            row.Values.LostProductionCapacitykWh, row.Values.ContractualPoPEColumn, row.Values.SiteAverageWindSpeedMps];
        if (values.Any(value => value.HasValue && !double.IsFinite(value.Value)))
        {
            throw new WorkValidationException("Production values must be finite or null.");
        }
    }
}
