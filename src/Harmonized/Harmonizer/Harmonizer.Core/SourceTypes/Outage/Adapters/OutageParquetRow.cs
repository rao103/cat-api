using System.Text.Json;
using Harmonizer.Core.Models;
using Harmonizer.Core.Serialization;
using Harmonizer.Core.SourceTypes.Outage.Models;

namespace Harmonizer.Core.SourceTypes.Outage.Adapters;

/// <summary>Known descriptive fields are ordinary typed columns for SQL and UI consumption.</summary>
public sealed class OutageParquetRow
{
    public int SchemaVersion { get; set; } = 1;
    public int WindfarmId { get; set; }
    public string WindfarmName { get; set; } = "";
    public int WtgId { get; set; }
    public string WtgName { get; set; } = "";
    public string EventId { get; set; } = "";
    public string OutageClassificationId { get; set; } = "";
    public long EventRevision { get; set; }
    public bool IsTombstone { get; set; }
    public DateTime? OriginalStartUtc { get; set; }
    public DateTime? OriginalEndUtc { get; set; }
    public DateTime? SegmentStartUtc { get; set; }
    public DateTime? SegmentEndUtc { get; set; }
    public DateTime LastUpdatedDate { get; set; }
    public long LastUpdatedTicks { get; set; }
    public string? EventType { get; set; }
    public string? Status { get; set; }
    public string? SourceRevision { get; set; }
    public DateTime? SourceWatermarkUtc { get; set; }
    public string? OutageId { get; set; }
    public string? Comment { get; set; }
    public string? AlarmCode { get; set; }
    public string? AlarmText { get; set; }
    public string? Category { get; set; }
    public string? SubCategory { get; set; }
    public int? CanonicalStartYear { get; set; }
    public int? CanonicalStartMonth { get; set; }
    public long? CanonicalMonthRevision { get; set; }
    public long? CanonicalCommittedAttempt { get; set; }
    public string? CanonicalEventPath { get; set; }
    public string? ExtraAttributesJson { get; set; }

    private static readonly HashSet<string> KnownAttributes = new(StringComparer.Ordinal)
    { "EventType", "Status", "SourceRevision", "SourceWatermarkUtc", "OutageId", "Comment", "AlarmCode", "AlarmText", "Category", "SubCategory" };

    public static OutageParquetRow From(WindfarmIdentity farm, CanonicalOutage canonical)
    {
        var change = canonical.State.LatestChange;
        var attributes = change.Attributes.Values;
        var extras = attributes.Where(pair => !KnownAttributes.Contains(pair.Key)).ToDictionary(pair => pair.Key, pair => pair.Value);
        return new()
        {
            WindfarmId = farm.WindfarmId, WindfarmName = farm.WindfarmName,
            WtgId = change.Turbine.WtgId, WtgName = change.Turbine.WtgName,
            EventId = change.Key.OutageClassificationId, OutageClassificationId = change.Key.OutageClassificationId,
            EventRevision = canonical.EventRevision, IsTombstone = canonical.State.IsDeleted,
            OriginalStartUtc = canonical.State.CurrentEvent?.StartUtc, OriginalEndUtc = canonical.State.CurrentEvent?.EndUtc,
            LastUpdatedDate = change.LastUpdatedUtc, LastUpdatedTicks = change.LastUpdatedUtc.Ticks,
            EventType = Text(attributes, "EventType"), Status = Text(attributes, "Status"),
            SourceRevision = Text(attributes, "SourceRevision"), SourceWatermarkUtc = canonical.Origins
                .Where(origin => origin.SourceWatermarkUtc.HasValue).Select(origin => origin.SourceWatermarkUtc).Max(),
            OutageId = Text(attributes, "OutageId"), Comment = Text(attributes, "Comment"),
            AlarmCode = Text(attributes, "AlarmCode"), AlarmText = Text(attributes, "AlarmText"),
            Category = Text(attributes, "Category"), SubCategory = Text(attributes, "SubCategory"),
            ExtraAttributesJson = extras.Count == 0 ? null : JsonSerializer.Serialize(extras, HarmonizerJson.Options)
        };
    }

    public static OutageParquetRow FromDay(WindfarmIdentity farm, OutageDayReference reference)
    {
        var segment = reference.Segment;
        var row = From(farm, new(new(new OutageEvent(segment.Key, segment.Turbine, segment.LastUpdatedUtc,
            segment.Attributes, segment.OriginalStartUtc, segment.OriginalEndUtc)), reference.EventRevision, []));
        row.SegmentStartUtc = segment.StartUtc;
        row.SegmentEndUtc = segment.EndUtc;
        row.CanonicalStartYear = segment.OriginalStartUtc.Year;
        row.CanonicalStartMonth = segment.OriginalStartUtc.Month;
        row.CanonicalMonthRevision = reference.Canonical.Revision;
        row.CanonicalCommittedAttempt = reference.Canonical.Attempt;
        row.CanonicalEventPath = reference.Canonical.ParquetPath;
        return row;
    }

    private static string? Text(IReadOnlyDictionary<string, JsonElement> attributes, string name)
    {
        if (!attributes.TryGetValue(name, out var value) || value.ValueKind == JsonValueKind.Null) return null;
        if (value.ValueKind != JsonValueKind.String) throw new WorkValidationException($"{name} must remain a nullable text field.");
        return value.GetString();
    }

}
