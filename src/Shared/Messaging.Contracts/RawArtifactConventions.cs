namespace CAT.Messaging.Contracts;

/// <summary>Wire conventions, independent of physical source column names.</summary>
public static class RawArtifactConventions
{
    public const int EnvelopeVersion = 2;
    public const string MessageKind = "RawArtifactAvailable";
    public const string LastUpdatedUtc = "LastUpdatedUtc";
    public const string ProductionTimestampUtc = "ProductionTimestampUtc";
    public const string EventOverlapUtc = "EventOverlapUtc";
    public const string SelectedIntervals = "SelectedIntervals";
    public const string EventsOverlappingInterval = "EventsOverlappingInterval";
    public const int OutagePayloadSchemaVersion = 2;
    public const int ProductionPayloadSchemaVersion = 2;

    /// <summary>Attempt identity is the invariant extraction/lease token, scoped by source and windfarm.</summary>
    public static string AttemptId(long extractionToken) =>
        extractionToken.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Canonical container-relative object path, without a leading slash or URL credentials.</summary>
    public static void ValidatePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || path.Length > 2048 || path.Contains('\\') ||
            path.Contains('?') || path.Contains('#') || path.Contains(':') || path.Any(char.IsControl) ||
            path.Split('/').Any(segment => segment is "" or "." or ".."))
            throw new InvalidDataException("Invalid container-relative Raw artifact path.");
    }

    /// <summary>Represent a frozen inclusive maximum as a half-open boundary; never silently omit MaxValue.</summary>
    public static DateTime ExclusiveEnd(DateTime inclusiveMaximumUtc)
    {
        if (inclusiveMaximumUtc.Kind != DateTimeKind.Utc || inclusiveMaximumUtc == DateTime.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(inclusiveMaximumUtc), "A UTC maximum with a representable exclusive end is required.");
        return inclusiveMaximumUtc.AddTicks(1);
    }
}
