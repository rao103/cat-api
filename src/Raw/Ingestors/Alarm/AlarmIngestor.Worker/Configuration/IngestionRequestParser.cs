using CAT.Messaging.Contracts;
using System.Globalization;
using AlarmIngestor.Enums;
using AlarmIngestor.Models;

namespace AlarmIngestor.Worker.Configuration;

/// <summary>Parses the one-shot run request from configuration/environment (Kubernetes Job env vars).</summary>
public static class IngestionRequestParser
{
    public static IngestionRequestModel Parse(
        string? configuredWindfarmId, string? envWindfarmId, string? envRunId,
        string? envMode, string? envHistoricalStart, string? envHistoricalEnd, string? operationId = null)
    {
        OperationCorrelation.Validate(operationId);
        var windfarmId = envWindfarmId ?? configuredWindfarmId;
        ArgumentException.ThrowIfNullOrWhiteSpace(windfarmId, nameof(windfarmId));
        // extraction_cursor.windfarm_id is INTEGER; fail fast here rather than at the DB boundary.
        if (!int.TryParse(windfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedWindfarmId) || parsedWindfarmId <= 0)
            throw new ArgumentException("Windfarm identifier must be a positive integer.");

        var mode = string.Equals(envMode, "Historical", StringComparison.OrdinalIgnoreCase)
            ? IngestionMode.Historical
            : IngestionMode.Incremental;

        DateTime? start = null;
        DateTime? end = null;
        if (mode == IngestionMode.Historical)
        {
            if (!DateTime.TryParse(envHistoricalStart, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var parsedStart))
                throw new ArgumentException("Historical mode requires a valid REFRESH_HISTORICAL_START_UTC.");
            if (!DateTime.TryParse(envHistoricalEnd, null, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var parsedEnd))
                throw new ArgumentException("Historical mode requires a valid REFRESH_HISTORICAL_END_UTC.");
            start = parsedStart;
            end = parsedEnd;
        }

        return new IngestionRequestModel(windfarmId, "Alarm", mode, start, end, envRunId) { OperationId = operationId };
    }
}
