using System.Globalization;
using OutageIngestor.Enums;
using OutageIngestor.Models;

namespace OutageIngestor.Worker.Configuration;

public static class IngestionRequestParser
{
    public static IngestionRequestParseResult TryParse(IngestionRequestInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        CAT.Messaging.Contracts.OperationCorrelation.Validate(input.OperationId);
        var selectedWindfarmId = input.SelectedWindfarmId;
        if (string.IsNullOrWhiteSpace(selectedWindfarmId))
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.RequiredWindfarmId,
                input.IsRefresh ? "Set REFRESH_WINDFARM_ID for a refresh request." : "Set Ingestion:WindfarmId for an incremental request.");
        }

        // extraction_cursor.windfarm_id is INTEGER; fail fast here rather than at the DB boundary.
        if (!int.TryParse(selectedWindfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedWindfarmId) || parsedWindfarmId <= 0)
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.InvalidWindfarmId,
                "Windfarm identifier must be a positive integer.");
        }

        if (string.IsNullOrWhiteSpace(input.RefreshWindfarmName))
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.RequiredWindfarmName,
            input.IsRefresh ? "Set REFRESH_WINDFARM_NAME for a refresh request." : "A windfarm name is required for an incremental request.");
        }

        DateTime? scopeStartUtc = null;
        DateTime? scopeEndUtc = null;
        if (input.IsRefresh)
        {
            if (!TryParseUtc(input.RefreshScopeStartUtc, out var start) || !TryParseUtc(input.RefreshScopeEndUtc, out var end))
            {
                return new IngestionRequestParseResult(IngestionRequestFailure.RequiredWindfarmName,
                    "Set REFRESH_SCOPE_START_UTC and REFRESH_SCOPE_END_UTC (round-trip UTC timestamps) for a refresh request.");
            }
            if (end <= start)
            {
                return new IngestionRequestParseResult(IngestionRequestFailure.RequiredWindfarmName,
                    "REFRESH_SCOPE_END_UTC must be strictly after REFRESH_SCOPE_START_UTC.");
            }
            scopeStartUtc = start;
            scopeEndUtc = end;
        }

        var runId = string.IsNullOrWhiteSpace(input.RefreshRunId) ? Guid.NewGuid().ToString("N") : input.RefreshRunId;
        return new IngestionRequestParseResult(new IngestionRequestModel(parsedWindfarmId, input.RefreshWindfarmName, "Outage",
            input.IsRefresh ? IngestionMode.Refresh : IngestionMode.Incremental, runId, scopeStartUtc, scopeEndUtc) { OperationId = input.OperationId });
    }

    private static bool TryParseUtc(string? value, out DateTime result)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            result = default;
            return false;
        }
        return DateTime.TryParse(value, CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out result);
    }
}
