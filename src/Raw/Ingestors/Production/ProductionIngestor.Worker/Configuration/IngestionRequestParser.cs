using System.Globalization;
using ProductionIngestor.Enums;
using ProductionIngestor.Models;

namespace ProductionIngestor.Worker.Configuration;

public static class IngestionRequestParser
{
    public static IngestionRequestModel Parse(string? windfarmId, string? refreshWindfarmId, string windfarmName,
        string? refreshDates, string? refreshRunId)
    {
        var input = new IngestionRequestInput(windfarmId, refreshWindfarmId, windfarmName, refreshDates, refreshRunId);
        var result = TryParse(input);
        if (result.Succeeded)
        {
            return result.Request;
        }

        // Preserve the original exception types, messages and parameter names for existing callers.
        throw result.Failure switch
        {
            IngestionRequestFailure.RequiredWindfarmId => RequiredValueException(input.SelectedWindfarmId, "selectedWindfarmId"),
            IngestionRequestFailure.RequiredRefreshDates => RequiredValueException(input.RefreshDates, "refreshDates"),
            _ => new ArgumentException(result.Error)
        };
    }

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

        var selectedWindfarmName = input.RefreshWindfarmName;
        if (string.IsNullOrWhiteSpace(selectedWindfarmName))
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.RequiredWIndfarmName,
                input.IsRefresh ? "Set REFRESH_WINDFARM_NAME for a refresh request." : "Set Ingestion:WindfarmName for an incremental request.");
        }

        // extraction_cursor.windfarm_id is INTEGER; fail fast here rather than at the DB boundary.
        if (!int.TryParse(selectedWindfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedWindfarmId) || parsedWindfarmId <= 0)
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.InvalidWindfarmId,
                "Windfarm identifier must be a positive integer.");
        }

        if (!input.IsRefresh)
        {
            return new IngestionRequestParseResult(new IngestionRequestModel(parsedWindfarmId, selectedWindfarmName, "Production", IngestionMode.Incremental,
                RunId: Guid.NewGuid().ToString("N")) { OperationId = input.OperationId });
        }

        if (string.IsNullOrWhiteSpace(input.RefreshDates))
        {
            return new IngestionRequestParseResult(IngestionRequestFailure.RequiredRefreshDates,
                "Set REFRESH_DATA_DATES to comma-separated yyyy-MM-dd values for a refresh request.");
        }

        var dates = new SortedSet<DateOnly>();
        foreach (var value in input.RefreshDates.Split(',', StringSplitOptions.TrimEntries))
        {
            if (!DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return new IngestionRequestParseResult(IngestionRequestFailure.InvalidRefreshDates,
                    "Refresh dates must be comma-separated yyyy-MM-dd values.");
            }

            dates.Add(date);
        }

        return new IngestionRequestParseResult(new IngestionRequestModel(parsedWindfarmId, selectedWindfarmName, "Production", IngestionMode.Refresh, dates.ToArray(),
            string.IsNullOrWhiteSpace(input.RefreshRunId) ? Guid.NewGuid().ToString("N") : input.RefreshRunId) { OperationId = input.OperationId });
    }

    private static ArgumentException RequiredValueException(string? value, string parameterName)
        => value is null
            ? new ArgumentNullException(parameterName)
            : new ArgumentException("The value cannot be an empty string or composed entirely of whitespace.", parameterName);
}