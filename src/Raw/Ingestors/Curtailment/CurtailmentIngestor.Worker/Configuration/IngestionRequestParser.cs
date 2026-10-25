using CAT.Messaging.Contracts;
using System.Globalization;
using CurtailmentIngestor.Enums;
using CurtailmentIngestor.Models;

namespace CurtailmentIngestor.Worker.Configuration;

public static class IngestionRequestParser
{
    public static IngestionRequestModel Parse(string? windfarmId, string? refreshWindfarmId, string? refreshRunId, string? operationId = null)
    {
        OperationCorrelation.Validate(operationId);
        var refresh = refreshWindfarmId is not null || refreshRunId is not null;
        var selectedWindfarmId = refresh ? refreshWindfarmId : windfarmId;
        ArgumentException.ThrowIfNullOrWhiteSpace(selectedWindfarmId);
        // extraction_cursor.windfarm_id is INTEGER; fail fast here rather than at the DB boundary.
        if (!int.TryParse(selectedWindfarmId, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedWindfarmId) || parsedWindfarmId <= 0)
            throw new ArgumentException("Windfarm identifier must be a positive integer.");

        // Events are not Windfarm-Day shaped, so Refresh carries no explicit dates (Section 4.3.1).
        return new IngestionRequestModel(selectedWindfarmId, "Curtailment",
            refresh ? IngestionMode.Refresh : IngestionMode.Incremental,
            string.IsNullOrWhiteSpace(refreshRunId) ? Guid.NewGuid().ToString("N") : refreshRunId) { OperationId = operationId };
    }
}
